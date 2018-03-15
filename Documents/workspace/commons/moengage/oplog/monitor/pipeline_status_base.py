import os

from enum import Enum
from paramiko.ssh_exception import NoValidConnectionsError, SSHException
from pymongo.read_preferences import ReadPreference

from moengage.commons.connections import ConnectionUtils, InfraType
from moengage.commons.decorators import Retry
from moengage.commons.exceptions import MONGO_NETWORK_ERRORS
from moengage.commons.loggers.context_logger import ContextLogger
from moengage.oplog.exceptions import ZombieProcessNotKilled
from moengage.oplog.monitor.pipeline_config.service import PipelineConsumerConfigService
from moengage.oplog.monitor.pipeline_status_tracker.service import PipelineStatusTrackerService
from moengage.oplog.utils import OplogUtils


class PipelineStatus(Enum):
    STARTED = 1
    ERROR = 2
    STOPPED = 3
    FAILED_TO_START = 4
    INITIAL_IMPORT = 5
    RUNNING = 6

    def __str__(self):
        return {
            PipelineStatus.STARTED: "STARTED",
            PipelineStatus.ERROR: "ERROR",
            PipelineStatus.STOPPED: "STOPPED",
            PipelineStatus.FAILED_TO_START: "FAILED_TO_START",
            PipelineStatus.INITIAL_IMPORT: "INITIAL_IMPORT",
            PipelineStatus.RUNNING: "RUNNING"
        }.get(self, "")

    @staticmethod
    def fromStr(value):
        return {
            "STARTED": PipelineStatus.STARTED,
            "ERROR": PipelineStatus.ERROR,
            "STOPPED": PipelineStatus.STOPPED,
            "FAILED_TO_START": PipelineStatus.FAILED_TO_START,
            "INITIAL_IMPORT": PipelineStatus.INITIAL_IMPORT,
            "RUNNING": PipelineStatus.RUNNING
        }.get(value)


class PipelineProcessStatusReporterBase(ContextLogger):
    # WARNING if this key is absent or not present in this location ssh operations will fail
    key_filename = '/var/keys/pipeline_consumer.pem'

    def __init__(self, db_name, process_type, process_name=None):
        self.db_name = db_name
        self.process_type = process_type
        self.mongo_connection = ConnectionUtils.getMongoConnectionForInfraType(InfraType.SEGMENTATION,
                                                                               read_preference=ReadPreference.PRIMARY_PREFERRED)
        self.pipeline_status_tracker = PipelineStatusTrackerService(self.db_name, self.process_type)
        self.process_name = process_name or filter(lambda x: x.isalnum(), self.process_type)
        self.pipeline_consumer_config = PipelineConsumerConfigService(self.db_name, self.process_type)

    @Retry(MONGO_NETWORK_ERRORS, after=30)
    def get_status(self):
        return self.pipeline_status_tracker.get_tracker()

    @Retry(MONGO_NETWORK_ERRORS, after=30)
    def update_status(self, status=None, pid=-1, host_ip=None):
        status = str(status if status else "Undetermined")
        try:
            update_kwargs = {}
            if host_ip:
                update_kwargs['machine_ip'] = host_ip
            self.pipeline_status_tracker.update_or_create_tracker(status=status, pid=pid, **update_kwargs)
            self.logger.info("STATUS_REPORTING: Updated pipeline status for db_name: %s to status: %s and pid: %r" % (
                self.db_name, status, pid))
        except Exception, e:
            self.logger.exception(
                "STATUS_REPORTING: Failed to update pipeline status for db_name: %s to status: %s due to error %r" % (
                    self.db_name, status, e))

    @classmethod
    @Retry((NoValidConnectionsError, SSHException, EOFError), after=10, max_retries=10)
    def get_process_details(cls, host_ip, pid):
        cls.ensure_ssh_key()
        stdout, _ = OplogUtils.execute_remote_command(host_ip,
                                                      command="sudo ps -p {pid} -o stat=,cmd=".format(pid=pid),
                                                      key_filename=cls.key_filename)
        return stdout

    @classmethod
    @Retry((NoValidConnectionsError, SSHException, EOFError), after=10, max_retries=10)
    def terminate_process(cls, host_ip, pid):
        cls.ensure_ssh_key()
        stdout, _ = OplogUtils.execute_remote_command(host_ip,
                                                      command="sudo kill -9 {pid}".format(pid=pid),
                                                      key_filename=cls.key_filename)
        return stdout

    @classmethod
    def start_process(cls, host_ip, start_command):
        cls.ensure_ssh_key()
        stdout, _ = OplogUtils.execute_remote_command(host_ip,
                                                      command=start_command,
                                                      key_filename=cls.key_filename)
        return stdout

    @classmethod
    def get_process_details_by_process_and_config(cls, host_ip, process_name, config_id):
        cls.ensure_ssh_key()
        get_command = "sudo ps axo stat,pid,cmd | grep {process_name} | "
        get_command += "grep -w \"{config_id}\" | grep -v \"nohup \""
        get_command = get_command.format(config_id=config_id, process_name=process_name)
        stdout, _ = OplogUtils.execute_remote_command(host_ip,
                                                      command=get_command,
                                                      key_filename=cls.key_filename)
        return stdout

    @classmethod
    def ensure_ssh_key(cls):
        path_exists = os.path.exists(cls.key_filename)
        if not path_exists:
            raise OSError('key file missing %r' % cls.key_filename)

    @Retry(ZombieProcessNotKilled, after=10, max_retries=5)
    def get_and_validate_process(self, host_ip, pid):
        process_details = self.get_process_details(host_ip, pid)
        if not process_details:
            return None
        # Split details by space
        process_details = process_details.split()
        self.logger.debug("get_and_validate_process: %r" % process_details)
        config_id = self.pipeline_consumer_config.get_config_id()
        if len(process_details) == 4 and process_details[3] == config_id and \
                process_details[2].endswith(self.process_name):
            if process_details[0] == 'Z':
                self.terminate_process(host_ip, pid)
                raise ZombieProcessNotKilled('Zombie process with pid {pid} exists'.format(pid=pid))
            return process_details
        return None

    def check_pid_exists(self, host_ip, pid):
        # ps -o cmd= 226173
        """ Check For the existence of a unix pid as a mongo connector process"""
        proc = self.get_and_validate_process(host_ip, pid)
        self.logger.info("Found process with pid: %r" % pid)
        if proc:
            return True
        return False
