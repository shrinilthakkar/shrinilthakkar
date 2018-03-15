import time

from paramiko.ssh_exception import NoValidConnectionsError, SSHException

from moengage.commons.decorators import Retry
from moengage.commons.loggers.context_logger import ContextLogger
from moengage.oplog.exceptions import PipelineStatusCreationFailedException
from moengage.oplog.monitor.pipeline_status_base import PipelineStatus


class PipelineConnectorBase(ContextLogger):
    def __init__(self, pipeline_status, pipeline_config):
        self.pipeline_status = pipeline_status
        self.pipeline_start_wait_time = 5
        self.pipeline_config = pipeline_config

    def get_pid(self):
        """Get PID for the mongo-connector running for this process and db_name

        Returns:
            pid: pid, if pid found from file else -1
        """
        pid = -1
        try:
            status = self.pipeline_status.get_status()
            if status and status.pid > 0:
                proc = self.pipeline_status.get_and_validate_process(status.machine_ip, status.pid)
                self.logger.info("Retrieved status for app: %s, status: %r proc %r" % (self.pipeline_status.db_name,
                                                                                       status.to_dict(), proc))
                if proc:
                    return status.pid
        except (NoValidConnectionsError, SSHException, EOFError), e:
            self.logger.exception("Failed to get pid for app: %s due to error: %r" % (self.pipeline_status.db_name, e))
            raise
        except Exception, e:
            self.logger.exception("Failed to get pid for app: %s due to error: %r" % (self.pipeline_status.db_name, e))
            pid = -1
        return int(pid)

    def check_if_process_started(self, host_ip, pid):
        start_time = time.time()
        while (time.time() - start_time) < self.pipeline_start_wait_time:
            time.sleep(self.pipeline_start_wait_time)
            process_details = self.pipeline_status.get_and_validate_process(host_ip, pid)
            if process_details:
                return process_details[1]
        return None

    def get_process_start_command(self, *args):
        process_start_command = ["sudo", "nohup", self.pipeline_status.process_name]
        process_start_command.extend(args)
        process_start_command.append(">/dev/null 2>&1 &")
        return " ".join(process_start_command)

    @Retry((NoValidConnectionsError, SSHException), after=10, max_retries=10)
    def start_process(self, host_ip):
        config_id = self.pipeline_status.pipeline_consumer_config.get_config_id()
        if not config_id:
            config_id = self.pipeline_status.pipeline_consumer_config.create_pipeline_config(self.pipeline_config).id
        # sending config id as string to avoid case like Indiamart(Prod)
        process_start_command = self.get_process_start_command('"' + config_id + '"')
        process = self.pipeline_status.get_process_details_by_process_and_config(host_ip,
                                                                                 process_name=self.pipeline_status.process_name,
                                                                                 config_id=config_id)
        if not process:
            self.pipeline_status.start_process(host_ip, process_start_command)
            process = self.pipeline_status.get_process_details_by_process_and_config(host_ip,
                                                                                     process_name=self.pipeline_status.process_name,
                                                                                     config_id=config_id)
        if process:
            process = process.split()
        self.logger.info("Waiting for pipeline to start")
        return process

    def start_pipeline_consumer(self, host_ip):
        """
        Start a new consumer instance for db and process type
        
        Returns:
            True if mongo connector started successfully, False otherwise
        """
        self.logger.info("Starting mongo es pipeline for db_name: %s" % self.pipeline_status.db_name)
        pid = self.get_pid()
        if pid is -1:
            self.logger.info("Starting mongo es pipeline process")
            process = self.start_process(host_ip)
            self.logger.info("Checking if pipeline started - process %r" % process)
            if process and len(process) > 2 and self.check_if_process_started(host_ip, process[1]):
                pid = int(process[1])
                self.logger.info("Pipeline started successfully with pid: %d" % pid)
            else:
                self.logger.critical("Mongo Es Pipeline failed to start - terminating created process")
                self.stop()
                return -1
        else:
            self.logger.info("Existing pipeline instance found - PID: %d" % pid)
        return pid

    def start(self, host_ip):
        pid = self.start_pipeline_consumer(host_ip)
        status = pid is not -1
        if status:
            self.pipeline_status.update_status(host_ip=host_ip, status=PipelineStatus.RUNNING, pid=pid)
        else:
            self.pipeline_status.update_status(host_ip=host_ip, status=PipelineStatus.FAILED_TO_START)

    def stop(self):
        status = self.pipeline_status.get_status()
        if status and status.pid > 0 and status.machine_ip is not None:
            self.pipeline_status.terminate_process(status.machine_ip, status.pid)
        return self.exists()

    def exists(self):
        pid = self.get_pid()
        exists = pid is not -1
        if not exists:
            try:
                self.pipeline_status.update_status(status=PipelineStatus.STOPPED)
            except PipelineStatusCreationFailedException:
                pass
        return exists
