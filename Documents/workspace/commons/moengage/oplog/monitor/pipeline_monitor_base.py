import grp
import os
import pwd
import random
import socket
import time
from abc import ABCMeta, abstractmethod

from moengage.commons.config.provider import ConfigFileProvider
from moengage.commons.loggers.context_logger import ContextLogger
from moengage.commons.loggers.treysor import LogFormat
from moengage.commons.threadsafe import EXECUTION_CONTEXT
from moengage.commons.utils import CommonUtils
from moengage.daos.app_dao import AppDAO
from moengage.oplog.loggers import OplogMonitorTreysor


class PipelineMonitorBase(ContextLogger):
    __metaclass__ = ABCMeta

    def __init__(self, process_type, **kwargs):
        self.process_type = process_type
        self.config = self.get_config()
        self.iteration_sleep = kwargs.get('iteration_sleep', 0)
        if kwargs.get('setup_logging', True):
            self._init_monitor()

    def _init_monitor(self):
        self.create_directories()
        self.setup_logger()

    def get_config(self):
        config_provider = ConfigFileProvider('monitor_config.json', __name__)
        config = config_provider.config
        config['basePath'] = config['basePath'].format(process_name=str(self.process_type))
        config["folders"].append(str(self.process_type))
        return config

    @abstractmethod
    def filter_db_name(self, db_name):
        raise NotImplementedError("Child classes must implement filter_app method")

    @abstractmethod
    def get_pipeline_connector(self, db_name):
        raise NotImplementedError("Child classes must implement get_pipeline_connector method")

    @abstractmethod
    def get_consumer_hosts(self, db_name):
        raise NotImplementedError("Child classes must implement get_consumer_hosts method")

    def send_monitor_stopped_alert(self, **kwargs):
        pass

    def get_host_ips_for_db_name(self, db_name):
        consumer_ips = []
        hosts = self.get_consumer_hosts(db_name)
        for host in hosts:
            addrinfo = socket.getaddrinfo(host, "22", 0, socket.SOCK_STREAM)
            for addr in addrinfo:
                _, _, _, _, sa = addr
                consumer_ips.append(sa[0])
        return random.choice(consumer_ips)

    @classmethod
    def throttle_after_consumer_start(cls, db):
        pass

    def setup_logger(self):
        logging_config = dict(backupCount=5, when='D', interval=1, log_level=2,
                              filename=os.path.join(self.config['basePath'], "monitor.log"))
        monitor_treysor = OplogMonitorTreysor(logging_config=logging_config)
        treysor_logging_config = OplogMonitorTreysor.setup_treysor_logging_config(logging_config)
        treysor_logging_config.log_format = LogFormat.TEXT
        monitor_treysor.update_logging_config(logging_config=treysor_logging_config)
        EXECUTION_CONTEXT.set('logger', monitor_treysor)

    def create_directories(self):
        base_path = self.config['basePath']
        folders = self.config['folders']
        try:
            if not os.path.exists(base_path):
                os.makedirs(base_path)
                os.chmod(base_path, 0o764)
                os.chown(base_path, pwd.getpwnam("ubuntu").pw_uid, grp.getgrnam("syslog").gr_gid)
            for folder in folders:
                folder_path = os.path.join(base_path, folder)
                if not os.path.exists(folder_path):
                    os.makedirs(folder_path)
                    os.chmod(folder_path, 0o764)
                    os.chown(folder_path, pwd.getpwnam("ubuntu").pw_uid, grp.getgrnam("syslog").gr_gid)
        except OSError:
            self.logger.critical("Could not create required folder paths")
            raise

    def get_apps(self):
        app_filter = self.config.get('apps', {}).get('include', {}).get('filter', {})
        db_names = map(lambda x: x.db_name, AppDAO().find(app_filter, projection={'db_name': 1}))
        for db_name in db_names:
            try:
                if not self.filter_db_name(db_name):
                    yield dict(db_name=db_name, db_category=CommonUtils.getDBCategory(db_name))
                else:
                    self.logger.info("Skipping db_name %r" % db_name)
            except Exception, e:
                self.logger.exception("Failed to get info for db_name %r error %r" % (db_name, e))

    @classmethod
    def get_current_consumer_details(cls, pipeline_connector):
        return pipeline_connector.pipeline_status.get_status()

    def start_pipeline_consumer_for_db_name(self, db_name):
        try:
            pipeline_connector = self.get_pipeline_connector(db_name)
            pipeline_consumer_exists = pipeline_connector.exists()
            self.logger.info("App: %r - Pipeline running status: %s" % (db_name, str(pipeline_consumer_exists)))
            if not pipeline_consumer_exists:
                host_ip = self.get_host_ips_for_db_name(db_name)
                self.logger.info("Pipeline not found for app: %s" % db_name)
                pipeline_connector.start(host_ip)
                self.logger.info("Started pipeline for app: %r" % db_name)
                return True
        except Exception, e:
            self.logger.exception("Failed to create mongo es pipeline for app: %s due to error %r" % (db_name, e))
        return False

    def run(self):
        try:
            while True:
                for db in self.get_apps():
                    pipeline_started = self.start_pipeline_consumer_for_db_name(db['db_name'])
                    if pipeline_started:
                        self.throttle_after_consumer_start(db)
                self.logger.info("Finished checking all apps, sleeping for %d seconds" % self.iteration_sleep)
                time.sleep(self.iteration_sleep)
        except Exception, e:
            self.send_monitor_stopped_alert(reason=e)
            self.logger.exception("Pipeline monitor failed due to error %r" % e)
            raise
