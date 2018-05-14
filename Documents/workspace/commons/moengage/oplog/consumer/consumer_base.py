import threading
import time
from abc import ABCMeta, abstractmethod

from moengage.commons.connections import InfraType
from moengage.commons.loggers.context_logger import ContextLogger
from moengage.commons.loggers.treysor import LogFormat
from moengage.commons.threadsafe import EXECUTION_CONTEXT
from moengage.oplog.loggers import OplogConsumerTreysor
from moengage.oplog.monitor.pipeline_config.service import PipelineConsumerConfigService


class OplogConsumerBase(ContextLogger):
    __metaclass__ = ABCMeta

    def __init__(self, config_id):
        self.can_run = True
        self.config_id = config_id
        self.oplog_threads = {}
        self.config = self.get_consumer_config().to_dict()
        self.infra_type = InfraType.fromStr(self.config['infra_type'])
        self.thread_lock = threading.Lock()
        self.setup_logging()

    @abstractmethod
    def send_consumer_stopped_alert(self):
        pass

    def setup_logging(self):
        consumer_treysor = OplogConsumerTreysor(logging_config=self.config['logging'])
        treysor_logging_config = OplogConsumerTreysor.setup_treysor_logging_config(self.config['logging'])
        treysor_logging_config.log_format = LogFormat.TEXT
        consumer_treysor.update_logging_config(logging_config=treysor_logging_config)
        EXECUTION_CONTEXT.set('logger', consumer_treysor)

    @abstractmethod
    def get_consumer_object(self, **kwargs):
        raise NotImplementedError('Subclass must implement get_consumer_object function')

    def stop(self, thread_key=None):
        self.logger.error("OplogTail shutdown unexpectedly, collection: %r, stopping all threads" % thread_key)
        for oplog_thread in self.oplog_threads.values():
            oplog_thread.stop()
            oplog_thread.join()
        self.logger.info("Stopped all OplogTail threads, saving timestamp info")
        self.send_consumer_stopped_alert()
        self.can_run = False

    def get_consumer_config(self):
        return PipelineConsumerConfigService.get_config_by_id(self.config_id)

    def run(self):
        collections = self.config['collections']
        while self.can_run:
            for collection in collections:
                if collection not in self.oplog_threads:
                    self.oplog_threads[collection] = self.get_consumer_object(collection=collection)
                    self.oplog_threads[collection].start()
                else:
                    if self.oplog_threads[collection].running:
                        time.sleep(10)
                    else:
                        self.stop(collection)
