import time
from abc import ABCMeta

from moengage.commons.connections import InfraType
from moengage.commons.loggers.treysor import LogFormat
from moengage.commons.threadsafe import LockingDict, EXECUTION_CONTEXT
from moengage.commons.utils import CommonUtils
from moengage.oplog.loggers import OplogProducerTreysor
from moengage.oplog.producer_checkpoint_tracker.service import ProducerCheckpointTrackerService
from moengage.oplog.utils import OplogUtils


class OplogProducerBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, config, mongo_oplog_tail_class, producer_type, infra_type=None, oplog_ingestor=None):
        self.can_run = True
        self.shard_set = {}
        self.config = config
        self.oplog_ingestor = oplog_ingestor
        self.oplog_progress = LockingDict()
        self.mongo_oplog_tail_class = mongo_oplog_tail_class
        self.producer_type = producer_type
        self.infra_type = infra_type or InfraType.fromStr(self.config['infra_type'])
        self.checkpoint_tracker = None
        self.checkpoint_tracker_service = ProducerCheckpointTrackerService(self.producer_type)
        self.setup_logging(self.infra_type)

    def setup_logging(self, infra_type):
        self.config['logging']['filename'] = self.config['logging']['filename'] + 'producer-' + str(infra_type) + '.log'
        producer_treysor = OplogProducerTreysor(logging_config=self.config['logging'])
        treysor_logging_config = OplogProducerTreysor.setup_treysor_logging_config(self.config['logging'])
        treysor_logging_config.log_format = LogFormat.JSON
        producer_treysor.update_logging_config(logging_config=treysor_logging_config)
        EXECUTION_CONTEXT.set('logger', producer_treysor)

    def get_checkpoint_tracker(self):
        checkpoint_tracker = self.checkpoint_tracker_service.get_tracker()
        if checkpoint_tracker is None:
            OplogProducerTreysor().info(log_tag="read_oplog_progress",
                                        message="Tracker does not exist. Creating tracker")
            checkpoint_tracker = self.checkpoint_tracker_service.create_tracker()
        return checkpoint_tracker

    def read_oplog_progress(self):
        if self.checkpoint_tracker is None:
            self.checkpoint_tracker = self.get_checkpoint_tracker()
        data = self.checkpoint_tracker.checkpoint
        with self.oplog_progress:
            if data:
                if data.get('db_names'):
                    data['db_names'] = set(data['db_names'])
                self.oplog_progress.dict = data
        return self.oplog_progress

    def write_oplog_progress(self):
        with self.oplog_progress as oplog_prog:
            oplog_dict = oplog_prog.get_dict()
            if not oplog_dict:
                return

        self.checkpoint_tracker = self.checkpoint_tracker_service.update_or_create_tracker(
            checkpoint=CommonUtils.to_serializable_dict(oplog_dict),
            tracker=self.checkpoint_tracker)

    @classmethod
    def send_alert(cls, **kwargs):
        raise NotImplementedError('Send alert method not defined')

    def stop(self, shard_id=None):
        OplogProducerTreysor().error(log_tag="oplog_tail_threads_stopped", shard_id=repr(shard_id))
        for shard_id, oplog_thread in self.shard_set.items():
            oplog_thread.stop()
            oplog_thread.join()
        OplogProducerTreysor().info(log_tag="all_oplog_tail_threads_stopped")
        self.send_alert(errored_shard=shard_id,
                        suggested_action='Supervisor should restart the process automatically.Please verify')
        self.write_oplog_progress()
        self.can_run = False

    def run(self):
        shard_config = OplogUtils.get_shard_config(self.infra_type)
        self.oplog_progress = self.read_oplog_progress()
        while self.can_run:
            for shard_id, config in shard_config.items():
                if shard_id not in self.shard_set:
                    shard_oplog_tail = self.mongo_oplog_tail_class(shard_id, config['replica_set'], config['hosts'],
                                                                   self.oplog_progress, self.oplog_ingestor)
                    self.shard_set[shard_id] = shard_oplog_tail
                    shard_oplog_tail.start()
                else:
                    if self.shard_set[shard_id].running:
                        self.write_oplog_progress()
                        time.sleep(10)
                    else:
                        self.stop(shard_id)
                        break
