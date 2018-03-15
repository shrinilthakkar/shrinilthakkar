import random
import threading
from abc import ABCMeta

import pymongo

from moengage.oplog.loggers import OplogProducerTreysor
from moengage.oplog.utils import OperationType


class MongoOplogTailBase(threading.Thread):
    __metaclass__ = ABCMeta

    def __init__(self, shard_id, replica_set, hosts, oplog_progress, oplog_ingestor, oplog_batch_size=1000):
        super(MongoOplogTailBase, self).__init__()
        self.running = True
        self.shard_id = shard_id
        self.connection = pymongo.MongoClient(hosts, replicaSet=replica_set,
                                              read_preference=pymongo.ReadPreference.PRIMARY_PREFERRED)
        self.oplog_progress = oplog_progress
        self.oplog = self.connection.local.oplog.rs
        self.checkpoint = None
        self.oplog_ingestor = oplog_ingestor
        self.oplog_batch_size = oplog_batch_size
        self.oplog_counter = 0

    def update_checkpoint(self):
        """Store the current checkpoint in the oplog progress dictionary.
        """
        if self.checkpoint is not None:
            with self.oplog_progress as oplog_prog:
                oplog_dict = oplog_prog.get_dict()
                oplog_dict[self.shard_id] = self.checkpoint
                OplogProducerTreysor().debug(log_tag="mongo_tail_update_checkpoint", shard_id=repr(self.shard_id),
                                             checkpoint=self.checkpoint.as_datetime().isoformat())
        else:
            OplogProducerTreysor().debug(log_tag="mongo_tail_update_checkpoint",
                                         error="No checkpoint to update")

    def get_last_oplog_timestamp(self):
        curr = self.oplog.find().sort('$natural', pymongo.DESCENDING).limit(1)
        if curr.count(with_limit_and_skip=True) == 0:
            return None
        OplogProducerTreysor().info(log_tag="get_last_oplog_timestamp", latest_oplog=repr(curr[0]),
                                    shard_id=repr(self.shard_id))
        return curr[0]['ts']

    def read_last_checkpoint(self):
        """Read the last checkpoint from the oplog progress dictionary.
        """
        ret_val = None

        with self.oplog_progress as oplog_prog:
            oplog_dict = oplog_prog.get_dict()
            if self.shard_id in oplog_dict.keys():
                ret_val = oplog_dict[self.shard_id]
        if not ret_val:
            ret_val = self.get_last_oplog_timestamp()
        return ret_val

    def create_oplog_cursor(self):
        self.checkpoint = self.read_last_checkpoint()
        query = {}
        if self.checkpoint:
            query['ts'] = {'$gte': self.checkpoint}
        cursor = self.oplog.find(
            query, cursor_type=pymongo.cursor.CursorType.TAILABLE_AWAIT, oplog_replay=True).max_await_time_ms(120000)
        if not cursor:
            OplogProducerTreysor().error(log_tag="create_oplog_cursor_failed", shard_id=repr(self.shard_id),
                                         oplog_progress=repr(self.oplog_progress.dict))
            self.running = False
        OplogProducerTreysor().info(log_tag="create_oplog_cursor", shard_id=repr(self.shard_id),
                                    checkpoint=repr(self.checkpoint))
        return cursor

    def stop(self):
        self.running = False

    def save_checkpoint(self, oplog_batch, last_ts):
        try:
            self.oplog_ingestor.ingest_batch(self.shard_id, oplog_batch, self.oplog_progress)
            self.checkpoint = last_ts
            self.oplog_counter += len(oplog_batch)
            self.update_checkpoint()
            if len(oplog_batch) > 0 and random.randint(1, 10) == 10:
                OplogProducerTreysor().info(log_tag="ingest_batch", doc_count=len(oplog_batch),
                                            shard_id=repr(self.shard_id),
                                            total_consumed=repr(self.oplog_counter))
        except Exception, e:
            OplogProducerTreysor().exception(log_tag="ingest_batch_failed", shard_id=repr(self.shard_id), error=repr(e))
            self.running = False

    def filter_oplog(self, db_name, collection_name):
        raise NotImplementedError('Filter oplog logic not defined')

    def needs_flush(self, oplog_batch, total_oplog_count_for_batch):
        return oplog_batch and len(oplog_batch) >= self.oplog_batch_size

    @classmethod
    def send_failure_alert(cls, **kwargs):
        raise NotImplementedError('Send alert method not defined')

    def run(self):
        cursor = self.create_oplog_cursor()
        oplog_batch = []
        total_oplog_count_for_batch = 0
        OplogProducerTreysor().info(log_tag="tailing_shard", shard_id=repr(self.shard_id))
        while self.running:
            if not cursor.alive:
                cursor = self.create_oplog_cursor()
            last_ts = None
            try:
                for entry in cursor:
                    if not self.running:
                        break
                    if entry.get("fromMigrate"):
                        continue
                    operation = OperationType.fromStr(entry['op'])
                    if not operation:
                        continue
                    ns = entry['ns']
                    last_ts = entry['ts']
                    if '.' not in ns:
                        continue
                    db_name, collection = ns.split('.', 1)
                    if self.needs_flush(oplog_batch, total_oplog_count_for_batch):
                        self.save_checkpoint(oplog_batch, last_ts)
                        oplog_batch = []
                        total_oplog_count_for_batch = 0

                    total_oplog_count_for_batch += 1
                    if self.filter_oplog(db_name, collection):
                        continue
                    oplog_batch.append(entry)
                if last_ts:
                    self.save_checkpoint(oplog_batch, last_ts)
                    oplog_batch = []
            except Exception, e:
                OplogProducerTreysor().exception(log_tag="tailing_shard_failed", shard_id=repr(self.shard_id),
                                                 error=repr(e))
                self.send_failure_alert(errored_shard=self.shard_id, exception=str(e),
                                        suggested_action='Supervisor should restart the process automatically. Please verify')
                self.running = False
        self.running = False
