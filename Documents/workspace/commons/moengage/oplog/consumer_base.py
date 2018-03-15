import threading
from abc import ABCMeta, abstractmethod

import pymongo

from moengage.commons import ConnectionUtils, InfraType
from moengage.commons.loggers.context_logger import ContextLogger


class OplogTailCollectionBase(threading.Thread, ContextLogger):
    __metaclass__ = ABCMeta

    def __init__(self, db_name, collection, infra_type=InfraType.SEGMENTATION,
                 read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED,
                 replica_set='oplog0',
                 oplog_db_name='new_oplogs'):
        super(OplogTailCollectionBase, self).__init__()
        self.running = True
        self.db_name = db_name
        self.collection = collection
        self.infra_type = infra_type
        self.read_preference = read_preference
        self.connection = ConnectionUtils.getMongoConnectionForInfraType(infra_type,
                                                                         read_preference,
                                                                         replica_set=replica_set)
        self.oplog = self.connection['oplog_db_name'][self.db_name]
        self.checkpoint = None

    def get_last_oplog_id(self):
        curr = self.oplog.find({'collection': self.collection}).sort(
            '$natural', pymongo.DESCENDING
        ).limit(1)
        if curr.count(with_limit_and_skip=True) == 0:
            return None
        return curr[0]['_id']

    def get_first_oplog_id(self):
        curr = self.oplog.find({'collection': self.collection}).sort(
            '$natural', pymongo.ASCENDING
        ).limit(1)
        if curr.count(with_limit_and_skip=True) == 0:
            return None
        return curr[0]['_id']

    def is_checkpoint_old(self):
        return self.checkpoint < self.get_first_oplog_id()

    def create_oplog_cursor(self):
        query = {}
        if self.checkpoint:
            query['_id'] = {'$gte': self.checkpoint}
        cursor = self.oplog.find(query, cursor_type=pymongo.cursor.CursorType.TAILABLE_AWAIT).max_await_time_ms(120000)
        if not cursor:
            self.logger.error("Failed to create oplog cursor: %r for collection: %r" % (self.db_name, self.collection))
            self.running = False
        self.logger.debug(
            "Created oplog cursor for collection: %s with last id: %r" % (self.collection, self.checkpoint))
        return cursor

    @abstractmethod
    def update_checkpoint(self, last_id=None):
        raise NotImplementedError(
            "Child Class must implement {0} function from super".format([self.update_checkpoint.__name__]))

    @abstractmethod
    def read_last_checkpoint(self):
        raise NotImplementedError(
            "Child Class must implement {0} function from super".format([self.read_last_checkpoint.__name__]))

    @abstractmethod
    def dump_collection(self):
        raise NotImplementedError(
            "Child Class must implement {0} function from super".format([self.dump_collection.__name__]))

    @abstractmethod
    def filter_oplog_entry(self, o, op):
        raise NotImplementedError(
            "Child Class must implement {0} function from super".format([self.filter_oplog_entry.__name__]))

    @abstractmethod
    def stop(self):
        raise NotImplementedError("Child Class must implement {0} function from super".format([self.stop.__name__]))

    @abstractmethod
    def run(self):
        raise NotImplementedError("Child Class must implement {0} function from super".format([self.run.__name__]))
