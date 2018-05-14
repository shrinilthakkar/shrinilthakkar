import datetime
import threading
import time
from abc import ABCMeta, abstractmethod

import pymongo
from bson import ObjectId
from bson import json_util

from moengage.commons import ConnectionUtils, InfraType
from moengage.commons.loggers.context_logger import ContextLogger
from moengage.oplog.progress_tracker.service import ProgressTrackerService
from moengage.oplog.utils import OperationType


class OplogTailCollectionBase(threading.Thread, ContextLogger):
    __metaclass__ = ABCMeta

    def __init__(self, doc_handler, thread_lock, tracker_type,
                 oplog_db_name='oplogs',
                 infra_type=InfraType.SEGMENTATION,
                 read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED,
                 replica_set='oplog0'):
        super(OplogTailCollectionBase, self).__init__()
        self.doc_handler = doc_handler
        self.doc_formatter = self.doc_handler.doc_formatter
        self.doc_manager = self.doc_handler.doc_manager
        self.db_name = doc_handler.db_name
        self.collection = doc_handler.collection

        self.infra_type = infra_type
        self.read_preference = read_preference
        self.connection = ConnectionUtils.getMongoConnectionForInfraType(infra_type,
                                                                         read_preference,
                                                                         replica_set=replica_set)
        self.oplog = self.connection[oplog_db_name][self.db_name]

        self.error = None
        self.running = True
        self.checkpoint = None
        self.thread_lock = thread_lock
        self.progress_tracker = ProgressTrackerService(doc_handler.db_name, tracker_type)

    @abstractmethod
    def bulk_dump_mongo(self):
        # Should return boolean status True if success False if failure
        raise NotImplementedError(
            'Child Class must implement {0} function from super'.format(self.bulk_dump_mongo.__name__))

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
            query['_id'] = {'$gte': ObjectId(self.checkpoint)}
        cursor = self.oplog.find(query, cursor_type=pymongo.cursor.CursorType.TAILABLE_AWAIT).max_await_time_ms(120000)
        if not cursor:
            self.logger.error("Failed to create oplog cursor: %r for collection: %r" % (self.db_name, self.collection))
            self.running = False
        self.logger.info(
            "Created oplog cursor for collection: %s with last id: %r" % (self.collection, self.checkpoint))
        return cursor

    def update_checkpoint(self, last_id=None):
        """
            Store the current checkpoint in the oplog progress dictionary.
        """
        with self.thread_lock:
            self.checkpoint = last_id
            if self.checkpoint is not None:
                self.progress_tracker.update_or_create_tracker(checkpoint=self.checkpoint)
                checkpoint_generation_time = ObjectId(self.checkpoint).generation_time.isoformat()
                self.logger.info("OplogThread: oplog checkpoint updated to %s for collection: %s generation time %s" % (
                    self.checkpoint, self.collection, checkpoint_generation_time))
            else:
                self.logger.info("OplogThread: no checkpoint to update.")

    def read_last_checkpoint(self):
        """
            Read the last checkpoint from the oplog progress dictionary.
        """
        tracker = self.progress_tracker.get_tracker()
        if tracker:
            return tracker.checkpoint
        return None

    def dump_collection(self):
        self.checkpoint = self.get_last_oplog_id()
        if not self.checkpoint:
            self.checkpoint = ObjectId.from_datetime(datetime.datetime.utcnow())
        if self.bulk_dump_mongo():
            self.update_checkpoint(self.checkpoint)
            return True
        return False

    def filter_oplog_entry(self, o, op):
        entry_o = o
        # 'i' indicates an insert. 'o' field is the doc to be inserted.
        if op == OperationType.INSERT:
            self.doc_formatter.pop_excluded_fields(entry_o, self.db_name, self.collection)
        # 'u' indicates an update. The 'o' field describes an update spec
        # if '$set' or '$unset' are present.
        elif op == OperationType.UPDATE and ('$set' in entry_o or '$unset' in entry_o):
            self.doc_formatter.pop_excluded_fields(entry_o.get("$set", {}), self.db_name, self.collection)
            self.doc_formatter.pop_excluded_fields(entry_o.get("$unset", {}), self.db_name, self.collection)
            # not allowed to have empty $set/$unset, so remove if empty
            if "$set" in entry_o and not entry_o['$set']:
                entry_o.pop("$set")
            if "$unset" in entry_o and not entry_o['$unset']:
                entry_o.pop("$unset")
            if not entry_o:
                return None
        # 'u' indicates an update. The 'o' field is the replacement document
        # if no '$set' or '$unset' are present.
        elif op == OperationType.UPDATE:
            self.doc_formatter.pop_excluded_fields(entry_o, self.db_name, self.collection)

        return o

    def parse_entry_key(self, entry, key):
        return json_util.loads(entry.get(key, '{}'))

    def upsert_doc(self, _id, doc=None):
        return self.doc_handler.upsert_doc(_id=_id, doc=doc)

    def collection_dump_required(self):
        if not self.checkpoint:
            self.logger.info("Dumping all users due to missing checkpoint")
            return True
        elif self.is_checkpoint_old():
            self.logger.info("Dumping all users due to old checkpoint")
            return True
        return False

    def dump_collection_based_on_checkpoint(self):
        self.checkpoint = self.read_last_checkpoint()
        self.logger.info("Initializing cursor with initial timestamp: %r" % self.checkpoint)
        dump_status = None
        if self.collection_dump_required():
            dump_status = self.dump_collection()
        if dump_status is True:
            self.logger.info('OplogThread: Successfully dumped collection: %s' % self.collection)
        elif dump_status is False:
            self.running = False
        return dump_status

    def get_oplogs(self):
        cursor = self.create_oplog_cursor()
        while cursor.alive:
            try:
                yield cursor.next()
            except StopIteration:
                time.sleep(60)
                self.logger.info('Creating Oplog cursor due to timeout')
                try:
                    cursor.close()
                except Exception:
                    self.logger.exception('Error in closing cursor')
                if self.process_oplog():
                    cursor = self.create_oplog_cursor()
                else:
                    break

    def process_oplog(self):
        return True

    def stop(self):
        self.running = False

    def run(self):
        last_id = None
        try:
            self.dump_collection_based_on_checkpoint()
            remove_inc = 0
            upsert_inc = 0
            update_inc = 0
            while self.running:
                if not self.process_oplog():
                    self.stop()
                    break
                for n, entry in enumerate(self.get_oplogs()):
                    if not self.running:
                        break
                    if not self.process_oplog():
                        self.stop()
                        break
                    if entry['collection'] != self.collection:
                        continue

                    operation = OperationType.fromStr(entry['op'])
                    o = self.filter_oplog_entry(self.parse_entry_key(entry, 'o'), operation)
                    o2 = self.parse_entry_key(entry, 'o2')

                    if not o:
                        self.logger.debug("OplogThread: Nullified entry: %r" % entry)
                        continue

                    if operation == OperationType.DELETE:
                        self.doc_manager.remove(o['_id'])
                        remove_inc += 1
                    elif operation == OperationType.INSERT:
                        doc = o
                        self.upsert_doc(_id=doc['_id'], doc=doc)
                        upsert_inc += 1
                    elif operation == OperationType.UPDATE:
                        try:
                            self.doc_manager.update(o2['_id'], o)
                        except Exception:
                            _id = o2['_id']
                            self.logger.exception("Failed to update document with id: %r, trying re-upsert op id %r" % (
                                _id, entry['_id']))
                            self.upsert_doc(_id=_id)
                        update_inc += 1

                    if (remove_inc + upsert_inc + update_inc) is not 0 and (
                                    remove_inc + upsert_inc + update_inc) % 1000 == 0:
                        self.logger.info("OplogThread: Documents removed: %d, inserted: %d, updated: %d so far" %
                                         (remove_inc, upsert_inc, update_inc))
                    last_id = entry['_id']
                    if n % 1000 == 0 and last_id is not None:
                        self.update_checkpoint(last_id)
                if last_id is not None:
                    self.update_checkpoint(last_id)
        except Exception, e:
            self.logger.exception("Exception while processing oplogs: %r" % e)
            self.error = e
            self.stop()

        # To handle cases where exception occurs before updating checkpoint
        if last_id is not None:
            self.update_checkpoint(last_id)
        self.running = False
