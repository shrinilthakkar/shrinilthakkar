import random
import threading
import time
from abc import ABCMeta

import pymongo
from pymongo.errors import AutoReconnect, OperationFailure

from moengage.commons.loggers.context_logger import ContextLogger
from moengage.oplog.consumer.mongo_collection_dump_thread import MongoCollectionDumpThread


class MongoShardedCollectionDumpBase(threading.Thread, ContextLogger):
    __metaclass__ = ABCMeta

    def __init__(self, document_handler, shard_config, **kwargs):
        super(MongoShardedCollectionDumpBase, self).__init__()
        # Cannot use connection utils to create connection here because of replica set
        self.connection = pymongo.MongoClient(shard_config['hosts'], replicaSet=shard_config['replica_set'],
                                              read_preference=pymongo.ReadPreference.PRIMARY)
        self.doc_manager = document_handler.doc_manager
        self.shard_config = shard_config
        self.num_of_processing_threads = kwargs.get('num_of_processing_threads', 1)
        self.processing_thread_get_timeout = kwargs.get('processing_thread_get_timeout', 100)
        self.processing_thread_class = MongoCollectionDumpThread
        self.error = None
        self.doc_count = 0
        self.last_doc = None
        self.document_handler = document_handler
        self.processing_threads = {}
        self.processing_thread_checkpoint = {}
        self.checkpoint_lock = threading.Lock()
        self.max_time_to_wait_for_child_threads = 1800

    def parse_document(self, doc):
        return self.document_handler.doc_formatter.pop_excluded_fields(doc, self.document_handler.db_name,
                                                                       self.document_handler.collection)

    def get_connection(self):
        return self.connection[self.document_handler.db_name][self.document_handler.collection]

    def get_query_to_fetch_docs(self):
        # specify mongo queries and last_id filter will be applied in doc query
        return {}

    def docs_to_dump(self):
        collection = self.get_connection()
        attempts = 0
        last_id = None
        while attempts < 60:
            query = self.get_query_to_fetch_docs()
            if last_id:
                if '_id' in query:
                    query['_id'].update({'$gt': last_id})
                else:
                    query.update({'_id': {'$gt': last_id}})
            cursor = collection.find(query, sort=[("_id", pymongo.ASCENDING)])
            try:
                for doc in cursor:
                    self.parse_document(doc)
                    last_id = doc['_id']
                    yield doc
                break
            except (AutoReconnect, OperationFailure):
                attempts += 1
                time.sleep(1)

    def upsert_failed_doc(self, error_doc):
        self.document_handler.upsert_doc(_id=error_doc['_id'])

    def wait_for_child_thread_stop(self, max_time_to_wait):
        start_time = time.time()
        while (time.time() - start_time) < max_time_to_wait:
            self.logger.info("Waiting for all threads to stop")
            is_thread_alive = False
            for thread_name in self.processing_threads:
                is_thread_alive |= self.processing_threads[thread_name].isAlive()
                if is_thread_alive:
                    self.logger.info("Found alive thread: %s" % thread_name)
            if is_thread_alive:
                time.sleep(5)
            else:
                self.logger.info("All Threads stopped, shutting down main thread")
                return
        self.logger.info("Timeout when attempting child thread stop. Killing main thread anyway")

    def stop(self):
        self.logger.info("Stop oplog threads called")
        for thread_name in self.processing_threads:
            self.logger.info("Stopping oplog thread: %s" % thread_name)
            self.processing_threads[thread_name].stop()
        self.wait_for_child_thread_stop(self.max_time_to_wait_for_child_threads)

    def start_processing_thread(self, thread_name):
        if not self.processing_threads[thread_name].started:
            self.processing_threads[thread_name].start()
            time.sleep(0.2)
            return self.start_processing_thread(thread_name)
        return True

    def update_thread_checkpoint(self, thread_name, checkpoint):
        with self.checkpoint_lock:
            if checkpoint:
                self.processing_thread_checkpoint[thread_name] = checkpoint

    def run(self):
        thread_names = map(lambda x: 'mongo_coll_dump_thread_{0}'.format(str(x)),
                           range(0, self.num_of_processing_threads))
        for thread_num in range(0, self.num_of_processing_threads):
            thread_name = thread_names[thread_num]
            processing_thread_get_timeout = self.processing_thread_get_timeout
            oplog_thread = self.processing_thread_class(doc_manager=self.doc_manager,
                                                        document_handler=self.document_handler,
                                                        shard_config=self.shard_config,
                                                        thread_name=thread_name,
                                                        doc_get_timeout=processing_thread_get_timeout)
            self.processing_threads[thread_name] = oplog_thread
            self.processing_thread_checkpoint[thread_name] = None
        try:
            start_time = time.time()
            for doc in self.docs_to_dump():
                thread_name = random.choice(thread_names)
                self.start_processing_thread(thread_name)
                if not self.processing_threads[thread_name].can_run:
                    raise Exception('Thread %s is stopped. Stopping all threads' % thread_name)

                self.processing_threads[thread_name].add_item(doc)
                self.last_doc = doc
                self.update_thread_checkpoint(thread_name, self.processing_threads[thread_name].checkpoint_scrolled)
                self.doc_count += 1
            self.logger.info("Collection dump finished for doc_count: %r time_taken: %r" % (self.doc_count,
                                                                                            time.time() - start_time))
            self.logger.info("disabling data polling for threads")
            for thread_name in self.processing_threads:
                self.logger.info("disabling data polling for thread: %s" % thread_name)
                self.processing_threads[thread_name].disable_data_polling()
            self.wait_for_child_thread_stop(self.max_time_to_wait_for_child_threads)
        except Exception, e:
            self.logger.exception(
                "Collection dump for shard config: %r failed due to exception: %r" % (self.shard_config, e))
            self.error = e
            self.stop()
