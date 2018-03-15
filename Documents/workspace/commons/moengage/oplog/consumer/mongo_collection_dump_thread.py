import threading
import time
from Queue import Queue, Empty, Full

from moengage.commons.decorators.retry import Retry
from moengage.commons.loggers.context_logger import ContextLogger
from moengage.oplog.alerts import OplogAlert, CodeAlertDelivery, CodeAlertLevel, OplogErrorTag


class DataPollingStoppedException(Exception):
    pass


class MongoCollectionDumpThread(threading.Thread, ContextLogger):
    def __init__(self, doc_manager, document_handler, shard_config,
                 batch_size=500, thread_name=None, doc_get_timeout=100):
        super(MongoCollectionDumpThread, self).__init__(name=thread_name)
        self.can_run = True
        self.doc_manager = doc_manager
        self.document_handler = document_handler
        self.shard_config = shard_config
        self.batch_size = batch_size
        self.docs = Queue(batch_size * 3)
        self.started = False
        self.checkpoint_scrolled = None
        self.doc_get_timeout = doc_get_timeout
        self.can_poll_data = True
        self.setDaemon(True)

    @Retry(Full, max_retries=5)
    def add_item(self, doc):
        if self.can_run:
            try:
                self.docs.put(doc, timeout=1800)
            except Full:
                self.logger.warning(
                    "waiting_to_put_msg for message in shard config %r checkpoint_scrolled %r" % (
                        self.shard_config, self.checkpoint_scrolled))
                OplogAlert(OplogErrorTag.BULK_DUMP_STUCK, CodeAlertDelivery.SLACK,
                           CodeAlertLevel.WARNING).send(log_tag='waiting_to_put_msg',
                                                        db_name=repr(
                                                            self.document_handler.db_name),
                                                        shard_config=repr(self.shard_config),
                                                        checkpoint_scrolled=self.checkpoint_scrolled)
        else:
            raise Exception('Thread %s is stopped' % self.name)

    def stop(self):
        self.disable_data_polling()
        self.can_run = False

    def disable_data_polling(self):
        self.can_poll_data = False

    def upsert_failed_doc(self, error_doc):
        self.document_handler.upsert_doc(_id=error_doc['_id'])

    def get_docs(self):
        docs_to_upsert = []

        def get_doc():
            get_doc_start = time.time()
            while self.docs.qsize() > 0 or self.can_poll_data:
                try:
                    if self.can_run:
                        return self.docs.get(timeout=0.5)
                    else:
                        raise Exception('Thread %s is stopped' % self.name)
                except Empty:
                    self.logger.warning("Waiting for message in shard config %r checkpoint_scrolled %r" % (
                        self.shard_config, self.checkpoint_scrolled))
                    if time.time() - get_doc_start > 600:
                        get_doc_start = time.time()
                        OplogAlert(OplogErrorTag.BULK_DUMP_STUCK, CodeAlertDelivery.SLACK,
                                   CodeAlertLevel.WARNING).send(log_tag='waiting_for_msg',
                                                                db_name=repr(
                                                                    self.document_handler.db_name),
                                                                shard_config=repr(self.shard_config),
                                                                checkpoint_scrolled=self.checkpoint_scrolled)
            raise DataPollingStoppedException('All Data Scrolled')

        while True:
            try:
                doc = get_doc()
                if doc:
                    docs_to_upsert.append(doc)
                if len(docs_to_upsert) > self.batch_size:
                    self.checkpoint_scrolled = docs_to_upsert[0]['_id']
                    for doc in docs_to_upsert:
                        yield doc
                    docs_to_upsert = []
            except (Empty, DataPollingStoppedException) as e:
                if len(docs_to_upsert) > 0:
                    self.checkpoint_scrolled = docs_to_upsert[0]['_id']
                    for doc in docs_to_upsert:
                        yield doc
                    docs_to_upsert = []
                elif self.can_poll_data:
                    self.logger.exception("Failed to get users: %r" % repr(e))
                    self.stop()
                    raise
                break

    def run(self):
        self.started = True
        while self.can_run and (self.docs.qsize() > 0 or self.can_poll_data):
            try:
                for error_doc in self.doc_manager.bulk_upsert(self.get_docs()):
                    self.logger.info("Upserting failed doc error_doc %r" % error_doc)
                    self.upsert_failed_doc(error_doc)
            except Exception, e:
                self.logger.exception("Collection dump for thread %r failed due to exception: %r" % (self.name, e))
                self.stop()
                raise
        self.logger.info("Returning from processing thread name %r " % self.name)
