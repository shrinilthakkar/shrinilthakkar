from bson import ObjectId

from moengage.commons.connections import ConnectionUtils
from moengage.commons.decorators.retry import Retry
from moengage.commons.exceptions import MONGO_NETWORK_ERRORS
from moengage.commons.loggers.context_logger import ContextLogger


class CollectionDocumentHandlerBase(ContextLogger):
    def __init__(self, doc_manager, db_name, collection, doc_formatter):
        self.doc_manager = doc_manager
        self.db_name = db_name
        self.collection = collection
        self.doc_formatter = doc_formatter

    def get_collection(self):
        return ConnectionUtils.getMongoConnectionForDBName(self.db_name)[self.db_name][self.collection]

    @Retry(MONGO_NETWORK_ERRORS, max_retries=5, after=10, default_value={})
    def get_failed_doc(self, doc_id):
        target_coll = self.get_collection()
        doc = target_coll.find_one({"_id": ObjectId(doc_id)}, max_time_ms=2000)
        if doc:
            self.doc_formatter.pop_excluded_fields(doc, self.db_name, self.collection)
        else:
            self.logger.warning("Could not find document with id %s from mongodb", doc_id)
        return doc

    def parse_doc_before_upsert(self, doc, error):
        return doc

    def upsert_doc(self, _id=None, doc=None, retry_count=0, error=None):
        doc_to_upsert = doc
        try:
            if not doc_to_upsert and _id:
                doc_to_upsert = self.get_failed_doc(_id)
            if error:
                doc_to_upsert = self.parse_doc_before_upsert(doc_to_upsert, error)
            if retry_count > 3:
                raise Exception("Hit maximum retry attempts when trying to insert document: %r" % doc_to_upsert)
            if doc_to_upsert:
                error = self.doc_manager.upsert_doc(doc_to_upsert)
                if error:
                    self.upsert_doc(_id=_id, retry_count=retry_count + 1, error=error)
            else:
                self.logger.warning("Empty Document found: %r" % _id)
        except Exception, e:
            self.logger.exception("Failed to upsert document: %r due to error: %r" % (doc_to_upsert, e))
