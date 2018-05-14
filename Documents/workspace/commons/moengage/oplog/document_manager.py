from abc import ABCMeta, abstractmethod

from moengage.commons.loggers.context_logger import ContextLogger


class BaseDocManager(ContextLogger):
    __metaclass__ = ABCMeta

    def __init__(self, db_name, doc_formatter):
        self.db_name = db_name
        self.formatter = doc_formatter

    def apply_update(self, doc, update_spec):
        if "$set" not in update_spec and "$unset" not in update_spec:
            # Don't try to add ns and _ts fields back in from doc
            return update_spec
        return self.formatter.apply_update(doc, update_spec)

    @abstractmethod
    def upsert_docs(self, docs, bulk_size=100):
        raise NotImplementedError(
            "Child Class must implement {0} function from super".format([self.upsert_docs.__name__]))

    @abstractmethod
    def upsert_doc(self, doc):
        raise NotImplementedError(
            "Child Class must implement {0} function from super".format([self.upsert_doc.__name__]))

    @abstractmethod
    def remove(self, doc_id):
        raise NotImplementedError(
            "Child Class must implement {0} function from super".format([self.remove.__name__]))

    @abstractmethod
    def get_document(self, doc_id):
        raise NotImplementedError(
            "Child Class must implement {0} function from super".format([self.get_document.__name__]))

    @abstractmethod
    def get_bulk_document(self, doc_ids):
        raise NotImplementedError(
            "Child Class must implement {0} function from super".format([self.get_document.__name__]))

    def get_updated_doc(self, document_id, update_spec, doc=None):
        if not doc:
            document = self.get_document(document_id)
        else:
            document = doc
        if update_spec is not None:
            self.apply_update(document, update_spec)
        return document

    def update(self, document_id, update_spec, doc=None):
        try:
            document = self.get_updated_doc(document_id, update_spec, doc)
            self.upsert_doc(document)
        except Exception as e:
            self.logger.exception('Error in create_or_update %r ' % e)
            raise

    def bulk_upsert(self, docs, bulk_size=100):
        docs_to_upsert = []
        count = 0
        for doc in docs:
            count += 1
            docs_to_upsert.append(doc)
            if len(docs_to_upsert) > bulk_size:
                error_docs = self.upsert_docs(docs_to_upsert, bulk_size=bulk_size)
                docs_to_upsert = []
                for error_doc in error_docs:
                    yield error_doc
                self.logger.info("Bulk upserted %d docs" % count)
        if len(docs_to_upsert) > 0:
            error_docs = self.upsert_docs(docs_to_upsert)
            docs_to_upsert = []
            for error_doc in error_docs:
                yield error_doc
        self.logger.info("Finished bulk upsert docs upserted %d" % count)
