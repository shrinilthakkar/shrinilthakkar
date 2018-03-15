import datetime
import time

from moengage.sentry.logger import SentryTreysor
from pymongo.errors import DuplicateKeyError

from moengage.commons.decorators import MemCached, Retry
from moengage.commons.exceptions import MONGO_NETWORK_ERRORS
from moengage.commons.utils.common import CommonUtils
from moengage.sentry.alerts import SentryOperationTag
from moengage.sentry.dao import SentryAppDAO, SentryServiceDAO
from moengage.sentry.exceptions import SentryValueError, SentryAppNotFoundException
from moengage.sentry.model import SentryAppDocument, SentryStatus, SentryServiceDocument
from moengage.sentry.utils import LogSentryInterfaceOperations
from moengage.services.app_service import AppService


class SentryService(object):
    def __init__(self, app_key=None, db_name=None):
        self.app_key = app_key
        self.db_name = db_name
        if (not self.db_name and not self.app_key) or (self.db_name and self.app_key):
            raise SentryValueError("Sentry needs to be initialized with only one of app_key or db_name")

    @Retry(MONGO_NETWORK_ERRORS, after=30, max_retries=5)
    def _getSentryDocument(self, sentry_dao):
        if self.app_key:
            return sentry_dao.findByAppKey(self.app_key)
        elif self.db_name:
            return sentry_dao.findByDBName(self.db_name)

    def _getAppObject(self):
        app = None
        if self.app_key:
            app = AppService().getAppByAppKey(self.app_key)
        elif self.db_name:
            app = AppService().getAppByDBName(self.db_name)
        if not app:
            raise SentryAppNotFoundException("Failed to fetch app object for db_name: %s, app_key: %s" % (self.db_name,
                                                                                                          self.app_key))
        return app

    def _getAppCacheKey(self):
        primary_key = self.app_key or self.db_name
        return 'sentry_app_' + CommonUtils.encodeValue(primary_key)

    def _getServiceCacheKey(self, service_name):
        primary_key = self.app_key or self.db_name
        return 'sentry_service_' + CommonUtils.encodeValue(primary_key) + '_' + str(service_name)

    def _createSentryDocument(self, app_object, document_model, status):
        sentry_doc = document_model(app_key=app_object.app_key, db_name=app_object.db_name)
        sentry_doc.creation_time = datetime.datetime.utcnow()
        sentry_doc = self._updateSentryStatus(sentry_doc, status)
        return sentry_doc

    def _updateSentryStatus(self, sentry_doc, status):
        sentry_doc.status = status
        if status == SentryStatus.ALLOWED:
            sentry_doc.last_enable_time = datetime.datetime.utcnow()
        elif status == SentryStatus.BLOCKED:
            sentry_doc.last_disable_time = datetime.datetime.utcnow()
        return sentry_doc

    @LogSentryInterfaceOperations(SentryOperationTag.SENTRY_GET_FAILED)
    @Retry(MONGO_NETWORK_ERRORS, after=30, max_retries=5)
    def getAppDocument(self, ttl=1000):
        @MemCached(self._getAppCacheKey(), secs_to_refresh=ttl)
        def get_app_document():
            return self._getSentryDocument(SentryAppDAO())

        return get_app_document()

    @LogSentryInterfaceOperations(SentryOperationTag.SENTRY_GET_FAILED)
    def getServiceDocument(self, service_name, ttl=1000):
        @MemCached(self._getServiceCacheKey(service_name), secs_to_refresh=ttl)
        def get_service_document():
            return self._getSentryDocument(SentryServiceDAO(service_name=service_name))

        return get_service_document()

    def _getAppDocumentUntilFound(self, max_tries=10):
        tries = 0
        while tries < max_tries:
            time.sleep(0.1)
            sentry_app_doc = self.getAppDocument(ttl=0)
            if sentry_app_doc:
                return sentry_app_doc
            tries += 1
        return None

    def _getSentryDocumentUntilFound(self, service_name, max_tries=10):
        tries = 0
        while tries < max_tries:
            time.sleep(0.1)
            sentry_service_doc = self.getServiceDocument(service_name, ttl=0)
            if sentry_service_doc:
                return sentry_service_doc
            tries += 1
        return None

    @LogSentryInterfaceOperations(SentryOperationTag.SENTRY_OBJECT_CREATION_FAILED)
    @Retry(MONGO_NETWORK_ERRORS, after=30, max_retries=5)
    def createAppDocument(self, status):
        app_object = self._getAppObject()
        try:
            sentry_app_doc = self._createSentryDocument(app_object, SentryAppDocument, status)
            SentryTreysor().info(log_tag='sentry_app_document_created', app_key=app_object.app_key,
                                 db_name=app_object.db_name,
                                 sentry_document=sentry_app_doc.to_dict())
            SentryAppDAO().save(sentry_app_doc)
        except DuplicateKeyError:
            pass
        return self._getAppDocumentUntilFound()

    @LogSentryInterfaceOperations(SentryOperationTag.SENTRY_OBJECT_CREATION_FAILED)
    @Retry(MONGO_NETWORK_ERRORS, after=30, max_retries=5)
    def createServiceDocument(self, service_name, status):
        app_object = self._getAppObject()
        try:
            sentry_service_doc = self._createSentryDocument(app_object, SentryServiceDocument, status)
            sentry_service_doc.service_name = service_name
            SentryTreysor().info(log_tag='sentry_service_document_created', app_key=app_object.app_key,
                                 db_name=app_object.db_name,
                                 sentry_document=sentry_service_doc.to_dict())
            SentryServiceDAO(service_name=service_name).save(sentry_service_doc)
        except DuplicateKeyError:
            pass
        return self._getSentryDocumentUntilFound(service_name)

    @LogSentryInterfaceOperations(SentryOperationTag.SENTRY_STATUS_CHANGE_FAILED)
    @Retry(MONGO_NETWORK_ERRORS, after=30, max_retries=5)
    def updateAppDocument(self, status):
        sentry_app_doc = self.getAppDocument(ttl=0)
        if sentry_app_doc and sentry_app_doc.status != status:
            SentryTreysor().info(log_tag='sentry_app_document_updating',
                                 sentry_document=sentry_app_doc.to_dict())
            sentry_app_doc = self._updateSentryStatus(sentry_app_doc, status)
            SentryTreysor().info(log_tag='sentry_app_document_updated',
                                 sentry_document=sentry_app_doc.to_dict())
            SentryAppDAO().save(sentry_app_doc)
            return self._getAppDocumentUntilFound()
        return sentry_app_doc

    @LogSentryInterfaceOperations(SentryOperationTag.SENTRY_STATUS_CHANGE_FAILED)
    @Retry(MONGO_NETWORK_ERRORS, after=30, max_retries=5)
    def updateServiceDocument(self, service_name, status):
        sentry_service_doc = self.getServiceDocument(service_name, ttl=0)
        if sentry_service_doc and sentry_service_doc.status != status:
            SentryTreysor().info(log_tag='sentry_service_document_updating',
                                 sentry_document=sentry_service_doc.to_dict())
            sentry_service_doc = self._updateSentryStatus(sentry_service_doc, status)
            SentryTreysor().info(log_tag='sentry_service_document_updated',
                                 sentry_document=sentry_service_doc.to_dict())
            SentryServiceDAO(service_name=service_name).save(sentry_service_doc)
            return self._getSentryDocumentUntilFound(service_name)
        return sentry_service_doc
