from moengage.sentry.exceptions import SentryDocumentNotFoundException, SentryValueError
from moengage.sentry.model import SentryStatus
from moengage.sentry.service import SentryService


class Sentry(object):
    def __init__(self, app_key=None, db_name=None):
        self.app_key = app_key
        self.db_name = db_name
        if (not self.db_name and not self.app_key) or (self.db_name and self.app_key):
            raise SentryValueError("Sentry needs to be initialized with only one of app_key or db_name")
        self.sentry_service = SentryService(app_key=self.app_key, db_name=self.db_name)

    def app_status(self, default_status=None):
        sentry_doc = self.sentry_service.getAppDocument()
        if not sentry_doc:
            if default_status is not None:
                sentry_doc = self.sentry_service.createAppDocument(status=default_status)
                return default_status if not sentry_doc else sentry_doc.status
            else:
                raise SentryDocumentNotFoundException("Sentry document not found for db_name: %r,"
                                                      "app_key: %r" % (self.db_name, self.app_key))
        return sentry_doc.status

    def set_app_status(self, status):
        sentry_doc = self.sentry_service.updateAppDocument(status)
        if not sentry_doc:
            sentry_doc = self.sentry_service.createAppDocument(status=status)
        return sentry_doc.status if sentry_doc else status

    def service_status(self, service_name, default_status=None):
        sentry_doc = self.sentry_service.getServiceDocument(service_name=service_name)
        if not sentry_doc:
            if default_status is not None:
                sentry_doc = self.sentry_service.createServiceDocument(service_name=service_name, status=default_status)
                return default_status if not sentry_doc else sentry_doc.status
            else:
                raise SentryDocumentNotFoundException("Sentry document not found for db_name: %r, app_key: %r,"
                                                      "service_name: %r" % (self.db_name, self.app_key, service_name))
        return sentry_doc.status

    def set_service_status(self, service_name, status):
        sentry_doc = self.sentry_service.updateServiceDocument(service_name, status)
        if not sentry_doc:
            sentry_doc = self.sentry_service.createServiceDocument(service_name=service_name, status=status)
        return sentry_doc.status if sentry_doc else status

    def app_service_status(self, service_name, default_status=None):
        app_status = self.app_status(default_status=SentryStatus.ALLOWED)
        if app_status == SentryStatus.ALLOWED:
            service_status = self.service_status(service_name, default_status=default_status)
            return service_status
        return app_status
