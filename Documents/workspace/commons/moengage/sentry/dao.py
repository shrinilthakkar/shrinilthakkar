from moengage.commons.connections import InfraType
from moengage.daos import BaseDAO
from moengage.models.index import Index
from moengage.sentry.model import SentryAppDocument, SentryServiceDocument


class SentryAppDAO(BaseDAO):
    SENTRY_COLLECTION_NAME = 'MOESentryApps'
    SENTRY_DB_NAME = 'moengage'
    INDEXES = [
        Index(fields=['db_name'], unique=True),
        Index(fields=['app_key'], unique=True)
    ]

    def __init__(self, model_class=SentryAppDocument):
        super(SentryAppDAO, self).__init__(SentryAppDAO.SENTRY_DB_NAME, SentryAppDAO.SENTRY_COLLECTION_NAME,
                                           model_class=model_class, infra_type=InfraType.DEFAULT, indexes=self.INDEXES)

    def findByDBName(self, db_name, **kwargs):
        return self.findOne({'db_name': db_name}, **kwargs)

    def findByAppKey(self, app_key, **kwargs):
        return self.findOne({'app_key': app_key}, **kwargs)


class SentryServiceDAO(BaseDAO):
    SENTRY_COLLECTION_NAME = 'MOESentryServices'
    SENTRY_DB_NAME = 'moengage'
    INDEXES = [
        Index(fields=['db_name']),
        Index(fields=['db_name', 'service_name'], unique=True),
        Index(fields=['app_key']),
        Index(fields=['app_key', 'service_name'], unique=True)
    ]

    def __init__(self, service_name, model_class=SentryServiceDocument):
        super(SentryServiceDAO, self).__init__(SentryServiceDAO.SENTRY_DB_NAME, SentryServiceDAO.SENTRY_COLLECTION_NAME,
                                               model_class=model_class, infra_type=InfraType.DEFAULT,
                                               indexes=self.INDEXES)
        self.service_name = str(service_name)

    def findByAppKey(self, app_key, **kwargs):
        return self.findOne({'app_key': app_key, 'service_name': self.service_name}, **kwargs)

    def findByDBName(self, db_name, **kwargs):
        return self.findOne({'db_name': db_name, 'service_name': self.service_name}, **kwargs)
