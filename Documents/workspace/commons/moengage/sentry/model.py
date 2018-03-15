from moengage.models import SchemaDocument
from enum import Enum


class SentryStatus(Enum):
    ALLOWED = 1
    BLOCKED = 2

    def __str__(self):
        return {
            SentryStatus.ALLOWED: "ALLOWED",
            SentryStatus.BLOCKED: "BLOCKED"
        }.get(self)

    @staticmethod
    def fromValue(value):
        return {
            "ALLOWED": SentryStatus.ALLOWED,
            "BLOCKED": SentryStatus.BLOCKED
        }.get(value)


class SentryAppDocument(SchemaDocument):
    def __init__(self, **kwargs):
        self.app_key = None
        self.db_name = None
        self._status = None
        self.creation_time = None
        self.last_enable_time = None
        self.last_disable_time = None
        super(SentryAppDocument, self).__init__(**kwargs)

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = SentryStatus.fromValue(status) if isinstance(status, basestring) else status


class SentryServiceDocument(SentryAppDocument):
    def __init__(self, **kwargs):
        self.service_name = None
        super(SentryServiceDocument, self).__init__(**kwargs)
