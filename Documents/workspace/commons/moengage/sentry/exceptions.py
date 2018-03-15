class SentryException(Exception):
    pass


class SentryValueError(SentryException):
    pass


class SentryDocumentNotFoundException(SentryException):
    pass


class SentryAppNotFoundException(SentryException):
    pass
