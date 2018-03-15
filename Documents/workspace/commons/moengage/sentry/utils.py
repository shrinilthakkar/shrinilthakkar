import sys
from functools import wraps

from moengage.commons.utils import CommonUtils
from moengage.sentry.alerts import SentryAlert, CodeAlertDelivery, CodeAlertLevel
from moengage.sentry.exceptions import SentryException
from moengage.sentry.logger import SentryTreysor


class LogSentryInterfaceOperations(object):
    def __init__(self, log_type):
        self.log_type = log_type

    def __call__(self, func):
        @wraps(func)
        def logger(*args, **kwargs):
            try:
                return_value = func(*args, **kwargs)
            except Exception as e:
                error_string = CommonUtils.view_traceback()
                SentryTreysor().exception(log_type=str(self.log_type),
                                          func_name=func.__name__, error_string=error_string)
                if not isinstance(e, SentryException):
                    SentryAlert(self.log_type, CodeAlertDelivery.SLACK,
                                CodeAlertLevel.ERROR).send(exception=CommonUtils.view_traceback(),
                                                           func_name=func.__name__,
                                                           **SentryTreysor().getContext())
                    raise SentryException, e.message, sys.exc_info()[2]
                else:
                    raise
            return return_value

        return logger
