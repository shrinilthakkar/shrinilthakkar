from enum import Enum

from moengage.alerts import CodeAlert, CodeAlertConfig, CodeAlertLevel, CodeAlertDelivery

# created dummy variable to allow CodeAlertDelivery import in other classes from alerts
# without dummy var CodeAlertDelivery will be removed in auto format
_dummy_var = CodeAlertDelivery


class SentryOperationTag(Enum):
    SENTRY_STATUS_CHANGE_FAILED = 1
    SENTRY_GET_FAILED = 2
    SENTRY_OBJECT_CREATION_FAILED = 3

    def __str__(self):
        return {
            SentryOperationTag.SENTRY_STATUS_CHANGE_FAILED: 'sentry_status_change_failed',
            SentryOperationTag.SENTRY_GET_FAILED: 'sentry_get_failed',
            SentryOperationTag.SENTRY_OBJECT_CREATION_FAILED: 'sentry_object_creation_failed'
        }.get(self)

    @staticmethod
    def fromStr(value):
        return {
            'sentry_status_change_failed': SentryOperationTag.SENTRY_STATUS_CHANGE_FAILED,
            'sentry_get_failed': SentryOperationTag.SENTRY_GET_FAILED,
            'sentry_object_creation_failed': SentryOperationTag.SENTRY_OBJECT_CREATION_FAILED
        }.get(value)


class SentryAlert(CodeAlert):
    def __init__(self, alert_type, alert_delivery, alert_level):
        tag_names = {
            CodeAlertLevel.ERROR: '@mshekhar @akshaygoel',
            CodeAlertLevel.WARNING: '@mshekhar @akshaygoel',
            CodeAlertLevel.INFO: ''
        }
        emails = {
            CodeAlertLevel.ERROR: ["mayank@moengage.com", "akshay@moengage.com"],
            CodeAlertLevel.WARNING: ["mayank@moengage.com", "akshay@moengage.com"],
            CodeAlertLevel.INFO: ["mayank@moengage.com", "akshay@moengage.com"]
        }
        channel_name = '#segmentation-alerts'
        alert_config = CodeAlertConfig(tag_names=tag_names, emails=emails, channel_name=channel_name)
        super(SentryAlert, self).__init__(alert_type=alert_type, alert_delivery=alert_delivery,
                                          alert_level=alert_level, alert_config=alert_config)
