from enum import Enum

from moengage.alerts import CodeAlert, CodeAlertConfig, CodeAlertLevel, CodeAlertDelivery

# created dummy variable to allow CodeAlertDelivery import in other classes from alerts
# without dummy var CodeAlertDelivery will be removed in auto format
_dummy_var = CodeAlertDelivery


class OplogErrorTag(Enum):
    BULK_DUMP_STUCK = 1

    def __str__(self):
        return {
            OplogErrorTag.BULK_DUMP_STUCK: 'BULK_DUMP_STUCK',
        }.get(self)

    @staticmethod
    def fromStr(value):
        return {
            'BULK_DUMP_STUCK': OplogErrorTag.BULK_DUMP_STUCK,
        }.get(value)


class OplogAlert(CodeAlert):
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
        super(OplogAlert, self).__init__(alert_type=alert_type, alert_delivery=alert_delivery,
                                         alert_level=alert_level, alert_config=alert_config)
