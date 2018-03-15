from enum import Enum

from moengage.alerts import CodeAlert, CodeAlertConfig, CodeAlertLevel, CodeAlertDelivery

# created dummy variable to allow CodeAlertDelivery import in other classes from alerts
# without dummy var CodeAlertDelivery will be removed in auto format
_dummy_var = CodeAlertDelivery


class PackageOperationType(Enum):
    CONFIG_GENERATION_FAILED = 1
    CONFIG_DOWNLOAD_FAILED = 2

    def __str__(self):
        return {
            PackageOperationType.CONFIG_GENERATION_FAILED: 'CONFIG_GENERATION_FAILED',
            PackageOperationType.CONFIG_DOWNLOAD_FAILED: 'CONFIG_DOWNLOAD_FAILED'
        }.get(self)

    @staticmethod
    def fromStr(value):
        return {
            'CONFIG_GENERATION_FAILED': PackageOperationType.CONFIG_GENERATION_FAILED,
            'CONFIG_DOWNLOAD_FAILED': PackageOperationType.CONFIG_DOWNLOAD_FAILED
        }.get(value)


class PackageOperationAlert(CodeAlert):
    def __init__(self, alert_type, alert_delivery, alert_level):
        tag_names = {
            CodeAlertLevel.ERROR: '@mshekhar @akshaygoel',
            CodeAlertLevel.WARNING: '@mshekhar @akshaygoel',
            CodeAlertLevel.INFO: ''
        }
        emails = {
            CodeAlertLevel.ERROR: ['devops@moengage.com', 'segmentationteam@moengage.com'],
            CodeAlertLevel.WARNING: ['devops@moengage.com', 'segmentationteam@moengage.com'],
            CodeAlertLevel.INFO: ['devops@moengage.com', 'segmentationteam@moengage.com']
        }
        channel_name = '#package_builds'
        alert_config = CodeAlertConfig(tag_names=tag_names, emails=emails, channel_name=channel_name)
        super(PackageOperationAlert, self).__init__(alert_type=alert_type, alert_delivery=alert_delivery,
                                                    alert_level=alert_level, alert_config=alert_config)
