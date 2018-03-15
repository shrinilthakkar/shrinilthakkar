from collections import Counter
from datetime import timedelta, datetime
from threading import Lock

from enum import Enum

from moengage.alerts.email_sender import EmailSender
from moengage.alerts.pager_duty import PagerDutyIncident
from moengage.alerts.slack import SlackMessageSender
from moengage.commons import SingletonMetaClass
from moengage.commons.loggers import Treysor
from moengage.commons.loggers.context_logger import ContextLogger
from moengage.commons.utils import CommonUtils
from moengage.package.utils import PackageUtils


class CodeAlertLevel(Enum):
    ERROR = 1
    WARNING = 2
    INFO = 3

    def __str__(self):
        return {
            CodeAlertLevel.ERROR: 'ERROR',
            CodeAlertLevel.WARNING: 'WARNING',
            CodeAlertLevel.INFO: 'INFO'
        }.get(self, "")

    @staticmethod
    def fromStr(value):
        return {
            'ERROR': CodeAlertLevel.ERROR,
            'WARNING': CodeAlertLevel.WARNING,
            'INFO': CodeAlertLevel.INFO
        }.get(value)


class CodeAlertDelivery(Enum):
    EMAIL = 1
    SLACK = 2
    PAGER_DUTY = 3

    def __str__(self):
        return {
            CodeAlertDelivery.EMAIL: 'email',
            CodeAlertDelivery.SLACK: 'slack',
            CodeAlertDelivery.PAGER_DUTY: 'pager_duty'
        }.get(self, "")

    @staticmethod
    def fromStr(value):
        return {
            'email': CodeAlertDelivery.EMAIL,
            'slack': CodeAlertDelivery.SLACK,
            'pager_duty': CodeAlertDelivery.PAGER_DUTY
        }.get(value)


class CodeAlertConfig(object):
    def __init__(self, tag_names, emails, channel_name, pager_duty_service_key=None):
        """
            Sample values :
            tag_names = {
                CodeAlertLevel.ERROR: '@abc @xyz',
                CodeAlertLevel.WARNING: '@abc @xyz',
                CodeAlertLevel.INFO: '@abc @xyz'
            }
            emails = {
                CodeAlertLevel.ERROR: ['abc@moengage.com', 'xyz@moengage.com'],
                CodeAlertLevel.WARNING: ['abc@moengage.com', 'xyz@moengage.com'],
                CodeAlertLevel.INFO: ['abc@moengage.com', 'xyz@moengage.com']
            }
            channel_name = '#abc'
        """
        self.tag_names = tag_names
        self.emails = emails
        self.channel_name = channel_name
        self.pager_duty_service_key = pager_duty_service_key

    def get_emails(self, code_alert_level):
        return self.emails.get(code_alert_level, [])

    def get_tag_names(self, code_alert_level):
        return self.tag_names.get(code_alert_level, "")

    def get_channel(self):
        return self.channel_name

    @staticmethod
    def get_color(code_alert_level):
        return {
            CodeAlertLevel.ERROR: 'danger',
            CodeAlertLevel.WARNING: 'warning',
            CodeAlertLevel.INFO: 'good'
        }.get(code_alert_level, SlackMessageSender.DEFAULT_COLOR)

    @staticmethod
    def get_icon_url(code_alert_level):
        return {
            CodeAlertLevel.ERROR: 'https://pixabay.com/static/uploads/photo/2014/03/25/15/17/cross-296395_960_720.png',
            CodeAlertLevel.WARNING: 'http://www.freeiconspng.com/uploads/warning-icon-24.png',
            CodeAlertLevel.INFO: 'https://upload.wikimedia.org/wikipedia/commons/6/66/Info_groen.png'
        }.get(code_alert_level, "")


class CodeAlertEmail(object):
    def __init__(self, alert_level, alert_type, alert_config):
        self.alert_type = alert_type
        self.alert_level = alert_level
        self.alert_config = alert_config

    def __bold(self, text):
        return "<b>" + text + "</b>"

    def __keyValueToStr(self, key, value):
        return self.__bold(str(key)) + ' : ' + str(CommonUtils.encodeValue(value))

    def __buildBody(self, **kwargs):
        body = ""
        for key in sorted(kwargs.keys()):
            body += self.__keyValueToStr(key, kwargs.get(key))
            body += '<br/>'
        return body

    def send(self, **kwargs):
        to_emails = self.alert_config.get_emails(self.alert_level)
        alert_class_name = kwargs.pop('alert_class_name')
        subject = alert_class_name + " - " + str(self.alert_level) + " - " + str(self.alert_type)
        body = self.__buildBody(**kwargs)
        EmailSender.sendEmail(subject, body, to_emails)


class CodeAlertSlackMessage(object):
    def __init__(self, alert_level, alert_type, alert_config):
        self.alert_type = alert_type
        self.alert_level = alert_level
        self.alert_config = alert_config

    def send(self, **kwargs):
        sender = SlackMessageSender(self.alert_config.get_channel())
        alert_class_name = kwargs.pop('alert_class_name')
        message = alert_class_name + " - " + str(self.alert_level) + " - " + str(self.alert_type)
        sender.sendDictMessage(message, alert_level=self.alert_level, alert_config=self.alert_config, **kwargs)


class CodeAlertPagerDuty(object):
    def __init__(self, alert_type, alert_level, alert_config):
        self.alert_type = alert_type
        self.alert_level = alert_level
        self.alert_config = alert_config

    def send(self, **kwargs):
        service_key = self.alert_config.pager_duty_service_key
        if not service_key:
            raise ValueError("Invalid pager duty service key")
        sender = PagerDutyIncident(service_key=service_key)
        if not kwargs.get('incident_key'):
            kwargs.update({'incident_key': str(self.alert_type)})
        kwargs.pop('alert_class_name', None)
        return sender.trigger(**kwargs)


class CodeAlert(object):
    def __init__(self, alert_type, alert_delivery, alert_level, alert_config):
        self.alert_type = alert_type
        self.alert_delivery = alert_delivery
        self.alert_level = alert_level
        self.alert_config = alert_config
        if not isinstance(self.alert_config, CodeAlertConfig):
            raise ValueError("alert_config must be an instance of moengage.alerts.CodeAlertConfig")

    def __senderForAlertDelivery(self, alert_delivery):
        return {
            CodeAlertDelivery.EMAIL: CodeAlertEmail,
            CodeAlertDelivery.SLACK: CodeAlertSlackMessage,
            CodeAlertDelivery.PAGER_DUTY: CodeAlertPagerDuty
        }.get(alert_delivery, CodeAlertEmail)

    def send(self, **kwargs):
        kwargs.update(ContextLogger().logger.getContext())
        kwargs.update({'server_timestamp': datetime.utcnow().isoformat()})
        if CommonUtils.getEnv() != 'prod' or PackageUtils.getPackageEnv() == 'dev':
            kwargs.pop('alert_log_tag', None)
            Treysor().warning(alert_log_tag="code_alert_skipped", **kwargs)
            return
        try:
            sender_class = self.__senderForAlertDelivery(self.alert_delivery)
            sender = sender_class(self.alert_level, self.alert_type, self.alert_config)
            return sender.send(alert_class_name=self.__class__.__name__, **kwargs)
        except Exception:
            Treysor().warning(alert_log_tag="code_alert_sending_failed", exception=CommonUtils.view_traceback())

    def sendFrequencyControlled(self, key, alert_frequency_seconds=180, **kwargs):
        AlertFrequencyChecker.incrementCounterValue(key, self.alert_type)
        if AlertFrequencyChecker.sendAllowed(key, self.alert_type, alert_frequency_seconds=alert_frequency_seconds):
            kwargs.update({'frequency_counter': AlertFrequencyChecker.getCounterValue(key, self.alert_type)})
            self.send(**kwargs)
            AlertFrequencyChecker.resetCounterValue(key, self.alert_type)


class AlertFrequencyChecker(object):
    __metaclass__ = SingletonMetaClass
    alert_last_sent = {}
    thread_lock = Lock()
    alert_frequency_counter = Counter()

    @staticmethod
    def getAlertKey(key, alert_type):
        return str(key) + '_' + str(alert_type)

    @staticmethod
    def sendAllowed(key, alert_type, alert_frequency_seconds=180):
        with AlertFrequencyChecker.thread_lock:
            alert_sending_threshold = datetime.utcnow() - timedelta(seconds=alert_frequency_seconds)
            alert_key = AlertFrequencyChecker.getAlertKey(key, alert_type)
            last_sent_time = AlertFrequencyChecker.alert_last_sent.get(alert_key, alert_sending_threshold)
            if last_sent_time <= alert_sending_threshold:
                AlertFrequencyChecker.alert_last_sent[alert_key] = datetime.utcnow()
                return True
        return False

    @staticmethod
    def getCounterValue(key, alert_type):
        with AlertFrequencyChecker.thread_lock:
            return AlertFrequencyChecker.alert_frequency_counter.get(AlertFrequencyChecker.getAlertKey(key,
                                                                                                       alert_type), 0)

    @staticmethod
    def incrementCounterValue(key, alert_type):
        with AlertFrequencyChecker.thread_lock:
            AlertFrequencyChecker.alert_frequency_counter[AlertFrequencyChecker.getAlertKey(key, alert_type)] += 1

    @staticmethod
    def resetCounterValue(key, alert_type):
        with AlertFrequencyChecker.thread_lock:
            AlertFrequencyChecker.alert_frequency_counter[AlertFrequencyChecker.getAlertKey(key, alert_type)] = 0
