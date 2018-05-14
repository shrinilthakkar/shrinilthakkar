import datetime

from enum import Enum

from moengage.commons.attrs import DataSource
from moengage.commons.attrs.attribute_model import MOEAttribute
from moengage.commons.attrs.config.provider import ActionInfoProvider, ActionAttrInfoProvider
from moengage.commons.attrs.data_type import DataType


class ActionStatus(Enum):
    BLACKLISTED = 1
    WHITELISTED = 2

    def __str__(self):
        return {
            ActionStatus.BLACKLISTED: "BLACKLISTED",
            ActionStatus.WHITELISTED: "WHITELISTED"
        }.get(self)

    @staticmethod
    def fromStr(status):
        return {
            "BLACKLISTED": ActionStatus.BLACKLISTED,
            "WHITELISTED": ActionStatus.WHITELISTED
        }.get(status)


class MOEAction(MOEAttribute):
    DEFAULT_CATEGORY = 'Tracked User Events'
    INFO_PROVIDER = ActionInfoProvider()

    def __init__(self, **kwargs):
        self._action_status = ActionStatus.WHITELISTED
        self._action_data_source = None
        self.last_received_time = None
        self.blacklisting_time = None
        super(MOEAction, self).__init__(**kwargs)
        if not self.last_received_time:
            self.last_received_time = datetime.datetime.utcnow()

    @property
    def action_status(self):
        return self._action_status

    @action_status.setter
    def action_status(self, action_status):
        self._action_status = ActionStatus.fromStr(action_status) if isinstance(action_status, basestring) \
            else action_status

    @property
    def action_data_source(self):
        return self._action_data_source

    @action_data_source.setter
    def action_data_source(self, data_source):
        self._action_data_source = DataSource.fromStr(data_source) if isinstance(data_source, basestring) \
            else data_source


class MOEActionAttribute(MOEAttribute):
    DEFAULT_CATEGORY = 'Tracked Action Attribute'
    DEFAULT_ATTRIBUTES = [
        MOEAttribute(name='appVersion', data_types=[DataType.STRING]),
        MOEAttribute(name='sdkVersion', data_types=[DataType.STRING]),
        MOEAttribute(name='os', data_types=[DataType.STRING])
    ]
    INFO_PROVIDER = ActionAttrInfoProvider()

    def __init__(self, **kwargs):
        self.action_id = None
        super(MOEActionAttribute, self).__init__(**kwargs)
