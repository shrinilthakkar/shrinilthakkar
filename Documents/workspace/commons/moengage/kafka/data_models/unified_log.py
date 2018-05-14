from enum import Enum

from moengage.models.base import SimpleDocument


class OS(Enum):
    ANDROID = 1
    IOS = 2
    WINDOWS = 3
    WEB = 4
    MWEB = 5
    EMAIL = 6
    WEBHOOK = 7

    def __str__(self):
        return {
            OS.ANDROID: "A",
            OS.IOS: "I",
            OS.WINDOWS: "W",
            OS.MWEB: "M",
            OS.WEB: "D",
            OS.EMAIL: "E",
            OS.WEBHOOK: "H"
        }.get(self)

    @staticmethod
    def fromStr(value):
        return {
            "A": OS.ANDROID,
            "I": OS.IOS,
            "W": OS.WINDOWS,
            "M": OS.MWEB,
            "D": OS.WEB,
            "E": OS.EMAIL,
            "H": OS.WEBHOOK
        }.get(value)

    def to_moe_os(self):
        return {
            OS.ANDROID: "ANDROID",
            OS.IOS: "iOS",
            OS.WINDOWS: "Windows",
            OS.MWEB: "mweb",
            OS.WEB: "web",
            OS.EMAIL: "email",
            OS.WEBHOOK: "webhook"
        }.get(self)


class Action(SimpleDocument):
    def __init__(self, **kwargs):
        self.n = None
        self.t = None
        self.a = None
        super(Action, self).__init__(**kwargs)


class UnifiedLog(SimpleDocument):
    def __init__(self, **kwargs):
        self.unique_id = None
        self.DBname = None
        self.appId = None
        self.user_id = None
        self.push_id = None
        self.sdk = None
        self._os = None
        self.uattr = None
        self.notic = None
        self._action = None
        self.ov = None
        self.sv = None
        self.av = None
        self.tz = None
        self.tzo = None
        super(UnifiedLog, self).__init__(**kwargs)

    @property
    def os(self):
        return self._os

    @os.setter
    def os(self, value):
        self._os = OS.fromStr(str(value)) if isinstance(value, basestring) else value

    @property
    def action(self):
        return self._action

    @action.setter
    def action(self, actions):
        if actions and isinstance(actions, list):
            self._action = map(lambda a: Action(**a), actions)
        else:
            self._action = actions
