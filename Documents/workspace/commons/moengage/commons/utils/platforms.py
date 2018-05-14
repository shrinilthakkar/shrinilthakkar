from enum import Enum


class Platforms(Enum):
    ANDROID = 0
    IOS = 1
    WEB = 2
    WINDOWS = 3
    M_WEB = 4
    WEB_HOOK = 5
    SMS = 6
    UNKNOWN = 7

    def __str__(self):
        return {
            Platforms.ANDROID: "ANDROID",
            Platforms.IOS: "iOS",
            Platforms.WEB: "web",
            Platforms.WINDOWS: "Windows",
            Platforms.M_WEB: "mweb",
            Platforms.WEB_HOOK: "webhook",
            Platforms.SMS: "sms",
            Platforms.UNKNOWN: "unknown"
        }.get(self)

    @staticmethod
    def fromStr(value):
        return {
            "android": Platforms.ANDROID,
            "ios": Platforms.IOS,
            "web": Platforms.WEB,
            "windows": Platforms.WINDOWS,
            "mweb": Platforms.M_WEB,
            "webhook": Platforms.WEB_HOOK,
            "sms": Platforms.SMS,
            "unknown": Platforms.UNKNOWN
        }.get(value.lower(), None)
