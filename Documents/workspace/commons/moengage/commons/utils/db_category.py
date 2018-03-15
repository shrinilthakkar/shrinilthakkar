from enum import Enum


class DBCategory(Enum):
    XSMALL = 0
    SMALL = 1
    MEDIUM = 2
    LARGE = 3
    XLARGE = 4

    def count(self):
        try:
            return self._count
        except Exception:
            return 0

    def setCount(self, count):
        self._count = count

    def pipelineThrottle(self):
        try:
            return self._throttle
        except Exception:
            return 10

    def setPipelineThrottle(self, value):
        self._throttle = value

    def min(self):
        try:
            return self._min
        except Exception:
            return 0

    def setMin(self, value):
        self._min = value

    def max(self):
        try:
            return self._max
        except Exception:
            return 0

    def setMax(self, value):
        self._max = value

    def __str__(self):
        return {
            DBCategory.XSMALL: "xsmall",
            DBCategory.SMALL: "small",
            DBCategory.MEDIUM: "medium",
            DBCategory.LARGE: "large",
            DBCategory.XLARGE: "xlarge"
        }.get(self, None)

    @staticmethod
    def fromStr(category):
        return {
            "xsmall": DBCategory.XSMALL,
            "small": DBCategory.SMALL,
            "medium": DBCategory.MEDIUM,
            "large": DBCategory.LARGE,
            "xlarge": DBCategory.XLARGE
        }.get(category, None)
