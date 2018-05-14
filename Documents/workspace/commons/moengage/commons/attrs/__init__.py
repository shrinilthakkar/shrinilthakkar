from enum import Enum


class DataSource(Enum):
    SDK = 1
    S2S = 2
    INTERNAL = 3

    def __str__(self):
        return {
            DataSource.SDK: "SDK",
            DataSource.S2S: "S2S",
            DataSource.INTERNAL: "INTERNAL"
        }.get(self)

    @staticmethod
    def fromStr(value):
        return {
            "SDK": DataSource.SDK,
            "S2S": DataSource.S2S,
            "INTERNAL": DataSource.INTERNAL
        }.get(value)
