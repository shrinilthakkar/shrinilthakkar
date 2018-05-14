import hashlib
import json

from moengage.commons.utils.common import CommonUtils


class HashUtils(object):
    @classmethod
    def generateHash(cls, value):
        if isinstance(value, list) or isinstance(value, set) or isinstance(value, tuple):
            return cls._generateHashForListValue(value)
        elif isinstance(value, dict):
            dict_for_hashing = {k: cls.generateHash(v) for k, v in value.items()}
            return cls._hash(dict_for_hashing)
        return cls._hash(value)

    @classmethod
    def _generateHashForListValue(cls, list_value):
        value_hashes = map(cls.generateHash, list_value)
        value_hashes.sort()
        return cls._hash(value_hashes)

    @classmethod
    def _hash(cls, value):
        json_dump_hashes = json.dumps(value, default=CommonUtils.serializable, sort_keys=True)
        return hashlib.md5(json_dump_hashes).hexdigest()
