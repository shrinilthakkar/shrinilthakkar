import datetime
import logging
import time
from functools import wraps
from threading import Lock

from apscheduler.schedulers.background import BackgroundScheduler
from enum import Enum

from moengage.commons import SingletonMetaClass
from moengage.commons.cache.cache_manager import CacheClient
from moengage.commons.loggers.treysor import Treysor
from moengage.commons.threadsafe import EXECUTION_CONTEXT
from moengage.commons.utils import CommonUtils


class CacheType(Enum):
    INMEMORY = 1
    REDIS = 2
    PERMANENT = 3

    def __str__(self):
        return {
            CacheType.INMEMORY: "inmemory",
            CacheType.REDIS: "redis",
            CacheType.PERMANENT: "permanent"
        }.get(self)

    @staticmethod
    def fromStr(value):
        return {
            "inmemory": CacheType.INMEMORY,
            "redis": CacheType.REDIS,
            "permanent": CacheType.PERMANENT
        }.get(value)

    def dataClass(self):
        return {
            CacheType.INMEMORY: MemCachedData,
            CacheType.REDIS: RedisData,
            CacheType.PERMANENT: PermanentCacheData
        }.get(self, MemCachedData)


class RedisData(object):
    def __init__(self):
        self.redis = None
        self.value = None
        self.value_fetched = False

    def __get_redis(self, key):
        if not self.redis:
            cache_type = key.split('_', 1)[0]
            self.redis = CacheClient({'type': cache_type})
        return self.redis

    @classmethod
    def get_data_key(cls, key, func=None):
        return key

    def get(self, key):
        if not self.value and not self.value_fetched:
            self.value = self.__get_redis(key).get(key)
            self.value_fetched = True
        return self.value

    def remove_key(self, key):
        self.__get_redis(key).delete(key)

    def has_key(self, key):
        return bool(self.get(key))

    def get_value(self, key):
        value = self.get(key)
        if value:
            return value.get('data')
        else:
            return value

    def set_value(self, key, value, fetch_time, ttl=86400):
        self.__get_redis(key).set(key, dict(data=value, fetch_time=fetch_time), expiry=ttl)

    def reset_value(self, key):
        self.remove_key(key)

    def is_value_none(self, key):
        value = self.get(key)
        if value and value.get('data') is None:
            return True
        return False

    def is_key_expired(self, key, seconds_to_refresh):
        value = self.get(key)
        if not value:
            return True
        fetch_time = value.get('fetch_time')
        return (fetch_time + datetime.timedelta(seconds=seconds_to_refresh)) < datetime.datetime.utcnow()


class MemCachedData(object):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        self._data = {}
        MemCachedGC()

    @classmethod
    def get_data_key(cls, key, func=None):
        if func:
            return CommonUtils.getClassNameForMethod(func) + "_" + key
        else:
            return key

    def get_keys(self):
        return self._data.keys()

    def remove_key(self, key):
        self._data.pop(key, None)

    def has_key(self, key):
        return key in self._data

    def get_value(self, key):
        key_data = self._data.get(key, {})
        if key_data and isinstance(key_data, dict):
            key_data['last_accessed'] = datetime.datetime.utcnow()
        return key_data.get('data')

    def get_last_accessed(self, key):
        key_data = self._data.get(key, {})
        return key_data.get('last_accessed', datetime.datetime.utcfromtimestamp(0))

    def set_value(self, key, value, fetch_time, ttl=0):
        data_dict = self._data.get(key, {})
        data_dict['data'] = value
        data_dict['fetch_time'] = fetch_time
        self._data[key] = data_dict

    def reset_value(self, key):
        data_dict = self._data.get(key, {})
        data_dict['fetch_time'] = datetime.datetime.utcfromtimestamp(0)
        self._data[key] = data_dict

    def is_value_none(self, key):
        value = self._data.get(key, {}).get('data')
        if key in self._data and value is None:
            return True
        return False

    def is_key_expired(self, key, seconds_to_refresh):
        if not self._data.get(key):
            return True
        fetch_time = self._data.get(key, {}).get('fetch_time')
        return (fetch_time + datetime.timedelta(seconds=seconds_to_refresh)) < datetime.datetime.utcnow()


class PermanentCacheData(object):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        self._cache_key = 'permanent_cache_data_key'
        EXECUTION_CONTEXT.set(self._cache_key, {})
        self._data = EXECUTION_CONTEXT.get(self._cache_key)

    @classmethod
    def get_data_key(cls, key, func=None):
        return MemCachedData.get_data_key(key, func=func)

    def get_keys(self):
        return self._data.keys()

    def remove_key(self, key):
        self._data.pop(key, None)

    def has_key(self, key):
        return key in self._data

    def get_value(self, key):
        return self._data.get(key)

    def set_value(self, key, value, fetch_time, ttl=0):
        self._data[key] = value

    def reset_value(self, key):
        self._data[key] = None

    def is_value_none(self, key):
        value = self._data.get(key)
        if key in self._data and value is None:
            return True
        return False

    def is_key_expired(self, key, seconds_to_refresh):
        return False


class MemCached(object):
    """
    This decorator caches the calls to functions in-memory for the specified refresh time before executing the next call
    and refreshing the function response.
    Usage:

    Caching the function_result:
    @MemCached(<YOUR UNIQUE_KEY (can be anything)>, <time_to_refresh>)

    Clearing the function_cache:
    MemCached(<YOUR UNIQUE_KEY>).clear(<Reference to function containing the MemCached decorator>)
    """
    init_lock = Lock()
    write_locks = {}

    def __init__(self, key, secs_to_refresh=600, skip_none=False, cache_type=CacheType.INMEMORY):
        self._key = key
        self._seconds_to_refresh = secs_to_refresh
        self._cache_type = cache_type
        self._data = cache_type.dataClass()()
        self.skip_none = skip_none

    @staticmethod
    def createKey(*args):
        try:
            return '_'.join(map(lambda x: str(x), args))
        except UnicodeError:
            return '_'.join(map(lambda x: CommonUtils.encodeValue(x), args))

    # Unused - Added for convenience - Verify before using
    def clear(self, func):
        data_key = self._data.get_data_key(self._key, func)
        with MemCached.write_locks[data_key]:
            self._data.reset_value(data_key)

    # Unused - Added for convenience - Verify before using
    def exists(self, func):
        data_key = self._data.get_data_key(self._key, func)
        return self._data.has_key(data_key)

    def value_needs_refresh(self, data_key):
        expired = self._data.is_key_expired(data_key, self._seconds_to_refresh)
        none = self._data.is_value_none(data_key)
        return expired or (none and self.skip_none)

    def __call__(self, func):
        @wraps(func)
        def get_value(*args, **kwargs):
            data_key = self._data.get_data_key(self._key, func)
            return_value = None
            if self.value_needs_refresh(data_key):
                with MemCached.write_locks.setdefault(data_key, Lock()):
                    if self.value_needs_refresh(data_key):
                        return_value = func(*args, **kwargs)
                        fetch_time = datetime.datetime.utcnow()
                        # If return_value is None and skip_none is True, then don't cache
                        if not (return_value is None and self.skip_none):
                            self._data.set_value(data_key, return_value, fetch_time, self._seconds_to_refresh)
            return return_value or self._data.get_value(data_key)

        return get_value


class MemCachedGC(object):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        self.access_threshold = 10
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.run, 'interval', minutes=self.access_threshold)
        scheduler.start()
        logging.getLogger('apscheduler.scheduler').setLevel(logging.WARNING)

    def run(self):
        gc_start_time = time.time()
        data = MemCachedData()
        data_keys = data.get_keys()
        total_keys = len(data_keys)
        removed_keys = 0
        for key in data_keys:
            with MemCached.write_locks.setdefault(key, Lock()):
                key_last_accessed = data.get_last_accessed(key)
                if datetime.datetime.utcnow() - key_last_accessed >= datetime.timedelta(minutes=self.access_threshold):
                    data.remove_key(key)
                    removed_keys += 1
        if removed_keys > 0:
            Treysor().info(log_tag='memcached_gc', time_taken=time.time() - gc_start_time, total_keys=total_keys,
                           removed_keys=removed_keys,
                           remaining_keys=total_keys - removed_keys)
