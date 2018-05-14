"""
This module provides CacheLayer service for application developers.
Reads configuration from .yaml file and initializes connections ondemand if lazy is set true
Also monitors different statistics for each end point mentioned in .yaml file [ set,get,error]
"""
import cPickle as pickle
from threading import Lock

import redis

from moengage.commons import SingletonMetaClass
from moengage.commons.config import CommonConfigProvider
from moengage.commons.loggers.treysor import Treysor


class CacheLayerException(Exception):
    pass


def rotate_index(func):
    """
    function decorate for rotating index in replica list
    :param func: CacheClient methods using replica list
    :return:
    """

    def rotate_index_and_call(*args, **kwargs):
        if args[0].cache_replica_connection_list:
            args[0].replica_index = (args[0].replica_index + 1) % len(args[0].cache_replica_connection_list)
        return func(*args, **kwargs)

    return rotate_index_and_call


class CacheManager(object):
    """
    Class that reads the configuration and manages the connections to various endpoints
    """
    __metaclass__ = SingletonMetaClass

    __lock = Lock()
    __stats_lock = Lock()
    __replica_lock = Lock()

    def __init__(self):
        """
        constructor: reads the configuration
                     initializes the connection pool for lazy
        :return:
        """
        self.configuration = CommonConfigProvider().getCacheConfig()
        self.connection_pools = {}
        self.replica_connection_pools = {}
        self.ts_logger = Treysor(domain='moengage-cache')
        self.init_connectionpool()

    # during intialization with lazy =0
    def init_connection(self, service_name):
        """

        :param service_name: destination listed in configuration file
        :return:
        """
        end_point = self.configuration[service_name]
        try:
            self.ts_logger.debug(msg="connecting to host [%s], port [%s]" % (end_point['host'], end_point['port']))
            self.connection_pools[service_name] = redis.ConnectionPool(host=end_point['host'], port=end_point['port'],
                                                                       db=end_point['db'],
                                                                       socket_timeout=end_point['timeout'])
        except redis.RedisError:
            self.ts_logger.exception(msg='exception in creating connection pool for %s ' % service_name)

        if 'replica' in end_point:
            self.replica_connection_pools[service_name] = []
            for entry in end_point['replica']:
                self.ts_logger.debug(msg="connecting to backup host [%s], port [%s]" % (entry['host'], entry['port']))
                try:
                    self.replica_connection_pools[service_name].append(
                        redis.ConnectionPool(host=entry['host'], port=entry['port'], db=entry['db'],
                                             socket_timeout=entry['timeout']))
                except redis.RedisError:
                    self.ts_logger.exception(msg='exception in creating backup connection pool for %s' % entry['name'])

    def init_connectionpool(self):
        """

        :return:
        """
        for service_name in self.configuration.keys():
            end_point = self.configuration[service_name]
            if not end_point['lazy']:
                self.init_connection(service_name)

    def get_connection(self, params=None):
        """
        Thread safe method to get connection from connection pool for service_name mentioned in params
        :param params: should be a dictionary containing 'type' field
        :return: redis connection from connection pool
        """
        if params is None:
            params = {'type': 'default'}
        service_name = params['type']
        with CacheManager.__lock:
            try:
                if service_name in self.connection_pools:
                    return redis.Redis(connection_pool=self.connection_pools[service_name])
                else:
                    self.init_connection(service_name)
                    return redis.Redis(connection_pool=self.connection_pools[service_name])
            except redis.RedisError:
                self.ts_logger.exception(msg='cannot create connection for service name  [%s]' % service_name)
        return None

    def get_replica_list(self, params=None):
        """
        gets list of redis connections for replica set mentioned in configuration
        :param params: dictionary contatining 'type' field mapping to service_name of end_point
        :return: list of redis connections pointing to replica endpoints
        """
        if params is None:
            params = {'type': 'default'}
        service_name = params['type']
        with CacheManager.__replica_lock:
            replica_sets = []
            try:
                if service_name in self.replica_connection_pools:
                    for entry in self.replica_connection_pools[service_name]:
                        replica_sets.append(redis.Redis(connection_pool=entry))
                if len(replica_sets) == 0:
                    return None
                else:
                    return replica_sets
            except redis.RedisError:
                self.ts_logger.exception(msg='error in getting replica connection_list for service [%s]' % service_name)
        return None


class CacheClient(object):
    """
    Wrapper for cache client implementing counters, replica sets and other error handling
    """
    __default_expiry = 3600

    def __init__(self, params=None, client_name='undefined'):
        """
        constructor
        :param params: dictionary contatining 'type' field mapping to service name of destination
        :return:
        :exception: CacheLayerException if connection cannot be intialized
        """
        if params is None:
            params = {'type': 'prod'}
        self.service_name = params['type']
        self.ts_logger = Treysor(domain='moengage-cache')
        self.client_name = client_name
        self.__cm = CacheManager()
        self.__cache_connection = self.__cm.get_connection(params)
        self.cache_replica_connection_list = self.__cm.get_replica_list(params)
        self.replica_index = 0
        if self.__cache_connection is None:
            raise CacheLayerException("Connection not initialised")

    @rotate_index
    def get(self, k, master=False):
        """
        gets the value for the given key and deserialize using pickle
        :param k: key to be fetched
        :param master: boolean
        :return: value for that key
        :exception: CacheLayerException
        """
        try:
            if master is False and self.cache_replica_connection_list:
                item = self.cache_replica_connection_list[self.replica_index].get(k)
            else:
                item = self.__cache_connection.get(k)
            if item is not None:
                return pickle.loads(item)
            else:
                return None
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))
        except TypeError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    @rotate_index
    def getValueAndStatus(self, k, master=False):
        """
        gets value and status for given key
        :param k: key
        :param master:
        :return: deserialize value and boolean status
        :exception: CacheLayerException
        """
        try:
            if master is False and self.cache_replica_connection_list:
                item = self.cache_replica_connection_list[self.replica_index].get(k)
            else:
                item = self.__cache_connection.get(k)
            return item, True
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))
        except TypeError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    def set(self, k, v, expiry=None):
        """
        sets value for given key and value is serialized using pickle
        :param k: key
        :param v: value to be set
        :param expiry: time to live for given key
        :exception: CacheLayerException
        :return:
        """

        try:
            if expiry is None:
                expiry = self.__default_expiry
            if expiry is 0:
                return self.__cache_connection.set(k, pickle.dumps(v))
            else:
                return self.__cache_connection.setex(k, pickle.dumps(v), expiry)
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))
        except TypeError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))
        except Exception, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    def mset(self, kvpairs, expiry=None):
        """
        wrapper around redis mset
        :param kvpairs: dictionary of key value pairs
        :return: True/False depending on status of transaction
        :exception: Throws CacheLayerException in case of error
        """
        try:
            return self.__cache_connection.mset(kvpairs)
        except Exception, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    def mget(self, keys):
        """
        method for multi-get
        :param keys: list containing keys
        :return: True/False depending on status of transaction
        :exception Throws CacheLayerException in case of error
        """
        try:
            return self.__cache_connection.mget(keys)
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))
        except Exception, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    def delete(self, k):
        """
        method to delete entry of key from cache
        :param k: key to be deleted
        :return: True/False depending on status of transaction
        :exception Throws CacheLayerException in case of error
        """
        try:
            return self.__cache_connection.delete(k)
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))
        except Exception, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    def deleteKeysWithPrefix(self, prefix):
        """
        method to delete entries matching prefix from cache
        :param prefix: pattern matching keys
        :return: True/False depending on status of transaction
        :except:Throws CacheLayerException in case of error
        """
        try:
            for key in self.__cache_connection.keys('%s*' % (prefix)):
                self.delete(key)
            return True
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    def getKeysWithPrefix(self, prefix):
        """
        method to get entries matching prefix from cache
        :param prefix: pattern matching keys
        :return: True/False depending on status of transaction
        :except:Throws CacheLayerException in case of error
        """
        try:
            return self.__cache_connection.keys('%s*' % (prefix,))
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))
        except Exception, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    def lpush(self, key, listName):
        """
        method to push entry to a list in cache
        :param key: keyname of entry
        :param listName: list name of list
        :return: True/False
        :exception: Throws CacheLayerException in case of error
        """
        try:
            return self.__cache_connection.lpush(listName, key)
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))
        except Exception, e:
            raise CacheLayerException(str(e))

    def rpop(self, listName):
        """
        method to pop entry from list type in cache
        :param listName: name of the list
        :return: True/False
        :exception: Throws CacheLayerException in case of error
        """
        try:
            return self.__cache_connection.rpop(listName)
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    def rpush(self, key, listName):
        """
        method to push entry to a list cache
        :param key: entry
        :param listName: name of the list in cache
        :return: True/False
        :exception: CacheLayerException in case of error
        """
        try:
            return self.__cache_connection.rpush(listName, key)
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    def exists(self, key):
        """
        method to check if a value exists for given key
        :param key: key
        :return:True/False
        :exception: CacheLayerException in case of connection error
        """
        try:
            return self.__cache_connection.exists(key)
        except Exception, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    def getAsRedis(self, key):
        """
        method to get value of key without serializaiton
        :param key: key name
        :return: value if found or None if Not
        :exception:CacheLayerException
        """
        try:
            return self.__cache_connection.get(key)
        except Exception, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    def setAsRedis(self, k, v, expiry=None):
        """
        sets value for given key in cache without any serialization
        :param k: key
        :param v: value in cache
        :param expiry: time to live for the given entry
        :return: True/False
        :exception:CacheLayerException
        """
        try:
            if expiry is None:
                return self.__cache_connection.set(k, v)
            else:
                return self.__cache_connection.setex(k, v, expiry)
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    def incr(self, key, expiry, val=1):
        """
        increments the value of key in cache
        :param key: key name
        :param val: value to be incremented by
        :param expiry : mandatory expiry
        :return: updated value
        :exception: CacheLayerException
        """
        try:
            pipe = self.__cache_connection.pipeline()
            if val == 1:
                pipe.incr(key)
            else:
                pipe.incrby(key, val)
            pipe.incrby(key, val)
            pipe.expire(key, expiry)
            result = pipe.execute()
            return result[0]
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    def incrByFloat(self, key, expiry, val=1.0):
        """
        increments the value of key in cache
        :param key: key name
        :param mandatory expiry
        :param val: value to be incremented by
        :return: updated value
        :exception: CacheLayerException
        """
        try:
            pipe = self.__cache_connection.pipeline()
            pipe.incrbyfloat(key, val)
            pipe.expire(key, expiry)
            result = pipe.execute()
            return result[0]
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    def decr(self, key, expiry, val=1):
        """
        decreases the value of key in cache
        :param key: key name
        :param expiry:
        :param val: value to decreamented by , default = 1
        :return: updated value
        :exception: CacheLayerException
        """
        try:
            pipe = self.__cache_connection.pipeline()
            pipe.decrby(key, val)
            pipe.expire(key, expiry)
            result = pipe.execute()
            return result[0]
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    def expire(self, key, expire):
        """
        sets expiry time for given key
        :param key: key
        :param expire: time to expire
        :return: True/False
        :exception: CacheLayerException
        """
        try:
            return self.__cache_connection.expire(key, expire)
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    def rename(self, key, newkey):
        """
        rename the key
        :param key: name of the key
        :param newkey: newanme for the key
        :return: True/False
        :exception: CacheLayerException
        """
        try:
            return self.__cache_connection.rename(key, newkey)
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))

    @rotate_index
    def getTTL(self, k, master=False):
        try:
            if master is False and self.cache_replica_connection_list:
                item = self.cache_replica_connection_list[self.replica_index].ttl(k)
            else:
                item = self.__cache_connection.ttl(k)
            return item
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))
        except TypeError, e:
            self.ts_logger.warning(msg=str(e), cause='Cannot de-pickle the object for the key : ' + str(k))
            raise CacheLayerException(str(e))

    def pipeline(self):
        try:
            return self.__cache_connection.pipeline()
        except redis.exceptions.ConnectionError, e:
            self.ts_logger.warning(msg=str(e))
            raise CacheLayerException(str(e))
