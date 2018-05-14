from redlock import RedLock, RedLockError

from moengage.commons.config import CommonConfigProvider
from moengage.commons.decorators import Retry


class DistributedLock(RedLock):
    """
    Distributed lock:
     Requires Redis setup
     ttl is in millis
    WARNING: The redis here does not have replication and locks won't be acquired in case of redis being down
    """

    def __init__(self, resource, ttl=30000):
        self.lock_acquire_retry_interval = 0.5
        cache_config = CommonConfigProvider().getCacheConfig()
        connection_details = [{
            'host': cache_config['default']['host'],
            'port': cache_config['default']['port'],
            'db': cache_config['default']['db'],
            'socket_timeout': cache_config['default']['timeout']
        }]
        super(DistributedLock, self).__init__(resource=resource, ttl=ttl,
                                              connection_details=connection_details)

    def __enter__(self):
        # TTL is in millis
        max_retries = 3 * int(self.ttl / (1000 * self.lock_acquire_retry_interval))

        @Retry(RedLockError, after=self.lock_acquire_retry_interval, max_retries=max_retries)
        def get_lock():
            super(DistributedLock, self).__enter__()

        return get_lock()
