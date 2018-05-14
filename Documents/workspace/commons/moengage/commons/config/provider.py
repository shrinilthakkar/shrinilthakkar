import threading
from moengage.commons.singleton import SingletonMetaClass


class ConfigMetaClass(type):
    def __init__(cls, name, bases, dictionary):
        super(ConfigMetaClass, cls).__init__(cls, bases, dictionary)
        cls._config_files = {}
        cls._lock = threading.Lock()

    def __call__(cls, config_file_path, _module):
        file_path = _module + config_file_path
        config = cls._config_files.get(file_path)
        if config is None:
            with cls._lock:
                if not cls._config_files.get(file_path):
                    config = super(ConfigMetaClass, cls).__call__(config_file_path, _module)
                    cls._config_files[file_path] = config
                else:
                    config = cls._config_files.get(file_path)
        return config


class ConfigFileProvider(object):
    __metaclass__ = ConfigMetaClass

    def __init__(self, config_file_path, _module):
        from moengage.commons.utils import CommonUtils
        self.config = CommonUtils.readResourceJson(_module, config_file_path)


class ConfigKeyProvider(object):
    def __init__(self, config_provider, key):
        self.config = config_provider.config.get(key, {})


class CommonConfigProvider(object):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        from moengage.commons.utils import CommonUtils
        self.config = CommonUtils.readResourceJson(__name__, 'config.json')
        self.config['moe_env'] = CommonUtils.getEnv()

    def __str__(self):
        return "MoEngage - Common Config Provider"

    def getEnv(self):
        return self.config.get('moe_env')

    def getLoggingConfig(self):
        return self.config.get('logging', {})

    def getInfraTypeConfig(self):
        return self.config.get('infra_type', {})

    def getMongoConfig(self):
        return self.config.get('connections', {}).get('mongo', {})

    def getCacheConfig(self):
        return self.config.get('connections', {}).get('cache', {})

    def getStatsConfig(self):
        return self.config.get('connections', {}).get('statsd', {})

    def getInfluxConfig(self):
        return self.config.get('connections', {}).get('influxdb', {})

    def getS3Config(self):
        return self.config.get('connections', {}).get('s3', {})

    def getAppCategoryConfig(self):
        return self.config.get('app_category', {})

    def getKafkaConfig(self):
        return self.config.get('connections', {}).get('kafka', {}).get('default', {})

    def getZookeeperConfig(self):
        return self.config.get('connections', {}).get('zookeeper', {}).get('default', {})

    def getWatchdogMetricConfig(self):
        return self.config.get('watchdog_metrics', {})

    def getMetricPrefixDBNameMap(self):
        return self.getWatchdogMetricConfig().get('metric_prefix_db_name_map', {})

    def getWatchdogDatabaseToPortMap(self):
        return self.getWatchdogMetricConfig().get('db_name_udp_port_map', {})
