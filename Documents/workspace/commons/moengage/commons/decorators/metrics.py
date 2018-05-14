import datetime
import time
import random

from functools import wraps

from moengage.commons.config.provider import CommonConfigProvider
from moengage.commons.loggers.treysor import Treysor

from moengage.models.base import SimpleDocument

from moengage.commons.utils import CommonUtils
from moengage.commons.connections import ConnectionUtils


class WatchdogMetric(SimpleDocument):
    def __init__(self, **kwargs):
        self.measurement = None
        self.tags = None
        self.fields = None
        self.time = None
        super(WatchdogMetric, self).__init__(**kwargs)
        self.time = datetime.datetime.utcnow().isoformat()
        if self.tags is None:
            self.tags = {}
        if self.fields is None:
            self.fields = {}

    def addTag(self, tag_name, tag_value):
        self.tags[tag_name] = tag_value

    def addField(self, field_name, field_value):
        self.fields[field_name] = field_value


class WatchdogMetricRecorder(object):
    def __init__(self, metric_key, tags=None, values=None, record_execution_count=True, record_execution_time=False,
                 sample_rate=1.0, **kwargs):
        tags = tags or dict()
        values = values or dict()
        self.record_execution_time = record_execution_time
        self.record_execution_count = record_execution_count
        self.sample_rate = sample_rate
        self.kwargs = kwargs
        self.watchdog_metric = WatchdogMetric(measurement=metric_key, tags=tags, fields=values)
        self.__start_time = time.time()

    def __call__(self, f):
        """Thread-safe timing function decorator."""
        @wraps(f)
        def _wrapped(*args, **kwargs):
            self.__start_time = time.time()
            try:
                return_value = f(*args, **kwargs)
            finally:
                self.__record()
            return return_value
        return _wrapped

    def __addDefaults(self):
        execution_time = 1000.0 * (time.time() - self.__start_time)
        if self.record_execution_time:
            self.watchdog_metric.addField('execution_time', execution_time)
        if self.record_execution_count:
            self.watchdog_metric.addField('execution_count', self.kwargs.get('execution_count', 1))
        self.watchdog_metric.addTag('environment', str(CommonUtils.getEnv()))

    def __enter__(self):
        self.__start_time = time.time()
        self.record_execution_time = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__record()

    def __record(self):
        if random.randint(0, 100) <= self.sample_rate * 100:
            self.__addDefaults()
            self.recordMetric(self.watchdog_metric)

    def record(self):
        self.__record()

    @staticmethod
    def recordMetric(watchdog_metric):
        try:
            key_components = watchdog_metric.measurement.split('.')
            database_name = CommonConfigProvider().getMetricPrefixDBNameMap().get(key_components[0], 'unknown_metrics')
            influx_client = ConnectionUtils.getInfluxClient(database_name)
            return influx_client.write_points([watchdog_metric.to_dict()])
        except Exception, e:
            Treysor().debug(action="metric_record_failed", metric=watchdog_metric.to_dict(), exception=str(e))
        return False
