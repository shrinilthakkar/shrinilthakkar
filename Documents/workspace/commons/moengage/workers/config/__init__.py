from enum import Enum
from moengage.commons import CommonUtils
from moengage.commons.connections import ConnectionUtils


class WorkerTags(Enum):
    NAME = 1
    QUEUE_NAMES = 2
    RUN_MODE = 3
    NUM_NODES = 4
    NODE_CONCURRENCY = 5
    CELERY_APP = 6

    @staticmethod
    def fromStr(value):
        return {
            'WorkerName': WorkerTags.NAME,
            'WorkerQueueNames': WorkerTags.QUEUE_NAMES,
            'WorkerRunMode': WorkerTags.RUN_MODE,
            'WorkerNumNodes': WorkerTags.NUM_NODES,
            'WorkerNodeConcurrency': WorkerTags.NODE_CONCURRENCY,
            'WorkerCeleryApp': WorkerTags.CELERY_APP
        }.get(value)

    def __str__(self):
        return {
            WorkerTags.NAME: 'WorkerName',
            WorkerTags.QUEUE_NAMES: 'WorkerQueueNames',
            WorkerTags.RUN_MODE: 'WorkerRunMode',
            WorkerTags.NUM_NODES: 'WorkerNumNodes',
            WorkerTags.NODE_CONCURRENCY: 'WorkerNodeConcurrency',
            WorkerTags.CELERY_APP: 'WorkerCeleryApp'
        }.get(self)


class WorkerBrokerType(Enum):
    REDIS = 1
    SQS = 2
    PRP = 3

    def __str__(self):
        return {
            WorkerBrokerType.REDIS: "redis",
            WorkerBrokerType.SQS: "sqs",
            WorkerBrokerType.PRP: "prp"
        }.get(self)

    @staticmethod
    def fromStr(value):
        return {
            "redis": WorkerBrokerType.REDIS,
            "sqs": WorkerBrokerType.SQS,
            "prp": WorkerBrokerType.PRP
        }.get(value)

    @staticmethod
    def brokerTypeForEnv(env):
        return {
            'prod': WorkerBrokerType.SQS,
            'pp': WorkerBrokerType.PRP,
            'dev': WorkerBrokerType.REDIS
        }.get(env, WorkerBrokerType.SQS)


class WorkerConfig(Enum):
    NEW_RELIC = 1
    SUPERVISOR = 2
    LOGSTASH = 3
    TREYSOR = 4
    CELERYCONFIG = 5
    INITD = 6
    LOG_DIR = 7
    BEAT_INITD = 8
    BEAT_CELERYCONFIG = 9

    @staticmethod
    def fromStr(value):
        return {
            'newrelic': WorkerConfig.NEW_RELIC,
            'supervisor': WorkerConfig.SUPERVISOR,
            'logstash': WorkerConfig.LOGSTASH,
            'treysor': WorkerConfig.TREYSOR,
            'celeryconfig': WorkerConfig.CELERYCONFIG,
            'initd': WorkerConfig.INITD,
            'log_dir': WorkerConfig.LOG_DIR,
            'beat_initd': WorkerConfig.BEAT_INITD,
            'beat_celeryconfig': WorkerConfig.BEAT_CELERYCONFIG
        }.get(value)

    def __str__(self):
        return {
            WorkerConfig.NEW_RELIC: 'newrelic',
            WorkerConfig.SUPERVISOR: 'supervisor',
            WorkerConfig.LOGSTASH: 'logstash',
            WorkerConfig.TREYSOR: 'treysor',
            WorkerConfig.CELERYCONFIG: 'celeryconfig',
            WorkerConfig.INITD: 'initd',
            WorkerConfig.LOG_DIR: 'log_dir',
            WorkerConfig.BEAT_INITD: 'beat_initd',
            WorkerConfig.BEAT_CELERYCONFIG: 'beat_celeryconfig'
        }.get(self)

    def templateFilePath(self):
        return {
            WorkerConfig.NEW_RELIC: '/newrelic.ini',
            WorkerConfig.SUPERVISOR: '/supervisor.conf',
            WorkerConfig.LOGSTASH: '/logstash.conf',
            WorkerConfig.TREYSOR: '/treysor.conf',
            WorkerConfig.CELERYCONFIG: '/worker_celeryconfig',
            WorkerConfig.INITD: '/worker_initd',
            WorkerConfig.LOG_DIR: '/logs',
            WorkerConfig.BEAT_INITD: '/beat_initd',
            WorkerConfig.BEAT_CELERYCONFIG: '/beat_celeryconfig'
        }.get(self, '')

    def readTemplate(self):
        return CommonUtils.readResourceString(__name__, self.templateFilePath())

    def configSavePath(self, base_path, worker_name):
        worker_dir = base_path + worker_name
        return {
            WorkerConfig.INITD: worker_dir + '/' + worker_name,
            WorkerConfig.BEAT_INITD: worker_dir + '/' + worker_name
        }.get(self, worker_dir + self.templateFilePath())

    @staticmethod
    def urlForBrokerType(broker_type=WorkerBrokerType.SQS):
        if broker_type == WorkerBrokerType.SQS:
            conn = ConnectionUtils.getSQSConnection()
            return "sqs://{aws_access_key}:{aws_secret_key}@".format(aws_access_key=conn.aws_access_key_id,
                                                                     aws_secret_key=conn.aws_secret_access_key)
        elif broker_type == WorkerBrokerType.REDIS:
            return "redis://localhost:6379/0"
        elif broker_type == WorkerBrokerType.PRP:
            return "redis://prpbroker.moengage.com:6379/0"
