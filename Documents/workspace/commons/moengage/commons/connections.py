import boto3
from boto.s3.connection import S3Connection
from boto.sqs.connection import SQSConnection
from enum import Enum
from influxdb import InfluxDBClient
from pykafka import KafkaClient
from pymongo import MongoClient, ReadPreference

from moengage.commons import SingletonMetaClass
from moengage.commons.config import CommonConfigProvider


class InfraType(Enum):
    DEFAULT = 1
    SNAPDEAL = 2
    SEGMENTATION = 3
    STAGING = 4
    SMARTTRIGGER = 5

    def __str__(self):
        return {
            InfraType.DEFAULT: "default",
            InfraType.SNAPDEAL: "snapdeal",
            InfraType.SEGMENTATION: "segmentation",
            InfraType.STAGING: "staging",
            InfraType.SMARTTRIGGER: "smarttrigger"
        }.get(self, None)

    @staticmethod
    def fromStr(infra_type):
        return {
            "default": InfraType.DEFAULT,
            "snapdeal": InfraType.SNAPDEAL,
            "segmentation": InfraType.SEGMENTATION,
            "staging": InfraType.STAGING,
            "smarttrigger": InfraType.SMARTTRIGGER
        }.get(infra_type, None)


class AWSConnectionUtils(object):
    @classmethod
    def getProfileName(cls, service_type, infra_type=InfraType.DEFAULT, profile_name=None):
        return profile_name or (str(infra_type) + '_' + service_type)

    @classmethod
    def getRegionFromProfile(cls, profile_name):
        session = boto3.Session(profile_name=profile_name)
        return session.region_name or 'us-east-1'

    @classmethod
    def getS3Connection(cls, infra_type=InfraType.DEFAULT, profile_name=None):
        profile = cls.getProfileName('s3', infra_type=infra_type, profile_name=profile_name)
        return S3ConnectionPool().get_connection(profile)

    @classmethod
    def getBoto3S3Connection(cls, infra_type=InfraType.DEFAULT, profile_name=None):
        profile = cls.getProfileName('s3', infra_type=infra_type, profile_name=profile_name)
        return Boto3S3ConnectionPool().get_connection(profile)

    @classmethod
    def getSQSConnection(cls, infra_type=InfraType.DEFAULT, profile_name=None):
        profile = cls.getProfileName('sqs', infra_type=infra_type, profile_name=profile_name)
        return SQSConnectionPool().get_connection(profile)

    @classmethod
    def getEC2Connection(cls, infra_type=InfraType.DEFAULT, profile_name=None):
        profile = cls.getProfileName('ec2', infra_type=infra_type, profile_name=profile_name)
        return EC2ConnectionPool().get_connection(profile)

    @classmethod
    def getELBConnection(cls, infra_type=InfraType.DEFAULT, profile_name=None):
        profile = cls.getProfileName('ec2', infra_type=infra_type, profile_name=profile_name)
        return ELBConnectionPool().get_connection(profile)


class ConnectionUtils(AWSConnectionUtils):
    @staticmethod
    def getMongoConnectionForInfraType(infra_type, read_preference, replica_set=None):
        return MongoConnectionPool().get_mongo_connection(infra_type, read_preference, replica_set=replica_set)

    @staticmethod
    def getInfraType(db_name):
        for infra_type, conf in CommonConfigProvider().getInfraTypeConfig().iteritems():
            if 'db_names' in conf and db_name.lower() in map(lambda x: x.lower(), conf['db_names']):
                return InfraType.fromStr(infra_type)
        # Default Infra Type for all DBs
        return InfraType.DEFAULT

    @classmethod
    def getMongoConnectionForDBName(cls, db_name, read_preference=ReadPreference.PRIMARY_PREFERRED, replica_set=None):
        infra_type = cls.getInfraType(db_name)
        return cls.getMongoConnectionForInfraType(infra_type, read_preference, replica_set=replica_set)

    @staticmethod
    def getKafkaClient():
        return KafkaConnectionPool().get_client()

    @staticmethod
    def getInfluxClient(database_name):
        return InfluxConnectionPool().get_client(database_name)


class InfluxConnectionPool(object):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        self.influx_config = CommonConfigProvider().getInfluxConfig()
        self.influx_clients = {}

    def _create_client(self, database_name):
        db_port = CommonConfigProvider().getWatchdogDatabaseToPortMap().get(database_name, 8092)  # 8092=unknown_metrics
        return self.influx_clients.setdefault(database_name, InfluxDBClient(udp_port=db_port, **self.influx_config))

    def get_client(self, database_name):
        try:
            return self.influx_clients[database_name]
        except KeyError:
            return self._create_client(database_name)


class KafkaConnectionPool(object):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        self.kafka_config = CommonConfigProvider().getKafkaConfig()
        self.kafka_broker_port = self.kafka_config['port']
        self.kafka_connection_url = ",".join(map(lambda x: str(x) + ':' + str(self.kafka_broker_port),
                                                 self.kafka_config['brokers']))
        self.kafka_broker_version = self.kafka_config['broker_version']
        self.kafka_client = KafkaClient(self.kafka_connection_url, broker_version=self.kafka_broker_version)

    def get_client(self):
        return self.kafka_client


class S3ConnectionPool(object):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        self.s3_connections = {}

    def create_connection(self, profile_name):
        region = AWSConnectionUtils.getRegionFromProfile(profile_name)
        host_s3 = 's3.' + region + '.amazonaws.com'
        return S3Connection(profile_name=profile_name, host=host_s3)

    def get_connection(self, profile_name):
        try:
            return self.s3_connections[profile_name]
        except KeyError:
            return self.s3_connections.setdefault(profile_name, self.create_connection(profile_name))

class Boto3S3ConnectionPool(object):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        self.s3_connections = {}

    def create_connection(self, profile_name):
        region = AWSConnectionUtils.getRegionFromProfile(profile_name)
        boto3.setup_default_session(profile_name=profile_name, region_name=region)
        return boto3.resource('s3')

    def get_connection(self, profile_name):
        try:
            return self.s3_connections[profile_name]
        except KeyError:
            return self.s3_connections.setdefault(profile_name, self.create_connection(profile_name))

class EC2ConnectionPool(object):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        self.ec2_connections = {}

    def create_connection(self, profile_name):
        region = AWSConnectionUtils.getRegionFromProfile(profile_name)
        boto3.setup_default_session(profile_name=profile_name, region_name=region)
        return boto3.resource('ec2')

    def get_connection(self, profile_name):
        try:
            return self.ec2_connections[profile_name]
        except KeyError:
            return self.ec2_connections.setdefault(profile_name, self.create_connection(profile_name))


class ELBConnectionPool(object):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        self.elb_connections = {}

    def create_connection(self, profile_name):
        region = AWSConnectionUtils.getRegionFromProfile(profile_name)
        boto3.setup_default_session(profile_name=profile_name, region_name=region)
        return boto3.client('elb')

    def get_connection(self, profile_name):
        try:
            return self.elb_connections[profile_name]
        except KeyError:
            return self.elb_connections.setdefault(profile_name, self.create_connection(profile_name))


class SQSConnectionPool(object):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        self.sqs_connections = {}

    def create_connection(self, profile_name):
        return SQSConnection(profile_name=profile_name)

    def get_connection(self, profile_name):
        try:
            return self.sqs_connections[profile_name]
        except KeyError:
            return self.sqs_connections.setdefault(profile_name, self.create_connection(profile_name))


class MongoConnectionPool(object):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        self.mongo_connections = {}

    def _create_mongo_connection(self, infra_type, read_preference, replica_set):
        mongo_config = CommonConfigProvider().getMongoConfig().get(str(infra_type))
        if mongo_config:
            conn_kwargs = {}
            if replica_set:
                conn_kwargs['replicaSet'] = replica_set
            mongo_conn = MongoClient(mongo_config.get('host'), read_preference=read_preference, **conn_kwargs)
            return self.mongo_connections.setdefault(infra_type, {}).setdefault(read_preference, mongo_conn)

    def get_mongo_connection(self, infra_type, read_preference=ReadPreference.PRIMARY_PREFERRED, replica_set=None):
        if not read_preference:
            read_preference = ReadPreference.PRIMARY_PREFERRED
        try:
            return self.mongo_connections[infra_type][read_preference]
        except KeyError:
            return self._create_mongo_connection(infra_type, read_preference, replica_set)
