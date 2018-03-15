from moengage.commons.decorators import MemCached
from moengage.commons.decorators import Retry
from moengage.commons.exceptions import MONGO_NETWORK_ERRORS
from moengage.oplog.monitor.pipeline_config.dao import PipelineConsumerConfigDAO
from moengage.oplog.monitor.pipeline_config.model import PipelineConsumerConfig


class PipelineConsumerConfigService(object):
    def __init__(self, db_name, process_type):
        self.db_name = db_name
        self.process_type = process_type
        self.consumer_config_dao = PipelineConsumerConfigDAO(self.db_name, self.process_type)

    def generate_id(self):
        return MemCached.createKey(self.db_name, self.process_type)

    def get_config_id(self, ttl=1000):
        @MemCached(self.generate_id(), secs_to_refresh=ttl)
        def cache_config_id():
            config = self.consumer_config_dao.get_config()
            return config.id if config else None

        return cache_config_id()

    @Retry(MONGO_NETWORK_ERRORS, after=30, max_retries=5)
    def get_config(self):
        return self.consumer_config_dao.get_config()

    def create_pipeline_config_object(self, collections, logging):
        pipeline_config = PipelineConsumerConfig()
        pipeline_config.db_name = self.db_name
        pipeline_config.process_type = self.process_type
        pipeline_config.collections = collections
        pipeline_config.logging = logging
        pipeline_config.id = self.generate_id()
        return pipeline_config

    @Retry(MONGO_NETWORK_ERRORS, after=30, max_retries=5)
    def create_pipeline_config(self, pipeline_config):
        self.consumer_config_dao.insert(pipeline_config)
        self.get_config_id(0)
        return pipeline_config

    @Retry(MONGO_NETWORK_ERRORS, after=30, max_retries=5)
    def update_or_create_pipeline_config(self, collections, logging, pipeline_config=None):
        if pipeline_config is None:
            pipeline_config = self.get_config()
        if pipeline_config is None:
            pipeline_config = self.create_pipeline_config_object(collections, logging)
            return self.create_pipeline_config(pipeline_config)
        pipeline_config.collections = collections
        pipeline_config.logging = logging

        fields_to_update = dict(collections=pipeline_config.collections, logging=pipeline_config.logging.to_dict())

        config = self.consumer_config_dao.findByIdAndModify(pipeline_config.id,
                                                            set_spec=fields_to_update, new=True)
        self.get_config_id(0)
        return config

    @classmethod
    @Retry(MONGO_NETWORK_ERRORS, after=30, max_retries=5)
    def get_config_by_id(cls, config_id):
        return PipelineConsumerConfigDAO().get_config_by_id(config_id=config_id)
