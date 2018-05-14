from moengage.commons.connections import ConnectionUtils
from moengage.models.base import SchemalessDocument, SchemaDocument


class PipelineConsumerLogging(SchemaDocument):
    def __init__(self, **kwargs):
        self.log_level = 2
        self.filename = "/var/log/pipeline_logs"
        self.when = "D"
        self.interval = 1
        self.backupCount = 7
        super(PipelineConsumerLogging, self).__init__(**kwargs)


class PipelineConsumerConfig(SchemalessDocument):
    def __init__(self, **kwargs):
        self._db_name = None
        self.process_type = None
        self.collections = None
        self.infra_type = None
        self._logging = None
        super(PipelineConsumerConfig, self).__init__(**kwargs)

    @property
    def db_name(self):
        return self._db_name

    @db_name.setter
    def db_name(self, db_name):
        self._db_name = db_name
        if self.infra_type is None:
            self.infra_type = ConnectionUtils.getInfraType(self.db_name)

    @property
    def logging(self):
        return self._logging

    @logging.setter
    def logging(self, log_config):
        if isinstance(log_config, dict):
            if self._logging:
                old_log_config = self._logging.to_dict() if isinstance(self._logging,
                                                                       SchemalessDocument) else self._logging
                old_log_config.update(log_config)
                new_log_config = old_log_config
            else:
                new_log_config = log_config
            self._logging = PipelineConsumerLogging(**new_log_config)
        else:
            self._logging = log_config
