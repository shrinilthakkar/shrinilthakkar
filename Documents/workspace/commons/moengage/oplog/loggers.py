from copy import deepcopy

from moengage.commons.loggers.treysor import Treysor, LoggingConfig


class OplogTreysor(Treysor):
    @classmethod
    def setup_treysor_logging_config(cls, logging_config):
        treysor_logging_config = deepcopy(logging_config)
        if 'backupCount' in logging_config:
            treysor_logging_config['backup_count'] = logging_config['backupCount']
        return LoggingConfig(**treysor_logging_config)


class OplogProducerTreysor(OplogTreysor):
    def __init__(self, logging_config=None):
        self.treysor_logging_config = self.setup_treysor_logging_config(logging_config or {})
        super(OplogProducerTreysor, self).__init__('oplog_producer',
                                                   logging_config=self.treysor_logging_config)


class OplogConsumerTreysor(OplogTreysor):
    def __init__(self, logging_config=None):
        self.treysor_logging_config = OplogProducerTreysor.setup_treysor_logging_config(logging_config or {})
        super(OplogConsumerTreysor, self).__init__(domain='oplog_consumer',
                                                   logging_config=self.treysor_logging_config)


class OplogMonitorTreysor(OplogTreysor):
    def __init__(self, logging_config=None):
        self.treysor_logging_config = OplogProducerTreysor.setup_treysor_logging_config(logging_config or {})
        super(OplogMonitorTreysor, self).__init__(domain='oplog_monitor',
                                                  logging_config=self.treysor_logging_config)
