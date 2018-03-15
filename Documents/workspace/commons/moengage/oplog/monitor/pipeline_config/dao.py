from moengage.commons.connections import InfraType
from moengage.daos.base_dao import BaseDAO
from moengage.oplog.monitor.pipeline_config.model import PipelineConsumerConfig


class PipelineConsumerConfigDAO(BaseDAO):
    def __init__(self, db_name=None, process_type=None):
        super(PipelineConsumerConfigDAO, self).__init__('PipelineStatus', 'PipelineConsumerConfig',
                                                        model_class=PipelineConsumerConfig,
                                                        infra_type=InfraType.SEGMENTATION)
        self.db_name = db_name
        self.process_type = process_type

    def get_config(self, **kwargs):
        return self.findOne(query={'db_name': self.db_name,
                                   'process_type': str(self.process_type)}, **kwargs)

    def get_config_by_id(self, config_id):
        return self.findOne(query={'_id': config_id})
