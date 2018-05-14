from moengage.daos.base_dao import BaseDAO
from moengage.models.device import Device


class DeviceDAO(BaseDAO):
    def __init__(self, db_name, model_class=Device):
        super(DeviceDAO, self).__init__(db_name, 'Devices', model_class=model_class)

    def findByUniqueId(self, unique_id, **kwargs):
        return self.findOne({'unique_id': unique_id}, **kwargs)
