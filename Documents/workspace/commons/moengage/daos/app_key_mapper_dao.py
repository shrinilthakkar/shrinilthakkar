from moengage.daos.base_dao import BaseDAO
from moengage.models import AppKeyMapper


class AppKeyMapperDAO(BaseDAO):
    def __init__(self, model_class=AppKeyMapper):
        super(AppKeyMapperDAO, self).__init__('moengage', 'AppKeyMapping', model_class=model_class)

    def findByAppKey(self, app_key, **kwargs):
        return self.findOne({'from_key': app_key}, **kwargs)
