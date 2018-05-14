from moengage.daos.base_dao import BaseDAO
from moengage.daos.app_key_mapper_dao import AppKeyMapperDAO
from moengage.models import App


class AppDAO(BaseDAO):
    def __init__(self, model_class=App):
        super(AppDAO, self).__init__('moengage', 'Apps', model_class=model_class)

    def findByAppKey(self, app_key, **kwargs):
        app_obj = self.findOne({'app_key': app_key}, **kwargs)
        if app_obj is None:
            app_key_mapper = AppKeyMapperDAO().findByAppKey(app_key)
            if app_key_mapper:
                mapped_app_key = app_key_mapper.to_key
                app_obj = self.findOne({'app_key': mapped_app_key}, **kwargs)
        return app_obj

    def findByDBName(self, db_name, **kwargs):
        return self.findOne({'db_name': db_name}, **kwargs)
