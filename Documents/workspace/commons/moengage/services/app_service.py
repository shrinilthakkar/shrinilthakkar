from moengage.commons.decorators import MemCached
from moengage.daos.app_dao import AppDAO


class AppService(object):
    def __init__(self):
        super(AppService, self).__init__()
        self.app_dao = AppDAO()

    def getAppByDBName(self, db_name):
        @MemCached(MemCached.createKey(db_name, 'app_doc'))
        def get_app():
            return self.app_dao.findByDBName(db_name)

        return get_app()

    def getAppByAppKey(self, app_key):
        @MemCached(MemCached.createKey(app_key, 'app_doc'))
        def get_app():
            return self.app_dao.findByAppKey(app_key)

        return get_app()

    def getAppByLowerDBName(self, lower_db_name):
        @MemCached(MemCached.createKey('lower', lower_db_name, 'app_doc'))
        def get_app():
            return self.app_dao.findOne({'lower_db_name': lower_db_name})

        return get_app()
