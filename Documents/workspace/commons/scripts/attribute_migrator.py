from moengage.commons.attrs.data_type import DataType
from moengage.commons.attrs.user.manager import UserAttributeManager
from moengage.commons.config.provider import CommonConfigProvider
from moengage.daos.user_dao import UserDAO


class UserAttributeMigrator(object):
    def __init__(self, db_name):
        self.db_name = db_name

    def migrate(self, **source_query):
        attr_manager = UserAttributeManager(self.db_name)
        source_user_dao = UserDAO(self.db_name)
        source_users = source_user_dao.find(**source_query)
        all_platforms = set(CommonConfigProvider().getAllPlatforms())
        for user in source_users:
            user = user.to_dict()
            print user
            platforms = map(lambda os: os.get('os_key') if os else None, user.get('os'))
            if platforms:
                for attr, value in user.items():
                    for platform in platforms:
                        if platform in all_platforms:
                            attr_manager.trackUserAttribute(attr, DataType.forValue(value), platform)
        print "Finished migrating users"
