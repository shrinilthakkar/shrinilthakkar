import datetime
from pymongo.errors import DuplicateKeyError

from moengage.commons.attrs.attribute_service import AttributeService
from moengage.commons.attrs.user.dao import UserAttributeDAO
from moengage.commons.attrs.user.model import MOEUserAttribute
from moengage.commons.utils.common import CommonUtils


class UserAttributeService(AttributeService):
    def __init__(self, db_name):
        super(UserAttributeService, self).__init__(db_name)
        self.user_attribute_dao = UserAttributeDAO(db_name)

    def getAttributeMap(self, **query):
        query['projection'] = {'description': 0}
        user_attributes = self.user_attribute_dao.find(**query)
        user_attr_map = {}
        # Add default attrs
        for default_attr in MOEUserAttribute.DEFAULT_ATTRIBUTES:
            attr_map = user_attr_map.setdefault(CommonUtils.encodeValue(default_attr.name), {})
            platforms = [default_attr.platform] if default_attr.platform else CommonUtils.getAllPlatforms()
            for platform in platforms:
                user_attr = MOEUserAttribute(db_name=self.db_name, name=default_attr.name,
                                             data_types=default_attr.data_types, platform=platform)
                attr_map.setdefault(platform, user_attr.to_dict())
        # Fetch from db
        for attr in user_attributes:
            attr_map = user_attr_map.setdefault(CommonUtils.encodeValue(attr['name']), {})
            attr_map[attr['platform']] = attr.to_dict()
        return user_attr_map

    def getAttributeMapByPlatforms(self, platform):
        query = dict()
        query['platform'] = platform
        return self.getAttributeMap(query=query)

    def saveAttr(self, user_attr):
        try:
            user_attr.updated_time = datetime.datetime.utcnow()
            return self.user_attribute_dao.save(user_attr)
        except DuplicateKeyError:
            return
