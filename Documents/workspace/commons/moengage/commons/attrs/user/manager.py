from moengage.commons.attrs.user.model import MOEUserAttribute
from moengage.commons.attrs.user.service import UserAttributeService
from moengage.commons.decorators.cached import MemCached
from moengage.commons.utils.common import CommonUtils


class UserAttributeManager(object):
    def __init__(self, db_name):
        self.db_name = db_name
        self.attr_service = UserAttributeService(self.db_name)

    def getAttributeMap(self, ttl=300):
        @MemCached('user_attrs_' + self.db_name, secs_to_refresh=ttl)
        def getUserAttrMap():
            return self.attr_service.getAttributeMap()
        return getUserAttrMap()

    def getAttributeMapByPlatforms(self, platforms=None, ttl=300):
        platforms = platforms or CommonUtils.getAllPlatforms()
        attr_map_by_platforms = dict()
        for platform in platforms:
            @MemCached(MemCached.createKey('user_attrs', self.db_name, platform), secs_to_refresh=ttl)
            def get_attrs_by_platform():
                attr_map = self.attr_service.getAttributeMapByPlatforms(platform)
                return attr_map
            platform_map = get_attrs_by_platform()
            for attr in platform_map:
                attr_map_by_platforms.setdefault(attr, {})
                attr_map_by_platforms[attr].update(platform_map[attr])
        return attr_map_by_platforms

    def trackUserAttribute(self, attribute_name, attribute_type, attribute_platform):
        attr_map = self.getAttributeMap()
        platform_attr = attr_map.get(CommonUtils.encodeValue(attribute_name), {}).get(attribute_platform)
        if platform_attr:
            if str(attribute_type) in (platform_attr['data_types'] or set()):
                return platform_attr['_id']
            else:
                user_attr = MOEUserAttribute(**platform_attr)
        else:
            user_attr = MOEUserAttribute(name=attribute_name, platform=attribute_platform, db_name=self.db_name)

        user_attr.addAttrDataType(attribute_type)
        saved = self.attr_service.saveAttr(user_attr)

        # If save failed, means it was a duplicate attribute and the map needs to be refreshed
        if not saved:
            attr_map = self.getAttributeMap(ttl=0)
            user_attr = attr_map.get(CommonUtils.encodeValue(attribute_name), {}).get(attribute_platform)
        return user_attr['_id']
