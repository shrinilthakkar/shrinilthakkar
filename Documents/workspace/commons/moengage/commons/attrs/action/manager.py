import datetime

from bson.objectid import ObjectId

from moengage.commons.attrs.config.provider import ActionInfoProvider

from moengage.commons.attrs import DataSource
from moengage.commons.attrs.action.model import ActionStatus, MOEAction, MOEActionAttribute
from moengage.commons.attrs.action.service import ActionService, ActionAttributeService
from moengage.commons.decorators.cached import MemCached
from moengage.commons.utils.common import CommonUtils


class ActionAttributeManager(object):
    def __init__(self, db_name, action_id):
        self.db_name = db_name
        self.action_id = ObjectId(action_id)
        self.action_attribute_service = ActionAttributeService(self.db_name)

    def getActionAttributeMap(self, ttl=300):
        @MemCached(MemCached.createKey('action_attr_map', self.db_name, str(self.action_id)), secs_to_refresh=ttl)
        def get_attr_map():
            return self.action_attribute_service.getAttributeMapForActionId(self.action_id).get(self.action_id, {})
        return get_attr_map()

    def trackActionAttribute(self, action_attr_map, attribute_name, attribute_type, platform):
        """
        Tracks an individual attribute of any action
        :param action_attr_map:
        :param attribute_name:
        :param attribute_type:
        :param platform:
        :return: Returns a bool indicating if an existing attribute was found
        """
        action_attr = action_attr_map.get(attribute_name)
        if action_attr:
            if str(attribute_type) in (action_attr['data_types'] or set()):
                return False
            else:
                action_attr = MOEActionAttribute(**action_attr)
        else:
            action_attr = MOEActionAttribute(db_name=self.db_name, platform=platform,
                                             name=attribute_name, action_id=self.action_id)
        action_attr.addAttrDataType(attribute_type)
        saved = self.action_attribute_service.saveAttr(action_attr)
        return bool(saved)

    def trackActionAttributes(self, action_attributes, platform):
        action_attr_map = self.getActionAttributeMap()
        attributes_updated = False
        for action_attr in action_attributes:
            attributes_updated = self.trackActionAttribute(action_attr_map, action_attr['name'],
                                                           action_attr['type'], platform)
        if attributes_updated:
            self.getActionAttributeMap(ttl=0)
        return attributes_updated

    def trackDefaultAttributes(self, platform):
        default_attrs = []
        for attr in MOEActionAttribute.DEFAULT_ATTRIBUTES:
            for data_type in attr.data_types:
                default_attrs.append({'name': attr.name, 'type': data_type})
        return self.trackActionAttributes(default_attrs, platform)


class ActionManager(object):
    def __init__(self, db_name):
        self.db_name = db_name
        self.action_service = ActionService(self.db_name)

    def getActionMapByStatus(self, status=ActionStatus.WHITELISTED, ttl=300):
        @MemCached(MemCached.createKey('action', str(status), self.db_name), secs_to_refresh=ttl)
        def get_actions_by_status():
            action_map = self.getActionMap()
            status_action_map = {}
            for action_name, platform_actions in action_map.items():
                for platform, action in platform_actions.items():
                    if action and action['action_status'] == str(status):
                        action_dict = status_action_map.setdefault(action_name, {})
                        action_dict.setdefault(platform, action)
            return status_action_map
        return get_actions_by_status()

    def getActionMap(self, ttl=300):
        @MemCached('action_map_' + self.db_name, secs_to_refresh=ttl)
        def get_action_map():
            return self.action_service.getActionMap()
        return get_action_map()

    def getActionMapFiltered(self, **filters):
        return self.action_service.getActionMap(**filters)

    def getActionMapByPlatforms(self, platforms=None, ttl=300):
        platforms = platforms or CommonUtils.getAllPlatforms()
        action_map_by_platforms = dict()
        for platform in platforms:
            @MemCached(MemCached.createKey('action', self.db_name, platform), secs_to_refresh=ttl)
            def get_actions_by_platform():
                platform_wise_action_map = self.action_service.getActionMapByPlatforms(platform)
                return platform_wise_action_map
            platform_map = get_actions_by_platform()
            for action in platform_map:
                action_map_by_platforms.setdefault(action, {})
                action_map_by_platforms[action].update(platform_map[action])
        return action_map_by_platforms

    def getActionInfo(self, action_name):
        return self.action_service.getActionInfo(action_name)

    def updateActionLastReceived(self, action):
        @MemCached(MemCached.createKey('action_received', str(action['_id'])), secs_to_refresh=1000)
        def action_last_received():
            updated_action = self.action_service.updateActionLastReceived(action)
            return updated_action['last_received_time']
        return action_last_received()

    def canTrackNewAction(self, action_map=None):
        if not action_map:
            action_map = self.getActionMapByStatus(status=ActionStatus.WHITELISTED)
        return len(action_map.keys()) < ActionInfoProvider.getActionCountThreshold(self.db_name)

    def trackAction(self, action_name, platform, action_data_source=DataSource.SDK):
        action_map = self.getActionMap()
        enc_action_name = CommonUtils.encodeValue(action_name)
        action = action_map.get(enc_action_name, {}).get(platform)
        if not action:
            action = MOEAction(db_name=self.db_name, name=action_name, platform=platform,
                               action_data_source=action_data_source, action_status=ActionStatus.WHITELISTED)
            if action.categories == {MOEAction.DEFAULT_CATEGORY} and not self.canTrackNewAction():
                action.action_status = ActionStatus.BLACKLISTED
                action.blacklisting_time = datetime.datetime.utcnow()
            saved = self.action_service.saveAction(action)
            if not saved:
                action_map = self.getActionMap(ttl=0)
                action = action_map.get(CommonUtils.encodeValue(action_name), {}).get(platform)
            else:
                ActionAttributeManager(self.db_name, action['_id']).trackDefaultAttributes(platform)
        else:
            self.updateActionLastReceived(action)
        return action['_id']

    def updateActionStatus(self, action, platforms=None, status=ActionStatus.BLACKLISTED):
        if not platforms:
            platforms = CommonUtils.getAllPlatforms()
        action_info = self.getActionInfo(action)
        for platform, info in action_info.items():
            if platform in platforms:
                set_spec = {
                    "action_status": str(status),
                    "updated_time": datetime.datetime.utcnow(),
                }
                if status == ActionStatus.BLACKLISTED:
                    set_spec["blacklisting_time"] =  datetime.datetime.utcnow()
                self.action_service.action_dao.findByIdAndModify(info['_id'], set_spec=set_spec)