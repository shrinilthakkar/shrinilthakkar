import datetime

from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError

from moengage.commons.attrs.action.dao import ActionAttributeDAO, ActionDAO
from moengage.commons.attrs.action.model import ActionStatus
from moengage.commons.attrs.attribute_service import AttributeService
from moengage.commons.utils.common import CommonUtils


class ActionAttributeService(AttributeService):
    def __init__(self, db_name):
        super(ActionAttributeService, self).__init__(db_name)
        self.action_attribute_dao = ActionAttributeDAO(db_name)

    def getAttributeMap(self, **filters):
        filters['projection'] = {'description': 0}
        action_attributes = self.action_attribute_dao.find(**filters)
        action_attribute_map = {}
        for action_attribute in action_attributes:
            attribute_map = action_attribute_map.setdefault(action_attribute['action_id'], {})
            attribute_map[CommonUtils.encodeValue(action_attribute['name'])] = action_attribute.to_dict()
        return action_attribute_map

    def getAttributeMapForActionId(self, action_id):
        return self.getAttributeMap(query=dict(action_id=ObjectId(action_id)))

    def saveAttr(self, attr):
        try:
            return self.action_attribute_dao.save(attr)
        except DuplicateKeyError:
            return


class ActionService(object):
    def __init__(self, db_name):
        self.db_name = db_name
        self.action_dao = ActionDAO(db_name)

    def getUniqueActions(self):
        return self.action_dao.distinct('action_name')

    def getActionMap(self, **query):
        query['projection'] = {'description': 0}
        actions = self.action_dao.find(**query)
        action_map = {}
        for action in actions:
            action_name = CommonUtils.encodeValue(action['name'])
            action_map.setdefault(action_name, {})
            action_map[action_name].setdefault(action['platform'], action.to_dict())
        return action_map

    def getActionMapByPlatforms(self, platform):
        query = dict()
        query['platform'] = platform
        query['action_status'] = str(ActionStatus.WHITELISTED)
        return self.getActionMap(query=query)

    def getActionInfo(self, action_name):
        action_map = self.getActionMap()
        return action_map.get(action_name, {})

    def getWhitelistedActions(self):
        return self.getActionMap(query=dict(action_status=str(ActionStatus.WHITELISTED)))

    def getBlacklistedActions(self):
        return self.getActionMap(query=dict(action_status=str(ActionStatus.BLACKLISTED)))

    def getActionsByActionType(self, action_type):
        return self.getActionMap(query=dict(action_type=str(action_type)))

    def updateActionLastReceived(self, action):
        return self.action_dao.findByIdAndModify(ObjectId(action['_id']),
                                                 set_spec={'last_received_time': datetime.datetime.utcnow(),
                                                           'updated_time': datetime.datetime.utcnow()},
                                                 new=True)

    def saveAction(self, action):
        try:
            return self.action_dao.save(action)
        except DuplicateKeyError:
            return
