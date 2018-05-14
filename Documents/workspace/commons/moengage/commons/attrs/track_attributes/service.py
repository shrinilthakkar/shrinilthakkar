from moengage.commons.attrs.track_attributes.dao import RecentActionAttributeDAO, RecentUserAttributeDAO
from datetime import datetime, timedelta


class RecentActionAttributeService(object):
    def __init__(self):
        self.track_attr_dao = RecentActionAttributeDAO()

    def get_last_used_attributes(self, days=10):
        date_n_days_ago = datetime.utcnow() - timedelta(days=days)
        used_attr_map = self.track_attr_dao.find(query={'last_used_time': {'$gt': date_n_days_ago}})
        return used_attr_map

    def save_attr(self, action_attr):
        return self.track_attr_dao.findAndModify(
            query={'db_name': action_attr.db_name, 'action_name': action_attr.action_name,
                   'attribute_name': action_attr.attribute_name, 'data_type': action_attr.data_type},
            set_spec={'last_used_time': action_attr.last_used_time},
            upsert=True)


class RecentUserAttributeService(object):
    def __init__(self):
        self.track_attr_dao = RecentUserAttributeDAO()

    def get_last_used_attributes(self, days=10):
        date_n_days_ago = datetime.utcnow() - timedelta(days=days)
        used_attr_map = self.track_attr_dao.find(query={'last_used_time': {'$gt': date_n_days_ago}})
        return used_attr_map

    def save_attr(self, user_attr):
        return self.track_attr_dao.findAndModify(
            query={'db_name': user_attr.db_name, 'attribute_name': user_attr.attribute_name,
                   'data_type': user_attr.data_type},
            set_spec={'last_used_time': user_attr.last_used_time},
            upsert=True)
