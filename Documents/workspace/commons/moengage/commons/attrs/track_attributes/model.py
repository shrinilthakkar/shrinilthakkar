from moengage.models.base import SchemaDocument
from datetime import datetime


class RecentAttribute(SchemaDocument):

    def __init__(self, **kwargs):
        self.db_name = None
        self.attribute_name = None
        self.last_used_time = None
        self.data_type = None
        super(RecentAttribute, self).__init__(**kwargs)
        if self.last_used_time is None:
            self.last_used_time = datetime.utcnow()


class RecentActionAttribute(RecentAttribute):

    def __init__(self, **kwargs):
        self.action_name = None
        super(RecentActionAttribute, self).__init__(**kwargs)


class RecentUserAttribute(RecentAttribute):

    def __init__(self, **kwargs):
        super(RecentUserAttribute, self).__init__(**kwargs)
