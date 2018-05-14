from moengage.models import SchemaDocument
import datetime
from datetime import timedelta


class RawLogTracker(SchemaDocument):
    def __init__(self, **kwargs):
        self.app_key = None
        self.last_updated_date = None
        self.last_updated_hour = 0
        super(RawLogTracker, self).__init__(**kwargs)
