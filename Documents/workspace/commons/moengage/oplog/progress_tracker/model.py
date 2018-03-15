import datetime

from bson.objectid import ObjectId

from moengage.oplog.tracker_base.model import TrackerBase


class ProgressTracker(TrackerBase):
    def __init__(self, **kwargs):
        self._generation_time = None
        self.db_name = None
        super(ProgressTracker, self).__init__(**kwargs)

    @property
    def checkpoint(self):
        return self._checkpoint

    @checkpoint.setter
    def checkpoint(self, checkpoint):
        self._checkpoint = checkpoint
        try:
            self._generation_time = ObjectId(checkpoint).generation_time
        except ValueError:
            self._generation_time = datetime.datetime.utcnow()
