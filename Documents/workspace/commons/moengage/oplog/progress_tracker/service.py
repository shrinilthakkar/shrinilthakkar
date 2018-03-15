import datetime

from bson.objectid import ObjectId

from moengage.oplog.progress_tracker.dao import ProgressTrackerDAO
from moengage.oplog.tracker_base.service import TrackerBaseService


class ProgressTrackerService(TrackerBaseService):
    def __init__(self, db_name, tracker_type):
        self.db_name = db_name
        super(ProgressTrackerService, self).__init__(tracker_type=tracker_type, tracker_dao=ProgressTrackerDAO())

    def __parse_generation_time(self, checkpoint):
        try:
            return ObjectId(checkpoint).generation_time
        except ValueError:
            return datetime.datetime.utcnow()

    def get_tracker(self, **kwargs):
        return super(ProgressTrackerService, self).get_tracker(query=dict(db_name=self.db_name))

    def create_tracker(self, checkpoint, **kwargs):
        checkpoint = checkpoint
        generation_time = kwargs.pop('generation_time', self.__parse_generation_time(checkpoint))
        return super(ProgressTrackerService,
                     self).create_tracker(checkpoint=checkpoint,
                                          generation_time=generation_time,
                                          db_name=self.db_name, **kwargs)

    def update_or_create_tracker(self, tracker=None, **kwargs):
        kwargs['generation_time'] = self.__parse_generation_time(kwargs.get('checkpoint'))
        super(ProgressTrackerService, self).update_or_create_tracker(tracker=tracker, **kwargs)
