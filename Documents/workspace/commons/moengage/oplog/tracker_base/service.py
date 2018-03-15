from moengage.commons.decorators import Retry
from moengage.commons.exceptions import MONGO_NETWORK_ERRORS


class TrackerBaseService(object):
    def __init__(self, tracker_type, tracker_dao):
        self.tracker_dao = tracker_dao
        self.tracker_type = tracker_type

    @Retry(MONGO_NETWORK_ERRORS, max_retries=5, after=30)
    def get_tracker(self, **kwargs):
        return self.tracker_dao.get_tracker(self.tracker_type, **kwargs)

    @Retry(MONGO_NETWORK_ERRORS, max_retries=5, after=30)
    def create_tracker(self, **kwargs):
        kwargs['tracker_type'] = kwargs.pop('tracker_type', self.tracker_type)
        tracker = self.tracker_dao.model_class(**kwargs)
        self.tracker_dao.insert(tracker, ensure_indexes=True)
        return tracker

    @Retry(MONGO_NETWORK_ERRORS, max_retries=5, after=30)
    def update_or_create_tracker(self, tracker=None, **kwargs):
        if not tracker:
            tracker = self.get_tracker(**kwargs)
            if not tracker:
                return self.create_tracker(**kwargs)
        tracker.update(**kwargs)
        self.tracker_dao.save(tracker)
        return tracker
