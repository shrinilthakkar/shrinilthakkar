from moengage.models.index import Index
from moengage.oplog.progress_tracker.model import ProgressTracker
from moengage.oplog.tracker_base.dao import TrackerBaseDAO


class ProgressTrackerDAO(TrackerBaseDAO):
    COLLECTION_NAME = 'PipelineProgressTracker'
    DB_NAME = 'PipelineStatus'
    INDEXES = [
        Index(fields=['tracker_type', 'db_name'], unique=True)
    ]

    def __init__(self):
        super(ProgressTrackerDAO, self).__init__(ProgressTrackerDAO.DB_NAME, ProgressTrackerDAO.COLLECTION_NAME,
                                                 model_class=ProgressTracker, indexes=self.INDEXES)

    def get_tracker(self, tracker_type, **kwargs):
        return super(ProgressTrackerDAO, self).get_tracker(tracker_type, query=kwargs.pop('query'), **kwargs)
