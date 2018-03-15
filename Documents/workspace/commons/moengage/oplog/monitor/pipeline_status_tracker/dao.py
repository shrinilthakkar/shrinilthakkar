from moengage.models.index import Index
from moengage.oplog.monitor.pipeline_status_tracker.model import PipelineStatusTracker
from moengage.oplog.tracker_base.dao import TrackerBaseDAO


class PipelineStatusTrackerDAO(TrackerBaseDAO):
    DB_NAME = 'PipelineStatus'
    COLLECTION_NAME = 'PipelineStatusTracker'
    INDEXES = [
        Index(fields=['tracker_type', 'db_name'], unique=True)
    ]

    def __init__(self):
        super(PipelineStatusTrackerDAO, self).__init__(PipelineStatusTrackerDAO.DB_NAME,
                                                       PipelineStatusTrackerDAO.COLLECTION_NAME,
                                                       model_class=PipelineStatusTracker, indexes=self.INDEXES)
