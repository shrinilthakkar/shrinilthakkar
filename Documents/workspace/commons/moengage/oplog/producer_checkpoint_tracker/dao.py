from moengage.models.index import Index
from moengage.oplog.producer_checkpoint_tracker.model import ProducerCheckpointTracker
from moengage.oplog.tracker_base.dao import TrackerBaseDAO


class ProducerCheckpointTrackerDAO(TrackerBaseDAO):
    DB_NAME = 'PipelineStatus'
    COLLECTION_NAME = 'ProducerCheckpointTracker'
    INDEXES = [
        Index(fields=['tracker_type'], unique=True)
    ]

    def __init__(self):
        super(ProducerCheckpointTrackerDAO, self).__init__(ProducerCheckpointTrackerDAO.DB_NAME,
                                                           ProducerCheckpointTrackerDAO.COLLECTION_NAME,
                                                           model_class=ProducerCheckpointTracker, indexes=self.INDEXES)
