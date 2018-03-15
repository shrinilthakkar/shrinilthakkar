from moengage.oplog.producer_checkpoint_tracker.dao import ProducerCheckpointTrackerDAO
from moengage.oplog.tracker_base.service import TrackerBaseService


class ProducerCheckpointTrackerService(TrackerBaseService):
    def __init__(self, producer_type):
        super(ProducerCheckpointTrackerService, self).__init__(tracker_type=producer_type,
                                                               tracker_dao=ProducerCheckpointTrackerDAO())
