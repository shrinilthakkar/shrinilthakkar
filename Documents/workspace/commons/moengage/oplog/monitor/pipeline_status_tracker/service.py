from moengage.models.base import InvalidFieldException
from moengage.oplog.exceptions import PipelineStatusCreationFailedException
from moengage.oplog.monitor.pipeline_status_tracker.dao import PipelineStatusTrackerDAO
from moengage.oplog.tracker_base.service import TrackerBaseService


class PipelineStatusTrackerService(TrackerBaseService):
    def __init__(self, db_name, process_type):
        self.db_name = db_name
        self.process_type = process_type
        super(PipelineStatusTrackerService, self).__init__(tracker_type=process_type,
                                                           tracker_dao=PipelineStatusTrackerDAO())

    def get_tracker(self, **kwargs):
        return super(PipelineStatusTrackerService, self).get_tracker(query=dict(db_name=self.db_name))

    def create_tracker(self, **kwargs):
        try:
            kwargs['db_name'] = self.db_name
            kwargs['status'] = str(kwargs['status'])
            return super(PipelineStatusTrackerService, self).create_tracker(**kwargs)
        except InvalidFieldException:
            raise PipelineStatusCreationFailedException("Cannot create pipeline status without machine ip")
