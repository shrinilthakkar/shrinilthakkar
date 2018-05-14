from moengage.oplog.tracker_base.model import TrackerBase


class PipelineStatusTracker(TrackerBase):
    def __init__(self, **kwargs):
        self.db_name = None
        self.pid = None
        self.status = None
        self.machine_ip = None
        super(PipelineStatusTracker, self).__init__(**kwargs)

    def validate_schema_document(self, invalid_fields=None):
        if not invalid_fields:
            invalid_fields = []
        for field in ['pid', 'machine_ip', 'status', 'db_name']:
            if self.get(field) is None:
                invalid_fields.append(field)
        super(PipelineStatusTracker, self).validate_schema_document(invalid_fields=invalid_fields)
