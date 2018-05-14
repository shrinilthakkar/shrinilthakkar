from moengage.models.base import SchemaDocument


class TrackerBase(SchemaDocument):
    def __init__(self, **kwargs):
        self.tracker_type = None
        self._checkpoint = None
        super(TrackerBase, self).__init__(**kwargs)

    @property
    def checkpoint(self):
        return self._checkpoint

    @checkpoint.setter
    def checkpoint(self, checkpoint):
        self._checkpoint = checkpoint