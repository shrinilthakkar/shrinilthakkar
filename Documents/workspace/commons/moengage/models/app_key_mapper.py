from moengage.models.base import SchemaDocument


class AppKeyMapper(SchemaDocument):
    def __init__(self, **kwargs):
        self.from_key = None
        self.to_key = None
        self.blocked_status = False
        super(AppKeyMapper, self).__init__(**kwargs)
