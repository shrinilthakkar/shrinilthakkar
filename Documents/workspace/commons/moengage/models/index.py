import pymongo

from moengage.models.base import SimpleDocument


class InvalidIndexDefinitionException(Exception):
    pass


class Index(SimpleDocument):
    def __init__(self, **kwargs):
        self._fields = None
        self.unique = None
        self.sparse = None
        self.background = True
        self.name = None
        self.expireAfterSeconds = None
        super(Index, self).__init__(**kwargs)
        if self.fields is None:
            raise InvalidIndexDefinitionException("field to index not specified")
        if self.name is None:
            self.name = self._createIndexName()

    @property
    def fields(self):
        return self._fields

    @fields.setter
    def fields(self, fields):
        self._fields = []
        if isinstance(fields, list) and len(fields) > 1:
            for field in fields:
                field_tuple = self._getFieldTuple(field)
                self._fields.append(field_tuple)
        else:
            field = fields[0] if isinstance(fields, list) else fields
            field_tuple = self._getFieldTuple(field)
            self._fields.append(field_tuple)

    def _getFieldTuple(self, field):
        return field if isinstance(field, tuple) else (field, pymongo.ASCENDING)

    def _nameForField(self, field):
        field_name = field[0] if isinstance(field, tuple) else field
        return field_name + "_"

    def _createIndexName(self):
        index_name = "_"
        if isinstance(self.fields, list) and len(self.fields) > 1:
            index_name += "compound_"
            for field in self.fields:
                index_name += self._nameForField(field)
        else:
            field = self.fields[0] if isinstance(self.fields, list) else self.fields
            index_name += self._nameForField(field)

        if self.unique is not None:
            index_name += 'unique_'
        if self.sparse is not None:
            index_name += 'sparse_'
        if self.expireAfterSeconds is not None:
            index_name += 'ttl_'
        return index_name
