from threading import Lock

from bson.objectid import ObjectId
from enum import Enum

from moengage.commons.utils import CommonUtils


class InvalidFieldException(Exception):
    pass


class InvalidFieldValueException(Exception):
    pass


class DocumentMetaClass(type):
    def __init__(cls, name, bases, dictionary):
        super(DocumentMetaClass, cls).__init__(cls, bases, dictionary)
        cls._schema = None
        cls._schema_keys = set()
        cls._schema_lock = Lock()


class SimpleDocument(object):
    __metaclass__ = DocumentMetaClass

    def __init__(self, **kwargs):
        super(SimpleDocument, self).__init__()
        # Object contains only default attributes set till now - This becomes the _schema
        self.setSchema(self)
        self.update(**kwargs)

    def update(self, **kwargs):
        raise_on_invalid_field = kwargs.pop('raise_on_invalid_field', False)

        # Set each attribute on this object
        for key in kwargs:
            try:
                self.__setattr__(key, kwargs.get(key, self.getSchema().get(key)))
            except InvalidFieldException:
                if raise_on_invalid_field:
                    raise
                # Ignore any fields not present in the model, but available in db (only in case of _schema document)
                # Doesnt handle unicode exceptions
                # Append _ to key to make it an internal field, but not accessible directly
                # to_dict handles such fields and includes them in to_dict return value
                super(SimpleDocument, self).__setattr__('_' + key, kwargs.get(key))
        self.validate_schema_document()

    @classmethod
    def getSchema(cls):
        return cls._schema

    @classmethod
    def getSchemaKeys(cls):
        if cls._schema_lock.locked():
            with cls._schema_lock:
                return cls._schema_keys
        else:
            return cls._schema_keys

    @classmethod
    def setSchema(cls, self):
        if not cls._schema:
            with cls._schema_lock:
                if not cls._schema:
                    cls._schema = self.__to_schema()
                    cls._schema_keys = set(cls._schema.keys())

    @classmethod
    def addKeyToSchema(cls, key):
        if cls._schema and key not in cls._schema:
            with cls._schema_lock:
                if cls._schema and key not in cls._schema:
                    cls._schema[key] = None
                    cls._schema_keys.add(key)

    def get(self, key, default=None):
        try:
            return self.__getattribute__(key)
        except AttributeError:
            return default

    def __eq__(self, other):
        if isinstance(other, SimpleDocument):
            return self.to_dict() == other.to_dict()
        return False

    def __contains__(self, item):
        try:
            self.__getattribute__(item)
            return True
        except AttributeError:
            return False

    def __getitem__(self, item):
        # To enable dictionary like access to the document
        try:
            return self.__getattribute__(item)
        except AttributeError:
            return None

    def __getattr__(self, item):
        # Called only if attribute is not found in object
        return None

    def __setitem__(self, key, value):
        # To enable dictionary like access to the document
        self.__setattr__(key, value)

    def __setattr__(self, key, value):
        self.addKeyToSchema(key)
        try:
            super(SimpleDocument, self).__setattr__(key, value)
        except UnicodeEncodeError:
            super(SimpleDocument, self).__setattr__(key.encode("utf-8"), value)

    def __to_schema(self):
        return dict(self.__dict__)

    def __serialize(self, v):
        serialized = v
        if isinstance(v, SimpleDocument):
            serialized = v.to_dict()
        elif isinstance(v, Enum):
            serialized = str(v)
        elif isinstance(v, dict):
            serialized = {key: self.__serialize(value) for key, value in v.items()}
        elif isinstance(v, list) or isinstance(v, set):
            serialized = list()
            for item in v:
                serialized.append(self.__serialize(item))
        return serialized

    def to_dict(self):
        """ Convert the document into a dictionary which can be saved
        :return: Dictionary with filtered fields to be saved in mongo
        """
        return {k[1:] if k.startswith('_') and k != '_id' else k: self.__serialize(v)
                for k, v in self.__dict__.items() if v is not None}

    def to_json(self):
        """ JSON serialize the to_dict representation of the document
        :return: JSON String
        """
        return CommonUtils.to_json(self.to_dict())

    def copy(self, **kwargs):
        class_dict = self.to_dict()
        class_dict.update(kwargs)
        return self.__class__(**class_dict)

    def __deepcopy__(self, memodict=None):
        return self.copy()

    def __copy__(self):
        return self.copy()

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state

    def validate_schema_document(self, invalid_fields=None):
        if invalid_fields:
            raise InvalidFieldException(
                'Some of the required fields are missing: {0}'.format(", ".join(invalid_fields)))


class SimpleSchemaDocument(SimpleDocument):
    def __checkKeyExists(self, key):
        if not self.getSchema():
            # Allow keys to be set before constructors are called
            # _schema will not be set till the __init__ method executes
            return True
        if key not in self.getSchemaKeys() and '_' + key not in self.getSchemaKeys():
            raise InvalidFieldException("Field: %s missing in Document Schema for %s" %
                                        (key, self.__class__.__name__))
        return True

    def __setitem__(self, key, value):
        if self.__checkKeyExists(key):
            super(SimpleSchemaDocument, self).__setitem__(key, value)

    def __setattr__(self, key, value):
        if self.__checkKeyExists(key):
            super(SimpleSchemaDocument, self).__setattr__(key, value)


class SchemalessDocument(SimpleDocument):
    def __init__(self, **kwargs):
        """Initialize a mongo document
        Defining attributes:
            * for attributes which dont need custom validations, simple add self.<attribute_name> in
        :param kwargs:
        """
        self._id = None

        super(SchemalessDocument, self).__init__(**kwargs)

        # For new objects (not being initialized via kwargs), _id will not be present. So generate one
        if not self._id:
            self._id = ObjectId()

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, object_id):
        self._id = object_id


class SchemaDocument(SimpleSchemaDocument):
    def __init__(self, **kwargs):
        self._id = None

        super(SchemaDocument, self).__init__(**kwargs)

        # For new objects (not being initialized via kwargs), _id will not be present. So generate one
        if not self._id:
            self._id = ObjectId()

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, object_id):
        self._id = object_id
