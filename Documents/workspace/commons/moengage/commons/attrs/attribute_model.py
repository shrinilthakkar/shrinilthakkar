import datetime
import re

from moengage.commons.attrs.config.provider import AttrInfoProvider
from moengage.commons.attrs.data_type import DataType
from moengage.models.base import SchemaDocument


class MOEAttribute(SchemaDocument):
    DEFAULT_CATEGORY = 'Tracked Attribute'
    INFO_PROVIDER = AttrInfoProvider()

    def __init__(self, **kwargs):
        self.db_name = None
        self.platform = None
        self.name = None
        self.readable_name = None
        self._data_types = None
        self._categories = None
        self.description = None
        self.hidden = None
        self.created_time = None
        self.updated_time = None
        super(MOEAttribute, self).__init__(**kwargs)
        if self.created_time is None:
            self.created_time = datetime.datetime.utcnow()
        if self.updated_time is None:
            self.updated_time = datetime.datetime.utcnow()
        self.readable_name = self._getReadableName()
        self.description = self._getDescription()
        self.hidden = self._isAttributeHidden()
        self._categories = self._getCategories()

    @property
    def categories(self):
        return self._categories

    @categories.setter
    def categories(self, categories):
        self._categories = categories

    @property
    def data_types(self):
        return self._data_types

    @data_types.setter
    def data_types(self, data_types):
        self._data_types = set(data_types)

    def addAttrDataType(self, data_type):
        attr_data_type = DataType.fromStr(data_type) if isinstance(data_type, basestring) else data_type
        if self._data_types is None:
            self._data_types = set()
        self._data_types.add(attr_data_type)

    def addCategory(self, category):
        if self._categories is None:
            self._categories = set()
        self._categories.add(category)

    def _getReadableName(self):
        return self.getReadableName(self.name, readable_name=self.readable_name)

    def _getDescription(self):
        return self.getDescription(self.name, description=self.description)

    def _getCategories(self):
        return self.getCategories(self.name, categories=self._categories)

    def _isAttributeHidden(self):
        return self.isAttributeHidden(self.name, hidden=self.hidden)

    @classmethod
    def getReadableName(cls, attr_name, readable_name=None):
        readable_name = cls.INFO_PROVIDER.getAttributeReadableName(attr_name) or readable_name or attr_name
        return re.sub("^moe_geo_", "", readable_name, 1)

    @classmethod
    def getDescription(cls, attr_name, description=None):
        return cls.INFO_PROVIDER.getAttributeDescription(attr_name) or description

    @classmethod
    def getCategories(cls, attr_name, categories=None):
        categories = cls.INFO_PROVIDER.getAttributeCategories(attr_name) or categories
        return set(categories) if categories else {cls.DEFAULT_CATEGORY}

    @classmethod
    def isAttributeHidden(cls, attr_name, hidden=False):
        is_attribute_hidden = cls.INFO_PROVIDER.getAttributeHidden(attr_name)
        if is_attribute_hidden is None:
            is_attribute_hidden = hidden
        return is_attribute_hidden

    def to_dict(self):
        attr_dict = super(MOEAttribute, self).to_dict()
        attr_dict.pop('description', None)
        return attr_dict
