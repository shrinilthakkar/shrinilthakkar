from moengage.commons.config import ConfigKeyProvider, ConfigFileProvider
from moengage.commons.singleton import SingletonMetaClass


class AttrInfoProvider(object):
    def __init__(self, config_provider=None):
        self.config = config_provider.config if config_provider else {}

    def getConfigForAttribute(self, attr_name):
        return self.config.get(attr_name, {})

    def getAttributeReadableName(self, attr_name):
        return self.getConfigForAttribute(attr_name).get('readable_name')

    def getAttributeCategories(self, attr_name):
        return self.getConfigForAttribute(attr_name).get('categories')

    def getAttributeDescription(self, attr_name):
        return self.getConfigForAttribute(attr_name).get('description')

    def getAttributeHidden(self, attr_name):
        return self.getConfigForAttribute(attr_name).get('hidden')


class UserAttrInfoProvider(AttrInfoProvider):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        config_provider = ConfigFileProvider('config.json', __name__)
        super(UserAttrInfoProvider, self).__init__(ConfigKeyProvider(config_provider, 'user_attributes'))


class ActionAttrInfoProvider(AttrInfoProvider):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        config_provider = ConfigFileProvider('config.json', __name__)
        super(ActionAttrInfoProvider, self).__init__(ConfigKeyProvider(config_provider, 'action_attributes'))


class ActionInfoProvider(AttrInfoProvider):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        config_provider = ConfigFileProvider('config.json', __name__)
        super(ActionInfoProvider, self).__init__(ConfigKeyProvider(config_provider, 'actions'))

    @staticmethod
    def getActionCountThreshold(db_name):
        config_provider = ConfigKeyProvider(ConfigFileProvider('config.json', __name__), 'action_count_thresholds')
        return config_provider.config.get(db_name) or config_provider.config.get('default')

    @staticmethod
    def getConfigForCategory():
        config_provider = ConfigKeyProvider(ConfigFileProvider('config.json', __name__), 'attribute_categories')
        return config_provider.config
