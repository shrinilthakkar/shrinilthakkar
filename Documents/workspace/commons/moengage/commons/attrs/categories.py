from moengage.commons import SingletonMetaClass
from moengage.commons.attrs.config.provider import ActionInfoProvider


class Categories(object):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        info_provider = ActionInfoProvider()
        self.categories = dict({value: position for position, value in enumerate(info_provider.getConfigForCategory())})

    def getCategoryOrder(self, category):
        return self.categories.get(category, len(self.categories))
