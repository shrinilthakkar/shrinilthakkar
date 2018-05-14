from abc import ABCMeta, abstractmethod


class AttributeService(object):
    __metaclass__ = ABCMeta

    def __init__(self, db_name):
        self.db_name = db_name

    @abstractmethod
    def getAttributeMap(self):
        pass

    @abstractmethod
    def saveAttr(self, attr):
        pass
