from moengage.daos.base_dao import SingleClientCollectionBaseDAO
from moengage.models.index import Index


class AttributeDAO(SingleClientCollectionBaseDAO):
    INDEXES = [
        Index(fields=['db_name', 'platform'])
    ]

    def __init__(self, db_name, collection_name, model_class=dict, indexes=None):
        if not indexes:
            indexes = []
        indexes.extend(AttributeDAO.INDEXES)
        super(AttributeDAO, self).__init__(db_name, 'MOEAttributes', collection_name,
                                           model_class=model_class, indexes=indexes)