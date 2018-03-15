from moengage.commons.attrs.track_attributes.model import RecentActionAttribute, RecentUserAttribute
from moengage.daos.base_dao import BaseDAO
from moengage.models.index import Index
from moengage.commons.loggers.context_logger import ContextLogger


class RecentActionAttributeDAO(BaseDAO):
    INDEXES = [
        Index(fields=['db_name', 'action_name', 'attribute_name', 'data_type'], unique=True),
    ]

    def __init__(self, db_name='MOEAttributes', collection_name='RecentActionAttributes',
                 model_class=RecentActionAttribute):
        super(RecentActionAttributeDAO, self).__init__(db_name, collection_name, model_class=model_class,
                                                       indexes=RecentActionAttributeDAO.INDEXES,
                                                       ensure_indexes=True)


class RecentUserAttributeDAO(BaseDAO):
    INDEXES = [
        Index(fields=['db_name', 'attribute_name', 'data_type'], unique=True),
    ]

    def __init__(self, db_name='MOEAttributes', collection_name='RecentUserAttributes',
                 model_class=RecentUserAttribute):
        super(RecentUserAttributeDAO, self).__init__(db_name, collection_name, model_class=model_class,
                                                     indexes=RecentUserAttributeDAO.INDEXES,
                                                     ensure_indexes=True)
