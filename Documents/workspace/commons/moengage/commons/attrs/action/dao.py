from moengage.commons.attrs.action.model import MOEActionAttribute, MOEAction
from moengage.commons.attrs.attribute_dao import AttributeDAO
from moengage.models.index import Index


class ActionDAO(AttributeDAO):
    INDEXES = [
        Index(fields=['db_name', 'name', 'platform'], unique=True),
        Index(fields=['action_status'])
    ]

    def __init__(self, db_name, model_class=MOEAction):
        super(ActionDAO, self).__init__(db_name, collection_name='MOEActions', model_class=model_class,
                                        indexes=ActionDAO.INDEXES)


class ActionAttributeDAO(AttributeDAO):
    INDEXES = [
        Index(fields=['db_name', 'action_id', 'name'], unique=True)
    ]

    def __init__(self, db_name, model_class=MOEActionAttribute):
        super(ActionAttributeDAO, self).__init__(db_name, collection_name='MOEActionAttributes',
                                                 model_class=model_class, indexes=ActionAttributeDAO.INDEXES)
