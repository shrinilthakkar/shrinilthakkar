from moengage.commons.attrs.attribute_dao import AttributeDAO
from moengage.commons.attrs.user.model import MOEUserAttribute
from moengage.models.index import Index


class UserAttributeDAO(AttributeDAO):
    INDEXES = [
        Index(fields=['db_name', 'name', 'platform'], unique=True)
    ]

    def __init__(self, db_name, model_class=MOEUserAttribute):
        super(UserAttributeDAO, self).__init__(db_name, collection_name='MOEUserAttributes',
                                               model_class=model_class, indexes=UserAttributeDAO.INDEXES)
