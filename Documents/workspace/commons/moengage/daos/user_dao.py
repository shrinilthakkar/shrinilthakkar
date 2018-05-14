from moengage.daos.base_dao import BaseDAO
from moengage.models.user import User


class UserDAO(BaseDAO):
    def __init__(self, db_name, model_class=User):
        super(UserDAO, self).__init__(db_name, 'Users', model_class=model_class)
