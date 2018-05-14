from moengage.daos.base_dao import BaseDAO
from moengage.models.campaign import Campaign


class CampaignDAO(BaseDAO):
    def __init__(self, db_name, model_class=Campaign):
        super(CampaignDAO, self).__init__(db_name, 'Campaigns', model_class=model_class)
