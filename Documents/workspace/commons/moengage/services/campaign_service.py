from moengage.commons.decorators import MemCached
from moengage.commons.utils.common import CommonUtils
from moengage.daos.campaign_dao import CampaignDAO


class CampaignService(object):
    def __init__(self, db_name):
        super(CampaignService, self).__init__()
        self.campaign_dao = CampaignDAO(db_name)
        self.db_name = db_name

    def getById(self, campaign_id, **kwargs):
        @MemCached(MemCached.createKey(self.db_name, campaign_id, CommonUtils.to_json(kwargs)))
        def get_campaign():
            return self.campaign_dao.findById(campaign_id, **kwargs)

        return get_campaign()
