from moengage.daos import BaseDAO
from moengage.models import Index
from moengage.external.rawlog_tracker.model import RawLogTracker


class RawLogTrackerDao(BaseDAO):
    INDEXES = [
        Index(fields=['app_key'], unique=True)
    ]

    def __init__(self, model_class=RawLogTracker):
        super(RawLogTrackerDao, self).__init__('LogTrackers', 'RawLogTracker', model_class=model_class,
                                               ensure_indexes=True, indexes=RawLogTrackerDao.INDEXES)
