from moengage.daos.base_dao import BaseDAO


class TrackerBaseDAO(BaseDAO):
    def get_tracker(self, tracker_type, **kwargs):
        query = kwargs.pop('query', {})
        query['tracker_type'] = str(tracker_type)
        return self.findOne(query=query, **kwargs)
