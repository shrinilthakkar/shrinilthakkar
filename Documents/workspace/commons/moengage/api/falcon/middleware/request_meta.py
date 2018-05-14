import time

from moengage.commons import CommonUtils

from moengage.commons.loggers import Treysor


class RequestMeta(object):
    def __init__(self, app_name):
        self.app_name = app_name

    def process_request(self, req, resp):
        request_id = CommonUtils.generateRandomString(8)
        logger = Treysor(self.app_name)
        logger.updateContext(request_id=request_id)
        req.context['req_start_time'] = time.time()
        req.context['request_id'] = request_id
        req.context['logger'] = logger
        req.context['api_app_name'] = self.app_name
