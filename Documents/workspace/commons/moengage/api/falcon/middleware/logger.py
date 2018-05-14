import time

from moengage.commons.loggers import Treysor


class RequestLogger(object):

    def process_request(self, req, resp):
        logger = req.context.get('logger', Treysor(req.context.get('api_app_name')))
        logger.debug(request_body=req.context.get('request_body'), method=req.method, path=req.path)

    def process_response(self, req, resp, resource, req_succeeded):
        if resource:
            logger = req.context.get('logger', Treysor(req.context.get('_app_name')))
            response_status = resp.status
            elapsed_time = time.time() - req.context['req_start_time']
            if str(response_status).startswith('2'):
                logger.debug(response_body=resp.body, status=response_status, time_taken=elapsed_time,
                             req_succeeded=req_succeeded)
            else:
                logger.warning(response_body=resp.body, status=response_status, time_taken=elapsed_time,
                               req_succeeded=req_succeeded)
            logger.clearContext()