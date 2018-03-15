import falcon

from moengage.api.falcon.handlers.exception_handler import GenericExceptionHandler
from moengage.commons import CommonUtils


class HealthCheckRouteExceptionHandler(GenericExceptionHandler):
    @classmethod
    def handle(cls, ex, req, resp, params):
        resp.status = falcon.HTTP_200
        resp.context['response'] = {"app_name": req.context.get("api_app_name")}
        result = resp.context.get('response', "")
        resp.body = CommonUtils.to_json(result)
        resp.content_type = 'application/json'
