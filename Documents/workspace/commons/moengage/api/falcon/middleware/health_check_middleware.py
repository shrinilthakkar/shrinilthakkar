from moengage.api.utils.exceptions import HealthCheckRouteException


class HealthCheckMiddleware(object):
    def is_health_check_request(self, req):
        if req.path.endswith('_health') or req.path.endswith('_health/'):
            return True
        return False

    def process_request(self, req, resp):
        if self.is_health_check_request(req):
            raise HealthCheckRouteException('Health Check Route Found')