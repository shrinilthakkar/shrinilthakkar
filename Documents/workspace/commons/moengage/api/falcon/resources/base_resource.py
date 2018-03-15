from abc import ABCMeta

import falcon


class BaseResource(object):
    __metaclass__ = ABCMeta

    @classmethod
    def set_response(cls, resp, response_body, response_status=falcon.HTTP_200):
        resp.status = response_status
        resp.context['response'] = response_body

    @classmethod
    def raise_resource_conflict(cls, message):
        raise falcon.HTTPConflict('Resource not created', message)
