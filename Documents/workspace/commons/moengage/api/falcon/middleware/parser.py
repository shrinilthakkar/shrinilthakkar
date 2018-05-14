import json

import falcon
from moengage.commons.utils import CommonUtils
from moengage.models import SimpleDocument


class JSONBodyParser(object):
    def process_request(self, req, resp):
        if req.content_length in (None, 0):
            return

        body = req.stream.read(req.content_length)
        if not body:
            raise falcon.HTTPBadRequest('Empty request body', 'A valid JSON document is required.')
        try:
            req.context['request_body'] = json.loads(body.decode('utf-8'))
        except (ValueError, UnicodeDecodeError):
            raise falcon.HTTPError(falcon.HTTP_753, 'Malformed JSON', 'Could not decode the request body. The JSON was '
                                                                      'incorrect or not encoded as UTF-8.')

    def process_response(self, req, resp, resource, req_succeeded):
        if resource and req_succeeded:
            result = resp.context.get('response', "")
            resp.body = result.to_json() if isinstance(result, SimpleDocument) else CommonUtils.to_json(result)
            resp.content_type = req.content_type or 'application/json'
