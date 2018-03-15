class CORSHeaders(object):
    def process_response(self, req, resp, resource, req_succeeded):
        resp.append_header('Access-Control-Allow-Origin', '*')
        resp.append_header('Access-Control-Allow-Methods', '*')
        resp.append_header('Access-Control-Allow-Headers', '*')