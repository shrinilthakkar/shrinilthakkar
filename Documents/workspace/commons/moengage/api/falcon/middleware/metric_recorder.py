import time

from moengage.commons.decorators import WatchdogMetricRecorder


class MetricRecorder(object):
    def process_response(self, req, resp, resource, req_succeeded):
        if resource:
            response_status = resp.status
            tags = dict(code=response_status)
            if 'app_object' in req.context:
                app_object = req.context['app_object']
                tags['db_name'] = app_object.db_name
            time_taken = time.time() - req.context.get('req_start_time')
            WatchdogMetricRecorder(req.context.get('api_app_name') + '.response.' + resource.__class__.__name__,
                                   tags=tags, values=dict(time_taken=time_taken)).record()
