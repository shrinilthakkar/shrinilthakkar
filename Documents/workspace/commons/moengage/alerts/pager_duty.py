import pygerduty


class PagerDutyIncident(object):
    API_TOKEN = "Mv3V-_HqrYbzRhqrQzWo"
    SUBDOMAIN = "moengageindia"
    pager_duty = pygerduty.PagerDuty(subdomain=SUBDOMAIN, api_token=API_TOKEN)

    def __init__(self, service_key):
        self.service_key = service_key

    def _get_incident_params(self, **kwargs):
        incident_params = dict(
            service_key=self.service_key,
            description=kwargs.pop('description'),
            incident_key=kwargs.pop('incident_key'))
        map(lambda x: incident_params.update({x: kwargs.pop(x)}),
            filter(lambda y: kwargs.get(y), ['client', 'client_url', 'contexts']))
        incident_params['details'] = kwargs
        return incident_params

    def trigger(self, **kwargs):
        return PagerDutyIncident.pager_duty.trigger_incident(**self._get_incident_params(**kwargs))
