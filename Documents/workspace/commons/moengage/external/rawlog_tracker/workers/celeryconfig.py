from celery.schedules import crontab

from moengage.workers.config import WorkerConfig
from moengage.workers.config.celeryconfig import MOE_BROKER_TYPE

BROKER_URL = WorkerConfig.urlForBrokerType(MOE_BROKER_TYPE)

CELERY_IMPORTS = (
    'moengage.external.rawlogs_track.workers.rawlogs_worker'
)

CELERY_ROUTES = (
    {'moengage.external.rawlogs_track.workers.rawlogs_worker.calculate_size_rawlog': {
        'queue': 'rawlogs_size_tracker'}}
)

CELERYBEAT_SCHEDULE = {
    'track-rawlogs-everyday': {
        'task': 'moengage.external.rawlogs_track.workers.rawlogs_worker.calculate_size_rawlog',
        'schedule': crontab(hour='*/6', minute=30),  # This is in GMT, it will run at 6 AM IST
        'args': ()
    }
}
