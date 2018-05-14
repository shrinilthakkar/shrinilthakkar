from moengage.workers.config import WorkerBrokerType
from moengage.commons.connections import ConnectionUtils
from moengage.package.utils import PackageUtils

MOE_BROKER_TYPE = WorkerBrokerType.brokerTypeForEnv(PackageUtils.getExecutionEnv())

CELERY_IGNORE_RESULT = True

CELERY_SEND_TASK_ERROR_EMAILS = False

BROKER_TRANSPORT_OPTIONS = {
    'polling_interval': 2,
    'region': ConnectionUtils.getSQSConnection().auth_region_name or 'us-east-1'
}
