from moengage.package.utils import PackageUtils
from moengage.workers.config import WorkerConfig, WorkerTags


class WorkerUtils(object):
    WORKERS_BASE_PATH = '/opt/workers/'

    def __init__(self, worker_name, celery_type=None):
        self._worker_name = worker_name
        self._celery_type = celery_type

    def getConfigSavePath(self, config_type):
        return config_type.configSavePath(WorkerUtils.WORKERS_BASE_PATH, self._worker_name)

    def getConfigDict(self, config, config_path):
        return {
            'path': config_path,
            'config': config
        }

    def getNewRelicConfig(self):
        new_relic = WorkerConfig.NEW_RELIC
        nr_config_template = new_relic.readTemplate()
        nr_config = nr_config_template.replace('{worker_name}', self._worker_name)
        nr_config_path = self.getConfigSavePath(new_relic)
        return self.getConfigDict(nr_config, nr_config_path)

    def getWorkerLogDirectory(self):
        log_dir = WorkerConfig.LOG_DIR
        log_dir_path = self.getConfigSavePath(log_dir)
        return self.getConfigDict('', log_dir_path)

    def getLogstashConfig(self, treysor_config_path):
        logstash = WorkerConfig.LOGSTASH
        logstash_config_template = logstash.readTemplate()
        logstash_config = logstash_config_template.replace('{worker_treysor_config_file}', treysor_config_path)
        logstash_config_path = self.getConfigSavePath(logstash)
        return self.getConfigDict(logstash_config, logstash_config_path)

    def getSupervisorConfig(self, logstash_config_path):
        supervisor = WorkerConfig.SUPERVISOR
        supervisor_config_template = supervisor.readTemplate()
        supervisor_config = supervisor_config_template.replace('{worker_logstash_config_path}', logstash_config_path)
        supervisor_config_path = self.getConfigSavePath(supervisor)
        return self.getConfigDict(supervisor_config, supervisor_config_path)

    def getTreysorConfig(self):
        treysor = WorkerConfig.TREYSOR
        treysor_config = treysor.readTemplate()
        treysor_config_path = self.getConfigSavePath(treysor)
        return self.getConfigDict(treysor_config, treysor_config_path)

    def getWorkerCeleryConfig(self, node_names, queue_names, run_mode):
        celeryconfig = WorkerConfig.CELERYCONFIG
        celery_config_template = celeryconfig.readTemplate()
        celery_config = celery_config_template.replace('{worker_queue_names}', queue_names)
        celery_config = celery_config.replace('{worker_nodes_list}', node_names)
        celery_config = celery_config.replace('{worker_run_mode}', run_mode)
        celery_config = celery_config.replace('{worker_logs_dir}', self.getWorkerLogDirectory().get('path'))
        celery_config_path = self.getConfigSavePath(celeryconfig)
        return self.getConfigDict(celery_config, celery_config_path)

    def getWorkerInitConfig(self, new_relic_config_path, celery_config_path, celery_app=''):
        initd = WorkerConfig.INITD
        initd_template = initd.readTemplate()
        initd_config = initd_template.replace('{worker_new_relic_config_file}', new_relic_config_path)
        initd_config = initd_config.replace('{worker_celery_config_path}', celery_config_path)
        initd_config = initd_config.replace('{worker_celery_app}', celery_app)
        initd_config_path = self.getConfigSavePath(initd)
        return self.getConfigDict(initd_config, initd_config_path)

    def getBeatInitConfig(self, new_relic_config_path, celery_config_path, celery_app=''):
        beat_initd = WorkerConfig.BEAT_INITD
        beat_initd_template = beat_initd.readTemplate()
        beat_initd_config = beat_initd_template.replace('{worker_new_relic_config_file}', new_relic_config_path)
        beat_initd_config = beat_initd_config.replace('{worker_celery_config_path}', celery_config_path)
        beat_initd_config = beat_initd_config.replace('{worker_celery_app}', celery_app)
        beat_initd_config_path = self.getConfigSavePath(beat_initd)
        return self.getConfigDict(beat_initd_config, beat_initd_config_path)

    def getBeatCeleryConfig(self):
        celeryconfig = WorkerConfig.BEAT_CELERYCONFIG
        celery_config_template = celeryconfig.readTemplate()
        celery_config = celery_config_template.replace('{worker_logs_dir}', self.getWorkerLogDirectory().get('path'))
        celery_config = celery_config.replace('{worker_name}', self._worker_name)
        celery_config_path = self.getConfigSavePath(celeryconfig)
        return self.getConfigDict(celery_config, celery_config_path)

    def getWorkerConfig(self, node_names, queue_names, run_mode, celery_app=''):
        from moengage.workers.manager import CeleryType
        worker_config = dict()
        worker_config[WorkerConfig.TREYSOR] = self.getTreysorConfig()
        worker_config[WorkerConfig.LOGSTASH] = self.getLogstashConfig(
            worker_config[WorkerConfig.TREYSOR].get('path', '')
        )
        worker_config[WorkerConfig.SUPERVISOR] = self.getSupervisorConfig(
            worker_config[WorkerConfig.LOGSTASH].get('path', '')
        )
        worker_config[WorkerConfig.NEW_RELIC] = self.getNewRelicConfig()
        if self._celery_type == CeleryType.WORKER:
            worker_config[WorkerConfig.CELERYCONFIG] = self.getWorkerCeleryConfig(node_names, queue_names, run_mode)
            worker_config[WorkerConfig.INITD] = self.getWorkerInitConfig(
                worker_config[WorkerConfig.NEW_RELIC].get('path', ''),
                worker_config[WorkerConfig.CELERYCONFIG].get('path', ''),
                celery_app
            )
        elif self._celery_type == CeleryType.BEAT:
            worker_config[WorkerConfig.BEAT_CELERYCONFIG] = self.getBeatCeleryConfig()
            worker_config[WorkerConfig.BEAT_INITD] = self.getBeatInitConfig(
                worker_config[WorkerConfig.NEW_RELIC].get('path', ''),
                worker_config[WorkerConfig.BEAT_CELERYCONFIG].get('path', ''),
                celery_app
            )
        worker_config[WorkerConfig.LOG_DIR] = self.getWorkerLogDirectory()
        return worker_config

    @staticmethod
    def getWorkerConfigTags():
        instance_tags = PackageUtils.getCurrentEC2Instance().tags
        worker_tags = filter(lambda x: WorkerTags.fromStr(x['Key']) in WorkerTags, instance_tags)
        return {WorkerTags.fromStr(worker_tag['Key']): worker_tag['Value'] for worker_tag in worker_tags}
