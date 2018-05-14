import os
import subprocess
from enum import Enum

from moengage.workers.utils import WorkerUtils
from moengage.workers.config import WorkerTags, WorkerConfig


class WorkerRunMode(Enum):
    GEVENT = 1
    CONCURRENCY = 2

    def parameterString(self, concurrency=1):
        return {
            WorkerRunMode.GEVENT: '-P eventlet',
            WorkerRunMode.CONCURRENCY: '--concurrency=' + str(concurrency)
        }.get(self)

    def __str__(self):
        return {action: str(action.name).lower() for action in WorkerRunMode}.get(self)

    @staticmethod
    def fromStr(value):
        return {str(action.name).lower(): action for action in WorkerRunMode}.get(value, WorkerRunMode.GEVENT)


class WorkerBase(object):
    def __init__(self, worker_name=None, celery_type=None):
        self._worker_tags = WorkerUtils.getWorkerConfigTags()
        self._worker_name = worker_name or self._worker_tags.get(WorkerTags.NAME, '')
        self._celery_type = celery_type
        self._worker_utils = WorkerUtils(self._worker_name, celery_type=self._celery_type)
        self._worker_initd_path = self._worker_utils.getConfigSavePath(self._celery_type.celeryConfig())

    def __createConfigFile(self, config_type, config_dict):
        file_path = config_dict.get('path')
        config = config_dict.get('config')
        file_folder = os.path.dirname(file_path)
        if not os.path.exists(file_folder):
            os.makedirs(file_folder)
            os.chmod(file_folder, 0o777)
        if config_type == WorkerConfig.LOG_DIR:
            os.makedirs(file_path)
        else:
            with open(file_path, 'w') as config_file:
                config_file.write(config)
        if config_type in [WorkerConfig.INITD, WorkerConfig.BEAT_INITD]:
            os.chmod(file_path, 0o755)
        elif config_type == WorkerConfig.LOG_DIR:
            os.chmod(file_path, 0o777)

    def __getRunMode(self, run_mode=None):
        worker_run_mode = run_mode or self._worker_tags.get(WorkerTags.RUN_MODE, '')
        return WorkerRunMode.fromStr(worker_run_mode)

    def __getNumNodes(self, num_nodes=None):
        worker_num_nodes = num_nodes or self._worker_tags.get(WorkerTags.NUM_NODES, 1)
        return worker_num_nodes

    def __getNodeNames(self, num_nodes):
        node_names = ""
        for i in range(0, int(num_nodes)):
            node_name = self._worker_name + str(i+1) + " "
            node_names += node_name
        return '"' + node_names.strip() + '"'

    def __getNodeConcurrency(self, concurrency):
        node_concurrency = concurrency or self._worker_tags.get(WorkerTags.NODE_CONCURRENCY, 1)
        return node_concurrency

    def __getCeleryApp(self, celery_app):
        worker_celery_app = celery_app or self._worker_tags.get(WorkerTags.CELERY_APP, '')
        return worker_celery_app

    def __runInitCommand(self, init_file, command):
        exec_command = init_file + ' ' + command
        print "Executing command - " + exec_command
        p = subprocess.Popen(exec_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in p.stdout.readlines():
            print line
        retval = p.wait()
        print "Exited with status: %r" % retval

    def create(self, celery_app='', num_nodes=None, worker_queues='', run_mode=None, concurrency=0):
        queues = worker_queues or self._worker_tags.get(WorkerTags.QUEUE_NAMES, '')
        worker_run_mode = self.__getRunMode(run_mode)
        worker_num_nodes = self.__getNumNodes(num_nodes)
        node_names = self.__getNodeNames(worker_num_nodes)
        node_concurrency = self.__getNodeConcurrency(concurrency)
        worker_celery_app = self.__getCeleryApp(celery_app)
        worker_config = self._worker_utils.getWorkerConfig(node_names, queues,
                                                           worker_run_mode.parameterString(node_concurrency),
                                                           celery_app=worker_celery_app)
        for config_type, config_dict in worker_config.items():
            self.__createConfigFile(config_type, config_dict)
        return worker_config

    def start(self):
        self.__runInitCommand(self._worker_initd_path, "start")

    def stop(self):
        self.__runInitCommand(self._worker_initd_path, "stop")

    def restart(self):
        self.__runInitCommand(self._worker_initd_path, "restart")

    def dry_run(self):
        self.__runInitCommand(self._worker_initd_path, "dry-run")

    def exists(self):
        worker_exists = os.path.exists(self._worker_initd_path)
        print self._worker_name + ": Exists=%r" % worker_exists
        return worker_exists
