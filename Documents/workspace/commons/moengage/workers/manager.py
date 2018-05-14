import sys
from enum import Enum
from moengage.workers.base import WorkerBase
from moengage.workers.config import WorkerConfig


class WorkerAction(Enum):
    CREATE = 1
    START = 2
    STOP = 3
    RESTART = 4
    EXISTS = 5
    DRY_RUN = 6

    def __str__(self):
        return {action: str(action.name).lower() for action in WorkerAction}.get(self)

    @staticmethod
    def fromStr(value):
        return {str(action.name).lower(): action for action in WorkerAction}.get(value)


class CeleryType(Enum):
    WORKER = 1
    BEAT = 2

    def __str__(self):
        return {celery_type: str(celery_type.name).lower() for celery_type in CeleryType}.get(self)

    @staticmethod
    def fromStr(value):
        return {str(celery_type.name).lower(): celery_type for celery_type in CeleryType}.get(value)

    def celeryConfig(self):
        return {
            CeleryType.WORKER: WorkerConfig.INITD,
            CeleryType.BEAT: WorkerConfig.BEAT_INITD
        }.get(self)


class WorkerManager(object):
    def __init__(self, worker_name, celery_type=CeleryType.WORKER):
        self._worker_name = worker_name
        self._celery_type = celery_type

    def performAction(self, action, action_args):
        worker_base = WorkerBase(self._worker_name, celery_type=self._celery_type)
        action_handler = worker_base.__getattribute__(str(action))
        action_handler(*action_args)


def main():
    action = WorkerAction.fromStr(sys.argv[1])
    worker_name = sys.argv[2] if len(sys.argv) >= 3 else None
    action_args = sys.argv[3:] if len(sys.argv) >= 4 else []
    worker_manager = WorkerManager(worker_name)
    worker_manager.performAction(action, action_args)


def beat_main():
    action = WorkerAction.fromStr(sys.argv[1])
    worker_name = sys.argv[2] if len(sys.argv) >= 3 else None
    action_args = sys.argv[3:] if len(sys.argv) >= 4 else []
    worker_manager = WorkerManager(worker_name, celery_type=CeleryType.BEAT)
    worker_manager.performAction(action, action_args)
