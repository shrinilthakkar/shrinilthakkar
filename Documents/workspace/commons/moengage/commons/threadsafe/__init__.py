import os
import socket
import threading
import time
import uuid
from threading import Semaphore
from weakref import WeakKeyDictionary

from moengage.commons.utils.common import CommonUtils
from moengage.package.utils import PackageUtils


class Lockable(object):
    def __init__(self):
        self.lock = threading.Lock()

    def __enter__(self):
        self.acquire_lock()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release_lock()

    def acquire_lock(self):
        self.lock.acquire()

    def release_lock(self):
        self.lock.release()


class LockingDict(Lockable):
    def __init__(self):
        super(LockingDict, self).__init__()
        self.dict = {}

    def get_dict(self):
        return self.dict

    def set_dict(self, dictionary):
        self.dict = dictionary


class LockingSet(Lockable):
    def __init__(self):
        super(LockingSet, self).__init__()
        self.set = set()

    def add(self, item):
        self.set.add(item)

    def add_all(self, items):
        self.set.update(items)

    def remove(self, item):
        self.set.remove(item)

    def remove_all(self, items):
        self.set = self.set - set(items)

    def get_set(self):
        return self.set


class Barrier(object):
    def __init__(self, n):
        self.n = n
        self.count = 0
        self.mutex = Semaphore(1)
        self.barrier = Semaphore(0)

    def wait(self):
        self.mutex.acquire()
        self.count += 1
        self.mutex.release()
        if self.count == self.n:
            self.barrier.release()
        self.barrier.acquire()
        self.barrier.release()


class ThreadPoolExecutor(object):
    # TODO fix all threadpool references
    def __init__(self, max_threads, thread_class, params_list):
        self.max_threads = max_threads
        self.params_list = params_list
        self.thread_class = thread_class
        self.finished_threads = 0
        self.started_threads = 0
        self.running_threads = {}
        self.is_running = True
        self.params_count = 0

    def get_threads_to_run(self):
        for params in self.params_list:
            self.params_count += 1
            yield self.thread_class(*params)

    def can_execute_thread(self):
        return len(self.running_threads) < self.max_threads

    def execute_thread(self, thread):
        if not self.is_running:
            raise Exception('ThreadPool is stopped')
        thread.start()
        self.started_threads += 1
        self.running_threads[uuid.uuid4().hex[:8]] = thread

    def get_completed_thread(self):
        for thread_id, thread in self.running_threads.items():
            if not thread.isAlive():
                self.running_threads.pop(thread_id)
                self.finished_threads += 1
                yield thread
            else:
                time.sleep(0.1)

    def start(self):
        for thread in self.get_threads_to_run():
            while not self.can_execute_thread():
                for completed_thread in self.get_completed_thread():
                    yield completed_thread
            self.execute_thread(thread)
        while len(self.running_threads) > 0:
            for completed_thread in self.get_completed_thread():
                yield completed_thread

        if self.finished_threads != self.params_count:
            raise Exception('Finished threads and params count do not match')


class ThreadContext(object):
    def __init__(self, **kwargs):
        """
        Maintains thread level contexts
        :param kwargs: contains the default global context
        """
        self._thread_contexts = WeakKeyDictionary()
        self._global_context = dict(**kwargs)

    def __get_thread_key(self):
        try:
            from greenlet import getcurrent
            return getcurrent()
        except ImportError:
            return threading.current_thread()

    def __get_thread_context(self):
        """
        :return: context of the current thread, if no context exists, create one with global context values
        """
        thread_id = self.__get_thread_key()
        return self._thread_contexts.setdefault(thread_id, dict(self._global_context))

    def get(self, key):
        return self.__get_thread_context().get(key)

    def set(self, key, value):
        self.__get_thread_context()[key] = value

    def unset(self, key):
        self.__get_thread_context().pop(key, None)

    def updateContext(self, **kwargs):
        self.__get_thread_context().update(**kwargs)

    def removeContext(self, *keys):
        for key in keys:
            self.unset(key)

    def clearContext(self):
        thread_id = self.__get_thread_key()
        self._thread_contexts[thread_id] = dict(self._global_context)

    def to_dict(self):
        return self.__get_thread_context()


GLOBAL_CONTEXT = {
    'correlationId': CommonUtils.generateRandomString(6),
    'pid': str(os.getpid()),
    'host': socket.gethostbyname(socket.gethostname())
}

EXECUTION_CONTEXT = ThreadContext(**GLOBAL_CONTEXT)
