import multiprocessing
import multiprocessing.pool
import signal
import threading
from functools import wraps
from abc import ABCMeta, abstractmethod


class Timeout(object):
    def __init__(self, timeout_handler, error_message=None, timeout=150):
        self.timeout = timeout
        self.timeout_handler = timeout_handler
        if error_message:
            self.set_error_message(error_message)

    def set_error_message(self, error_message):
        self.timeout_handler.set_error_message(error_message)

    def __call__(self, func):
        def signal_timeout(*args, **kwargs):
            signal.signal(signal.SIGALRM, self.timeout_handler.raise_timeout_error)
            signal.alarm(self.timeout)
            try:
                return_value = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return return_value

        def thread_timeout(*args, **kwargs):
            try:
                pool = multiprocessing.pool.ThreadPool(processes=1)
                async_result = pool.apply_async(func, args, kwargs)
                return async_result.get(timeout=self.timeout)
            except multiprocessing.TimeoutError:
                self.timeout_handler.raise_timeout_error()

        @wraps(func)
        def timeout(*args, **kwargs):
            if isinstance(threading.current_thread(), threading._MainThread):
                return signal_timeout(*args, **kwargs)
            else:
                return thread_timeout(*args, **kwargs)

        return timeout


class TimeOutHandler(object):
    __metaclass__ = ABCMeta

    def __init__(self, error_message):
        self.error_message = error_message

    @abstractmethod
    def raise_timeout_error(self, signum=None, frame=None):
        raise NotImplementedError("Child Class must implement raise_timeout_error function from super")

    def set_error_message(self, error_message):
        self.error_message = error_message
