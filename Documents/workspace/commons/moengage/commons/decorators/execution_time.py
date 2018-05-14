from datetime import datetime
from functools import wraps


class LogExecutionTime(object):
    def __init__(self, logger, log_args=True, execution_id=None):
        self.logger = logger
        self.log_args = log_args
        self.execution_id = execution_id

    def __call__(self, func):
        @wraps(func)
        def logger(*args, **kwargs):
            start = datetime.utcnow()
            return_value = func(*args, **kwargs)
            end = datetime.utcnow()
            time_taken = end - start
            log_dict = {
                "function": str(func.__name__),
                "module": str(func.__module__),
                "time_taken": str(time_taken)
            }
            if self.execution_id:
                log_dict['execution_id'] = str(self.execution_id)
            if self.log_args:
                log_dict['args'] = str(args)
                log_dict['kwargs'] = str(kwargs)
            self.logger.info(**log_dict)
            return {
                'return_value': return_value,
                'time_taken': time_taken
            }

        return logger
