from moengage.commons.threadsafe import EXECUTION_CONTEXT
from moengage.commons.loggers.treysor import Treysor


class ContextLogger(object):
    LOGGER = Treysor()

    @property
    def logger(self):
        return EXECUTION_CONTEXT.get('logger') or self.LOGGER
