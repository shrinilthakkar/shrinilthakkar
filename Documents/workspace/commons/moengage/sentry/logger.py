from moengage.commons.loggers import Treysor


class SentryTreysor(Treysor):
    def __init__(self):
        super(SentryTreysor, self).__init__('sentry')
