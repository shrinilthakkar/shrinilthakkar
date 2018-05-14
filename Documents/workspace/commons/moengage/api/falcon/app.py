import newrelic.agent

from moengage.api.falcon.helpers.app import App
from moengage.api.falcon.handlers.exception_handler import GenericExceptionHandler
from moengage.api.falcon.handlers.health_check_exception_handler import HealthCheckRouteExceptionHandler
from moengage.api.falcon.middleware import (JSONBodyParser, RequestLogger)
from moengage.api.falcon.middleware import HealthCheckMiddleware
from moengage.api.falcon.middleware import RequestMeta
from moengage.api.utils.exceptions import HealthCheckRouteException
from moengage.commons import SingletonMetaClass


class FalconApp(object):
    """
    A class which creates basic falcon app which can be extended to add routes and further middleware
    app_name: domain name/application name of the product
    new_relic_path: new_relic config path
    """
    # At a time only one instance of newrelic can be setup and initialized so
    __metaclass__ = SingletonMetaClass

    def __init__(self, new_relic_path, app_name):
        self.new_relic_path = new_relic_path
        self.app_name = app_name
        self._middleware = [RequestMeta(self.app_name), RequestLogger(), HealthCheckMiddleware(), JSONBodyParser()]
        self._exception_handler = [(Exception, GenericExceptionHandler().handle),
                                   (HealthCheckRouteException, HealthCheckRouteExceptionHandler().handle)]
        self.app = None

    def init_app(self):
        newrelic.agent.initialize(self.new_relic_path)
        self.app = newrelic.agent.WSGIApplicationWrapper(App(middleware=self._middleware))
        # LIFO order
        for exc_handler in self._exception_handler:
            self.app.add_error_handler(exc_handler[0], exc_handler[1])

    @property
    def middleware(self):
        """
        :return: middlewares
        """
        return self._middleware

    def add_middleware(self, middleware, index=None):
        """
        add middleware at given index or at the end if index not provided
        new middleware can not be added at start(0) position as it is reserved
        for RequestMeta
        :param middleware: Object
        :param index: int
        :return:
        """

        position = index if index is not None and index > 0 else len(self.middleware)
        self._middleware.insert(position, middleware)

    def append_error_handler(self, exception_class, handler):
        """
        It adds error handler in lifo order
        :param exception_class: Exception Class
        :param handler: Handler function
        :return:
        """
        if self.app is not None:
            self.app.add_error_handler(exception_class, handler)
        else:
            self._exception_handler.append((exception_class, handler))
