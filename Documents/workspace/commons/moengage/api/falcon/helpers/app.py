import falcon


class App(falcon.API):
    def list_all_error_handlers(self):
        """
        Lists error handlers in decreasing order of priority

        :return: list of error handler
        """
        return self._error_handlers

    def list_all_routes(self):
        routes = []
        for route in self._router._roots:
            routes.append(dict(resource_class=route.resource.__class__, uri_template=route.uri_template))
        return routes
