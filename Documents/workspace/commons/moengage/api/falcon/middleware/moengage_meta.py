import falcon
from moengage.services.app_service import AppService


class MoEngageAppMeta(object):
    def process_request(self, req, resp):
        moengage_app_key = req.get_header('X-MOENGAGE-APP-KEY')
        moengage_db_name = req.get_header('X-MOENGAGE-DBNAME')

        if not (moengage_app_key or moengage_db_name):
            raise falcon.HTTPBadRequest('Missing Required Headers', "One of 'X-MOENGAGE-APP-KEY' or "
                                                                    "'X-MOENGAGE-DBNAME' should be passed")

        app_service = AppService()
        app_object = None
        if moengage_app_key:
            app_object = app_service.getAppByAppKey(moengage_app_key)
        elif moengage_db_name:
            app_object = app_service.getAppByDBName(moengage_db_name)
        if not app_object:
            raise falcon.HTTPBadRequest('Request Error', 'MoEngage Client not found. Please check values for headers - '
                                                         'X-MOENGAGE-APP-KEY or X-MOENGAGE-DBNAME')
        req.context['app_object'] = app_object
        req.context['app_key'] = app_object.app_key
        req.context['db_name'] = app_object.db_name
