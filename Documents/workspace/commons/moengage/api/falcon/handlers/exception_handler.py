import falcon

from moengage.commons.loggers import Treysor


class GenericExceptionHandler(object):

    @staticmethod
    def get_error_code(logger):
        correlation_id = logger.correlationId
        request_id = logger.getContext().get('request_id', '')
        code = correlation_id + '-' + request_id if request_id else correlation_id
        return code

    def handle(self, ex, req, resp, params):
        if isinstance(ex, falcon.HTTPError):
            raise ex
        logger = req.context.get('logger', Treysor(req.context.get('api_app_name')))
        logger.exception(log_tag='unexpected_exception', exception_type=str(type(ex)), message=str(ex.message))
        raise falcon.HTTPInternalServerError('Server Error',
                                             'An unexpected error was encountered while processing this request. '
                                             'Please contact MoEngage Team',
                                             code=self.get_error_code(logger))
