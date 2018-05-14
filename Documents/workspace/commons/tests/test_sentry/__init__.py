import datetime
from unittest import TestCase

from bson import ObjectId
from enum import Enum
from mock import patch
from moengage.models.app import App

from moengage.commons.decorators import MemCached
from moengage.sentry.logger import SentryTreysor
from moengage.sentry.model import SentryAppDocument, SentryServiceDocument, SentryStatus


class SentryTestBase(TestCase):
    def setUp(self):
        self.patchers = []
        self.add_class_level_patches()
        for patch in self.patchers:
            # self.addCleanup(patch.stop)
            patch.start()
        self.initialize_vars()

    def add_class_level_patches(self):
        self.patchers.append(patch('moengage.sentry.alerts.SentryAlert.send',
                                   new=TestUtils.mock_alerts))
        for method in filter(lambda x: not x.startswith('_'), dir(SentryTreysor)):
            self.patchers.append(patch('moengage.sentry.logger.SentryTreysor.{0}'.format(str(method)),
                                       new=TestUtils.mock_logs))

    def initialize_vars(self):
        pass

    def tearDown(self):
        for patch in self.patchers:
            try:
                patch.stop()
            except RuntimeError:
                pass


class MockValues(object):
    @staticmethod
    def mock_sentry_dao_objects(return_value=None):
        def mock(*args, **kwargs):
            return return_value

        return mock


class TestUtils(object):
    db_name = 'DemoApp'
    app_key = 'DemoAppAppKey'
    service_name = 'sentry_test_case_DemoApp_service'
    test_time = datetime.datetime.utcnow()
    document_id = ObjectId('582c4b792bf7cbdec982bbee')

    @staticmethod
    def mock_alerts(*args, **kwargs):
        return {}

    @staticmethod
    def mock_logs(*args, **kwargs):
        return {}

    @staticmethod
    def dummy_sentry_app():
        dummy_sentry_document = SentryAppDocument()
        dummy_sentry_document.id = TestUtils.document_id
        dummy_sentry_document.app_key = TestUtils.app_key
        dummy_sentry_document.creation_time = TestUtils.test_time
        dummy_sentry_document.last_enable_time = TestUtils.test_time
        dummy_sentry_document.status = SentryStatus.ALLOWED
        return dummy_sentry_document

    @staticmethod
    def dummy_sentry_service():
        dummy_sentry_document = SentryServiceDocument()
        dummy_sentry_document.id = TestUtils.document_id
        dummy_sentry_document.app_key = TestUtils.app_key
        dummy_sentry_document.service_name = TestUtils.service_name
        dummy_sentry_document.creation_time = TestUtils.test_time
        dummy_sentry_document.last_enable_time = TestUtils.test_time
        dummy_sentry_document.status = SentryStatus.ALLOWED
        return dummy_sentry_document

    @staticmethod
    def dummy_app_object():
        dummy_app = App()
        dummy_app.db_name = TestUtils.db_name
        dummy_app.app_key = TestUtils.app_key
        dummy_app.name = 'SentryTestApp'
        return dummy_app


    @staticmethod
    def dummy_sentry_doc(document_type):
        pass

    @staticmethod
    def clear_mem_cache(sentry_cache_key):
        cached_data = MemCached(sentry_cache_key)._data
        for key in cached_data.get_keys():
            if key.endswith(sentry_cache_key):
                cached_data.remove_key(key)
