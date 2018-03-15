import datetime

from freezegun.api import freeze_time
from mock import patch

from moengage.sentry.dao import SentryAppDAO, SentryServiceDAO
from moengage.sentry.model import SentryStatus, SentryAppDocument, SentryServiceDocument
from moengage.sentry.service import SentryService
from tests.test_sentry import TestUtils, SentryTestBase, MockValues


class TestSentryService(SentryTestBase):
    def add_class_level_patches(self):
        super(TestSentryService, self).add_class_level_patches()
        self.patchers.append(patch('moengage.sentry.dao.SentryServiceDAO.save', new=MockValues.mock_sentry_dao_objects()))
        self.patchers.append(patch('moengage.sentry.dao.SentryServiceDAO.findOne',
                                   new=MockValues.mock_sentry_dao_objects(TestUtils.dummy_sentry_service())))
        self.patchers.append(patch('moengage.sentry.dao.SentryAppDAO.save', new=MockValues.mock_sentry_dao_objects()))
        self.patchers.append(patch('moengage.sentry.dao.SentryAppDAO.findOne',
                                   new=MockValues.mock_sentry_dao_objects(TestUtils.dummy_sentry_app())))

    def initialize_vars(self):
        self.sentry_service_app_key = SentryService(TestUtils.app_key)
        self.sentry_service_db_name = SentryService(TestUtils.db_name)

    def test_get_sentry_document(self):
        self.assertEqual(TestUtils.dummy_sentry_app(), self.sentry_service_app_key._getSentryDocument(SentryAppDAO()))
        self.assertEqual(TestUtils.dummy_sentry_app(), self.sentry_service_db_name._getSentryDocument(SentryAppDAO()))
        self.assertEqual(TestUtils.dummy_sentry_service(),
                         self.sentry_service_app_key._getSentryDocument(SentryServiceDAO(TestUtils.service_name)))
        self.assertEqual(TestUtils.dummy_sentry_service(),
                         self.sentry_service_db_name._getSentryDocument(SentryServiceDAO(TestUtils.service_name)))

    @patch('moengage.sentry.service.AppService.getAppByDBName')
    @patch('moengage.sentry.service.AppService.getAppByAppKey')
    def test_get_app_object(self, app_by_app_key, app_by_db_name):
        app_obj = TestUtils.dummy_app_object()
        app_by_app_key.return_value = app_obj
        app_by_db_name.return_value = app_obj
        self.assertEqual(app_obj, self.sentry_service_app_key._getAppObject())
        self.assertEqual(app_obj, self.sentry_service_db_name._getAppObject())

    def test_get_app_cache_key(self):
        self.assertEqual(self.sentry_service_app_key._getAppCacheKey(), 'sentry_app_' + TestUtils.app_key)
        self.assertEqual(self.sentry_service_db_name._getAppCacheKey(), 'sentry_app_' + TestUtils.db_name)

    def test_get_service_cache_key(self):
        self.assertEqual(self.sentry_service_app_key._getServiceCacheKey(TestUtils.service_name),
                         'sentry_service_' + TestUtils.app_key + '_' + TestUtils.service_name)
        self.assertEqual(self.sentry_service_db_name._getServiceCacheKey(TestUtils.service_name),
                         'sentry_service_' + TestUtils.db_name + '_' + TestUtils.service_name)

    @freeze_time(datetime.datetime(2017, 02, 22, 0, 0, 0))
    def test_update_sentry_status_allowed(self):
        dummy_sentry = TestUtils.dummy_sentry_app()
        self.sentry_service_app_key._updateSentryStatus(dummy_sentry, SentryStatus.ALLOWED)
        self.assertEqual(dummy_sentry.status, SentryStatus.ALLOWED)
        self.assertEqual(dummy_sentry.last_enable_time, datetime.datetime(2017, 02, 22, 0, 0, 0))

    @freeze_time(datetime.datetime(2017, 02, 21, 0, 0, 0))
    def test_update_sentry_status_blocked(self):
        dummy_sentry = TestUtils.dummy_sentry_app()
        self.sentry_service_app_key._updateSentryStatus(dummy_sentry, SentryStatus.BLOCKED)
        self.assertEqual(dummy_sentry.status, SentryStatus.BLOCKED)
        self.assertEqual(dummy_sentry.last_disable_time, datetime.datetime(2017, 02, 21, 0, 0, 0))

    @freeze_time(datetime.datetime(2017, 02, 20, 0, 0, 0))
    def test_create_sentry_document_app_allowed(self):
        app_obj = TestUtils.dummy_app_object()
        sentry_doc = self.sentry_service_app_key._createSentryDocument(app_obj, SentryAppDocument, SentryStatus.ALLOWED)
        self.assertEqual(sentry_doc.app_key, app_obj.app_key)
        self.assertEqual(sentry_doc.db_name, app_obj.db_name)
        self.assertEqual(sentry_doc.last_enable_time, datetime.datetime(2017, 02, 20, 0, 0, 0))
        self.assertEqual(sentry_doc.creation_time, datetime.datetime(2017, 02, 20, 0, 0, 0))
        self.assertEqual(sentry_doc.status, SentryStatus.ALLOWED)

    @freeze_time(datetime.datetime(2017, 02, 20, 0, 0, 0))
    def test_create_sentry_document_app_blocked(self):
        app_obj = TestUtils.dummy_app_object()
        sentry_doc = self.sentry_service_app_key._createSentryDocument(app_obj, SentryAppDocument, SentryStatus.BLOCKED)
        self.assertEqual(sentry_doc.app_key, app_obj.app_key)
        self.assertEqual(sentry_doc.db_name, app_obj.db_name)
        self.assertEqual(sentry_doc.last_disable_time, datetime.datetime(2017, 02, 20, 0, 0, 0))
        self.assertEqual(sentry_doc.creation_time, datetime.datetime(2017, 02, 20, 0, 0, 0))
        self.assertEqual(sentry_doc.status, SentryStatus.BLOCKED)

    @freeze_time(datetime.datetime(2017, 02, 20, 0, 0, 0))
    def test_create_sentry_document_service_allowed(self):
        app_obj = TestUtils.dummy_app_object()
        sentry_doc = self.sentry_service_app_key._createSentryDocument(app_obj, SentryServiceDocument, SentryStatus.ALLOWED)
        self.assertEqual(sentry_doc.app_key, app_obj.app_key)
        self.assertEqual(sentry_doc.db_name, app_obj.db_name)
        self.assertEqual(sentry_doc.last_enable_time, datetime.datetime(2017, 02, 20, 0, 0, 0))
        self.assertEqual(sentry_doc.creation_time, datetime.datetime(2017, 02, 20, 0, 0, 0))
        self.assertEqual(sentry_doc.status, SentryStatus.ALLOWED)

    @freeze_time(datetime.datetime(2017, 02, 20, 0, 0, 0))
    def test_create_sentry_document_service_blocked(self):
        app_obj = TestUtils.dummy_app_object()
        sentry_doc = self.sentry_service_app_key._createSentryDocument(app_obj, SentryServiceDocument, SentryStatus.BLOCKED)
        self.assertEqual(sentry_doc.app_key, app_obj.app_key)
        self.assertEqual(sentry_doc.db_name, app_obj.db_name)
        self.assertEqual(sentry_doc.last_disable_time, datetime.datetime(2017, 02, 20, 0, 0, 0))
        self.assertEqual(sentry_doc.creation_time, datetime.datetime(2017, 02, 20, 0, 0, 0))
        self.assertEqual(sentry_doc.status, SentryStatus.BLOCKED)
