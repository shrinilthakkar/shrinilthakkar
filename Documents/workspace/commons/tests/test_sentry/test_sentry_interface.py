from mock import patch

from moengage.sentry.exceptions import SentryDocumentNotFoundException
from moengage.sentry.interface import Sentry
from moengage.sentry.model import SentryStatus
from tests.test_sentry import SentryTestBase, TestUtils


class TestSentry(SentryTestBase):
    @patch('moengage.sentry.service.SentryService.createAppDocument')
    @patch('moengage.sentry.service.SentryService.getAppDocument')
    def test_get_app_status_existing_app(self, mock_app_document, mock_create_document):
        dummy_sentry_document = TestUtils.dummy_sentry_app()
        mock_app_document.return_value = dummy_sentry_document
        mock_create_document.return_value = None
        sentry = Sentry(TestUtils.app_key)
        status = sentry.app_status()
        self.assertEqual(status, SentryStatus.ALLOWED)

    @patch('moengage.sentry.service.SentryService.createAppDocument')
    @patch('moengage.sentry.service.SentryService.getAppDocument')
    def test_get_app_status_new_app(self, mock_app_document, mock_create_document):
        dummy_sentry_document = TestUtils.dummy_sentry_app()
        mock_app_document.return_value = None
        mock_create_document.return_value = dummy_sentry_document
        sentry = Sentry(TestUtils.app_key)
        status = sentry.app_status(default_status=SentryStatus.ALLOWED)
        self.assertEqual(status, SentryStatus.ALLOWED)

    @patch('moengage.sentry.service.SentryService.createAppDocument')
    @patch('moengage.sentry.service.SentryService.getAppDocument')
    def test_get_app_status_new_app_no_default(self, mock_app_document, mock_create_document):
        dummy_sentry_document = TestUtils.dummy_sentry_app()
        mock_app_document.return_value = None
        mock_create_document.return_value = dummy_sentry_document
        sentry = Sentry(TestUtils.app_key)
        with self.assertRaises(SentryDocumentNotFoundException):
            sentry.app_status()

    @patch('moengage.sentry.service.SentryService.createAppDocument')
    @patch('moengage.sentry.service.SentryService.updateAppDocument')
    def test_update_app_status_existing_app(self, mock_app_document, mock_create_document):
        dummy_sentry_document = TestUtils.dummy_sentry_app()
        dummy_sentry_document.status = SentryStatus.BLOCKED
        mock_app_document.return_value = dummy_sentry_document
        mock_create_document.return_value = None
        sentry = Sentry(TestUtils.app_key)
        status = sentry.set_app_status(SentryStatus.BLOCKED)
        self.assertEqual(status, SentryStatus.BLOCKED)

    @patch('moengage.sentry.service.SentryService.createAppDocument')
    @patch('moengage.sentry.service.SentryService.updateAppDocument')
    def test_update_app_status_new_app(self, mock_app_document, mock_create_document):
        dummy_sentry_document = TestUtils.dummy_sentry_app()
        dummy_sentry_document.status = SentryStatus.BLOCKED
        mock_app_document.return_value = None
        mock_create_document.return_value = dummy_sentry_document
        sentry = Sentry(TestUtils.app_key)
        status = sentry.set_app_status(SentryStatus.BLOCKED)
        self.assertEqual(status, SentryStatus.BLOCKED)

    @patch('moengage.sentry.service.SentryService.createServiceDocument')
    @patch('moengage.sentry.service.SentryService.getServiceDocument')
    def test_get_service_status_existing_service(self, mock_service_document, mock_create_document):
        dummy_sentry_document = TestUtils.dummy_sentry_service()
        mock_service_document.return_value = dummy_sentry_document
        mock_create_document.return_value = None
        sentry = Sentry(TestUtils.app_key)
        status = sentry.service_status(TestUtils.service_name)
        self.assertEqual(status, SentryStatus.ALLOWED)

    @patch('moengage.sentry.service.SentryService.createServiceDocument')
    @patch('moengage.sentry.service.SentryService.getServiceDocument')
    def test_get_service_status_new_service(self, mock_service_document, mock_create_document):
        dummy_sentry_document = TestUtils.dummy_sentry_service()
        mock_service_document.return_value = None
        mock_create_document.return_value = dummy_sentry_document
        sentry = Sentry(TestUtils.app_key)
        status = sentry.service_status(TestUtils.service_name, default_status=SentryStatus.ALLOWED)
        self.assertEqual(status, SentryStatus.ALLOWED)

    @patch('moengage.sentry.service.SentryService.createServiceDocument')
    @patch('moengage.sentry.service.SentryService.getServiceDocument')
    def test_get_service_status_new_service_no_default(self, mock_service_document, mock_create_document):
        dummy_sentry_document = TestUtils.dummy_sentry_service()
        mock_service_document.return_value = None
        mock_create_document.return_value = dummy_sentry_document
        sentry = Sentry(TestUtils.app_key)
        with self.assertRaises(SentryDocumentNotFoundException):
            sentry.service_status(TestUtils.service_name)

    @patch('moengage.sentry.service.SentryService.createServiceDocument')
    @patch('moengage.sentry.service.SentryService.updateServiceDocument')
    def test_update_service_status_existing_service(self, mock_service_document, mock_create_document):
        dummy_sentry_document = TestUtils.dummy_sentry_service()
        dummy_sentry_document.status = SentryStatus.BLOCKED
        mock_service_document.return_value = dummy_sentry_document
        mock_create_document.return_value = None
        sentry = Sentry(TestUtils.app_key)
        status = sentry.set_service_status(TestUtils.service_name, SentryStatus.BLOCKED)
        self.assertEqual(status, SentryStatus.BLOCKED)

    @patch('moengage.sentry.service.SentryService.createServiceDocument')
    @patch('moengage.sentry.service.SentryService.updateServiceDocument')
    def test_update_service_status_existing_service(self, mock_service_document, mock_create_document):
        dummy_sentry_document = TestUtils.dummy_sentry_app()
        dummy_sentry_document.status = SentryStatus.BLOCKED
        mock_service_document.return_value = None
        mock_create_document.return_value = dummy_sentry_document
        sentry = Sentry(TestUtils.app_key)
        status = sentry.set_service_status(TestUtils.service_name, SentryStatus.BLOCKED)
        self.assertEqual(status, SentryStatus.BLOCKED)

    @patch('moengage.sentry.interface.Sentry.service_status')
    @patch('moengage.sentry.interface.Sentry.app_status')
    def test_get_app_service_status_app_enabled_service_enabled(self, mock_app_status, mock_service_status):
        mock_app_status.return_value = SentryStatus.ALLOWED
        mock_service_status.return_value = SentryStatus.ALLOWED
        sentry = Sentry(TestUtils.app_key)
        status = sentry.app_service_status(TestUtils.service_name)
        self.assertEqual(status, SentryStatus.ALLOWED)

    @patch('moengage.sentry.interface.Sentry.service_status')
    @patch('moengage.sentry.interface.Sentry.app_status')
    def test_get_app_service_status_app_enabled_service_disabled(self, mock_app_status, mock_service_status):
        mock_app_status.return_value = SentryStatus.ALLOWED
        mock_service_status.return_value = SentryStatus.BLOCKED
        sentry = Sentry(TestUtils.app_key)
        status = sentry.app_service_status(TestUtils.service_name)
        self.assertEqual(status, SentryStatus.BLOCKED)

    @patch('moengage.sentry.interface.Sentry.service_status')
    @patch('moengage.sentry.interface.Sentry.app_status')
    def test_get_app_service_status_app_disabled(self, mock_app_status, mock_service_status):
        mock_app_status.return_value = SentryStatus.BLOCKED
        mock_service_status.return_value = SentryStatus.ALLOWED
        sentry = Sentry(TestUtils.app_key)
        status = sentry.app_service_status(TestUtils.service_name)
        self.assertEqual(status, SentryStatus.BLOCKED)
