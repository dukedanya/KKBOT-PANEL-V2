import unittest
from unittest.mock import patch

from services.yookassa import YooKassaAPI


class YooKassaStatusTests(unittest.TestCase):
    def test_status_helpers(self):
        self.assertEqual(YooKassaAPI.extract_status({"status": "succeeded"}), "succeeded")
        self.assertTrue(YooKassaAPI.is_success_status({"status": "succeeded"}))
        self.assertTrue(YooKassaAPI.is_failed_status({"status": "canceled"}))
        self.assertTrue(YooKassaAPI.is_waiting_status({"status": "pending"}))

    def test_checkout_url_normalization(self):
        normalized = YooKassaAPI._normalize_payment({
            "id": "pay-1",
            "status": "pending",
            "confirmation": {"confirmation_url": "https://pay.example/confirm"},
        })
        self.assertEqual(YooKassaAPI.get_checkout_url(normalized), "https://pay.example/confirm")

    def test_allowed_notification_ip(self):
        self.assertTrue(YooKassaAPI.is_allowed_notification_ip("185.71.76.1"))
        self.assertFalse(YooKassaAPI.is_allowed_notification_ip("8.8.8.8"))


class YooKassaConfigTests(unittest.TestCase):
    def test_validate_startup_requires_yookassa_credentials(self):
        with patch('config.Config.PAYMENT_PROVIDER', 'yookassa'), \
             patch('config.Config.YOOKASSA_SHOP_ID', ''), \
             patch('config.Config.YOOKASSA_SECRET_KEY', ''), \
             patch('config.Config.YOOKASSA_RETURN_URL', ''), \
             patch('config.Config.TG_CHANNEL', ''):
            errors = __import__('config').Config.validate_startup()
            self.assertTrue(any('YOOKASSA_SHOP_ID' in err for err in errors))
            self.assertTrue(any('YOOKASSA_SECRET_KEY' in err for err in errors))
