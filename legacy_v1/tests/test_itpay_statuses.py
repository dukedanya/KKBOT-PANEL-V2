import base64
import hashlib
import hmac
import json
import unittest

from services.itpay import ItpayAPI


class ItpayStatusTests(unittest.TestCase):
    def test_status_helpers(self):
        self.assertTrue(ItpayAPI.is_success_status({"status": "paid"}))
        self.assertTrue(ItpayAPI.is_failed_status({"status": "declined"}))
        self.assertFalse(ItpayAPI.is_failed_status({"status": "processing"}))

    def test_verify_webhook_signature(self):
        secret = "secret123"
        payload = {"data": {"payment_id": "abc", "status": "paid"}}
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        timestamp = "1710000000"
        data_str = json.dumps(payload["data"], separators=(",", ":"), ensure_ascii=False)
        digest = hmac.new(secret.encode(), f"{timestamp}.{data_str}".encode(), hashlib.sha256).hexdigest()
        header = f"t={timestamp},v1={digest}"
        self.assertTrue(ItpayAPI.verify_webhook_signature(secret, raw, header))
        self.assertFalse(ItpayAPI.verify_webhook_signature(secret, raw, f"t={timestamp},v1=deadbeef"))


if __name__ == "__main__":
    unittest.main()
