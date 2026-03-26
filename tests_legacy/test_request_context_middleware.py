import unittest
from types import SimpleNamespace

from middlewares.request_context import extract_log_context


class RequestContextMiddlewareTests(unittest.TestCase):
    def test_extract_log_context_from_callback_support_ticket(self):
        event = SimpleNamespace(
            id="cb-1",
            data="support:view:123",
            from_user=SimpleNamespace(id=77),
        )
        context = extract_log_context(event)
        self.assertEqual(context.get("request_id"), "cb:cb-1")
        self.assertEqual(context.get("user_id"), "77")
        self.assertEqual(context.get("ticket_id"), "123")

    def test_extract_log_context_from_callback_payment(self):
        event = SimpleNamespace(
            id="cb-2",
            data="paydiag_refund:pay-abc123",
            from_user=SimpleNamespace(id=10),
        )
        context = extract_log_context(event)
        self.assertEqual(context.get("request_id"), "cb:cb-2")
        self.assertEqual(context.get("payment_id"), "pay-abc123")


if __name__ == "__main__":
    unittest.main()
