import logging
import unittest

from app.log_context import bind_log_context, get_log_context, reset_log_context
from app.logging import ContextFilter


class LoggingContextTests(unittest.TestCase):
    def test_bind_and_reset_log_context(self):
        token = bind_log_context(request_id="req-1", payment_id="pay-1", user_id="42")
        try:
            context = get_log_context()
            self.assertEqual(context["request_id"], "req-1")
            self.assertEqual(context["payment_id"], "pay-1")
            self.assertEqual(context["user_id"], "42")
        finally:
            reset_log_context(token)

        reset_context = get_log_context()
        self.assertEqual(reset_context["request_id"], "-")
        self.assertEqual(reset_context["payment_id"], "-")

    def test_context_filter_populates_log_record(self):
        token = bind_log_context(request_id="req-ctx", ticket_id="10", payment_id="pay-ctx", user_id="77")
        try:
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname=__file__,
                lineno=1,
                msg="message",
                args=(),
                exc_info=None,
            )
            filtered = ContextFilter().filter(record)
            self.assertTrue(filtered)
            self.assertEqual(record.request_id, "req-ctx")
            self.assertEqual(record.ticket_id, "10")
            self.assertEqual(record.payment_id, "pay-ctx")
            self.assertEqual(record.user_id, "77")
        finally:
            reset_log_context(token)


if __name__ == "__main__":
    unittest.main()
