import logging
import unittest

from app.logging import HumanConsoleFormatter


class LoggingFormatterTests(unittest.TestCase):
    def test_human_console_formatter_renders_compact_bilingual_line(self) -> None:
        formatter = HumanConsoleFormatter()
        record = logging.LogRecord(
            name="services.panel",
            level=logging.INFO,
            pathname=__file__,
            lineno=10,
            msg="Успешная аутентификация в панели 3X-UI",
            args=(),
            exc_info=None,
        )
        record.user_id = "42"
        record.request_id = "req-1"
        record.ticket_id = "-"
        record.payment_id = "-"

        output = formatter.format(record)

        self.assertIn("INFO", output)
        self.assertNotIn("INFO/ИНФО", output)
        self.assertIn("svc.panel", output)
        self.assertIn("Успешная аутентификация в панели 3X-UI", output)
        self.assertIn("user=42", output)
        self.assertIn("req=req-1", output)
        self.assertNotIn("ticket=", output)
        self.assertNotIn("pay=", output)
