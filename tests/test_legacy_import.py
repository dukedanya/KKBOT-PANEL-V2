from pathlib import Path
import tempfile
import unittest

from kkbot.db.legacy_import import ImportReport, _rows, _table_exists


class LegacyImportTests(unittest.TestCase):
    def test_table_exists_and_rows_read_legacy_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "legacy.db"
            import sqlite3

            with sqlite3.connect(path) as conn:
                conn.execute("CREATE TABLE users (user_id INTEGER PRIMARY KEY, name TEXT)")
                conn.execute("INSERT INTO users(user_id, name) VALUES (1, 'denis')")
                conn.commit()

                self.assertTrue(_table_exists(conn, "users"))
                self.assertFalse(_table_exists(conn, "missing_table"))

                rows = _rows(conn, "users")
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["user_id"], 1)
                self.assertEqual(rows[0]["name"], "denis")

    def test_import_report_total_rows(self) -> None:
        report = ImportReport(
            users=2,
            subscriptions=1,
            withdraw_requests=3,
            payment_intents=4,
            payment_status_history=5,
            support_tickets=6,
            support_messages=7,
        )
        self.assertEqual(report.total_rows, 28)
