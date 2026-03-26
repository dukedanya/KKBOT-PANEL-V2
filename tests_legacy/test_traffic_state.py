import json
import os
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from services.traffic_state import format_grace_until, get_total_traffic_snapshot_for_user


class TrafficStateTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_total_traffic_snapshot_for_user_reads_matching_email(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as file:
            json.dump(
                {
                    "users": [
                        {
                            "email": "123@kakoitovpn",
                            "totalBytes": 10,
                            "quotaBytes": 100,
                            "mode": "grace",
                            "graceUntil": "2026-03-29T08:19:02+00:00",
                            "expired": True,
                            "overLimit": True,
                        }
                    ]
                },
                file,
            )
            path = file.name

        try:
            db = AsyncMock()
            with patch("services.traffic_state.Config.TOTAL_TRAFFIC_STATE_PATH", path), \
                 patch("services.traffic_state.Config.TOTAL_TRAFFIC_STATE_MAX_AGE_SEC", 1800), \
                 patch("services.subscriptions.panel_base_email", new=AsyncMock(return_value="123@kakoitovpn")):
                snapshot = await get_total_traffic_snapshot_for_user(123, db)

            self.assertIsNotNone(snapshot)
            assert snapshot is not None
            self.assertTrue(snapshot.found)
            self.assertEqual(snapshot.mode, "grace")
            self.assertEqual(snapshot.remaining_bytes, 90)
        finally:
            os.unlink(path)

    async def test_get_total_traffic_snapshot_for_user_reads_matching_email_from_url(self):
        payload = {
            "users": [
                {
                    "email": "123@kakoitovpn",
                    "totalBytes": 55,
                    "quotaBytes": 100,
                    "mode": "normal",
                    "graceUntil": None,
                    "expired": False,
                    "overLimit": False,
                }
            ]
        }
        db = AsyncMock()
        with patch("services.traffic_state.Config.TOTAL_TRAFFIC_STATE_URL", "http://example.com/state"), \
             patch("services.traffic_state.Config.TOTAL_TRAFFIC_STATE_PATH", ""), \
             patch("services.traffic_state._load_json_url", return_value=payload), \
             patch("services.subscriptions.panel_base_email", new=AsyncMock(return_value="123@kakoitovpn")):
            snapshot = await get_total_traffic_snapshot_for_user(123, db)

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.source_path, "http://example.com/state")
        self.assertEqual(snapshot.total_bytes, 55)
        self.assertEqual(snapshot.remaining_bytes, 45)

    def test_format_grace_until_humanizes_iso_datetime(self):
        self.assertEqual(format_grace_until("2026-03-29T08:19:02+00:00"), "29.03.2026 08:19")


if __name__ == "__main__":
    unittest.main()
