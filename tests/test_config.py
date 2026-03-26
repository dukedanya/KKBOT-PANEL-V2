import os
import unittest
from unittest.mock import patch

from kkbot.config import load_settings


class ConfigTests(unittest.TestCase):
    @patch.dict(
        os.environ,
        {
            "BOT_TOKEN": "token",
            "DATABASE_URL": "postgresql://test",
            "ADMIN_USER_IDS": "1, 2",
            "AUTO_MIGRATE_LEGACY": "true",
            "LEGACY_IMPORT_BATCH_SIZE": "250",
        },
        clear=False,
    )
    def test_load_settings_parses_values(self) -> None:
        settings = load_settings()
        self.assertEqual(settings.bot_token, "token")
        self.assertEqual(settings.database_url, "postgresql://test")
        self.assertEqual(settings.admin_user_ids, (1, 2))
        self.assertTrue(settings.auto_migrate_legacy)
        self.assertEqual(settings.legacy_import_batch_size, 250)
