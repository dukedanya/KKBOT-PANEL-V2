import unittest
from unittest.mock import AsyncMock, MagicMock, patch


class V2BootstrapSmokeTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_bootstrap_starts_and_closes_cleanly(self) -> None:
        from kkbot import bootstrap

        fake_db = MagicMock()
        fake_db.connect = AsyncMock()
        fake_db.close = AsyncMock()
        fake_db.set_meta = AsyncMock()
        fake_db.get_meta = AsyncMock(return_value={"completed": True})
        fake_db.ping = AsyncMock(return_value=True)

        fake_bot = MagicMock()
        fake_bot.session = MagicMock()
        fake_bot.session.close = AsyncMock()

        fake_dispatcher = MagicMock()
        fake_dispatcher.include_router = MagicMock()
        fake_dispatcher.start_polling = AsyncMock()

        fake_me = MagicMock()
        fake_me.username = "kkbot_test"
        fake_bot.get_me = AsyncMock(return_value=fake_me)

        settings = MagicMock()
        settings.bot_token = "token"
        settings.database_url = "postgresql://test"
        settings.database_min_pool = 1
        settings.database_max_pool = 2
        settings.log_level = "INFO"
        settings.auto_migrate_legacy = False
        settings.legacy_sqlite_path = ""
        settings.legacy_import_batch_size = 100
        settings.admin_user_ids = (1,)

        with (
            patch("kkbot.bootstrap.load_settings", return_value=settings),
            patch("kkbot.bootstrap.PostgresDatabase", return_value=fake_db),
            patch("kkbot.bootstrap.apply_postgres_migrations", new=AsyncMock(return_value=["001_bootstrap.sql"])),
            patch("kkbot.bootstrap.Bot", return_value=fake_bot),
            patch("kkbot.bootstrap.Dispatcher", return_value=fake_dispatcher),
        ):
            await bootstrap.run()

        fake_db.connect.assert_awaited()
        fake_dispatcher.start_polling.assert_awaited()
        fake_bot.session.close.assert_awaited()
        fake_db.close.assert_awaited()
