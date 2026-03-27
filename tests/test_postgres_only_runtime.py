import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch


class PostgresOnlyRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_database_requires_database_url(self) -> None:
        with patch("db.adaptive_database.Config.DATABASE_URL", ""):
            from db.adaptive_database import Database

            with self.assertRaises(RuntimeError):
                Database("/tmp/users.db")

    async def test_database_runs_postgres_connect_and_legacy_import_without_sqlite_runtime(self) -> None:
        with TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "legacy.db"
            sqlite_path.write_text("placeholder", encoding="utf-8")

            fake_pg = AsyncMock()
            fake_pg.pool = AsyncMock()
            fake_pg.get_meta = AsyncMock(return_value=None)
            fake_pg.set_meta = AsyncMock()

            with (
                patch("db.adaptive_database.Config.DATABASE_URL", "postgresql://test"),
                patch("db.adaptive_database.Config.DATABASE_MIN_POOL", 1),
                patch("db.adaptive_database.Config.DATABASE_MAX_POOL", 2),
                patch("db.adaptive_database.PostgresDatabase", return_value=fake_pg),
                patch("db.adaptive_database.apply_postgres_migrations", new=AsyncMock(return_value=["001_bootstrap.sql"])),
                patch(
                    "db.adaptive_database.import_legacy_sqlite_to_postgres",
                    new=AsyncMock(return_value=type("Report", (), {"total_rows": 5})()),
                ) as import_mock,
            ):
                from db.adaptive_database import Database

                db = Database(str(sqlite_path))
                self.assertFalse(db._sqlite_runtime_enabled)
                self.assertIsNone(db.legacy)
                await db.connect()
                await db.close()

            fake_pg.connect.assert_awaited_once()
            fake_pg.close.assert_awaited_once()
            import_mock.assert_awaited_once()
