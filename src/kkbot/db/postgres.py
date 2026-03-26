from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import asyncpg

logger = logging.getLogger(__name__)


class PostgresDatabase:
    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 10):
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self.pool: "asyncpg.Pool | None" = None

    async def connect(self) -> None:
        import asyncpg

        self.pool = await asyncpg.create_pool(
            dsn=self._dsn,
            min_size=self._min_size,
            max_size=self._max_size,
        )

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()

    async def execute_script(self, path: Path) -> None:
        sql = path.read_text(encoding="utf-8")
        async with self.pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(sql)

    async def ensure_bootstrap_schema(self, migrations_dir: Path) -> None:
        for path in sorted(migrations_dir.glob("*.sql")):
            await self.execute_script(path)

    async def set_meta(self, key: str, value: dict[str, Any]) -> None:
        payload = json.dumps(value, ensure_ascii=False)
        async with self.pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                """
                INSERT INTO app_meta(key, value, updated_at)
                VALUES($1, $2::jsonb, NOW())
                ON CONFLICT (key)
                DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                """,
                key,
                payload,
            )

    async def get_meta(self, key: str) -> dict[str, Any] | None:
        async with self.pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow("SELECT value FROM app_meta WHERE key = $1", key)
        if row is None:
            return None
        value = row["value"]
        if value is None:
            return None
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {"value": parsed}
        return dict(value)

    async def upsert_bot_user(self, user_id: int, username: str | None, first_name: str | None, last_name: str | None, language_code: str | None, *, is_admin: bool = False) -> None:
        async with self.pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                """
                INSERT INTO bot_users(user_id, username, first_name, last_name, language_code, is_admin)
                VALUES($1, $2, $3, $4, $5, $6)
                ON CONFLICT (user_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    language_code = EXCLUDED.language_code,
                    is_admin = EXCLUDED.is_admin,
                    updated_at = NOW()
                """,
                user_id,
                username,
                first_name,
                last_name,
                language_code,
                is_admin,
            )

    async def ping(self) -> bool:
        try:
            async with self.pool.acquire() as conn:  # type: ignore[union-attr]
                await conn.fetchval("SELECT 1")
            return True
        except Exception:
            logger.exception("PostgreSQL ping failed")
            return False
