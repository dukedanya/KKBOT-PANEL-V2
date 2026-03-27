from __future__ import annotations

from typing import Any

from kkbot.db.postgres import PostgresDatabase


class MetaRepository:
    def __init__(self, db: PostgresDatabase):
        self.db = db

    async def get_legacy_import_status(self) -> dict | None:
        return await self.db.get_meta("legacy_sqlite_import")

    async def get_legacy_setting(self, key: str) -> str | None:
        payload = await self.db.get_meta(f"legacy_setting:{key}")
        if not payload:
            return None
        value = payload.get("value")
        return None if value is None else str(value)

    async def set_legacy_setting(self, key: str, value: str) -> None:
        await self.db.set_meta(f"legacy_setting:{key}", {"value": value})

    async def set_legacy_payload(self, namespace: str, key: str, payload: dict[str, Any]) -> None:
        await self.db.set_meta(f"{namespace}:{key}", payload)

    async def get_legacy_payload(self, namespace: str, key: str) -> dict[str, Any] | None:
        return await self.db.get_meta(f"{namespace}:{key}")

    async def delete_legacy_payload(self, namespace: str, key: str) -> None:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                """
                DELETE FROM app_meta
                WHERE key = $1
                """,
                f"{namespace}:{key}",
            )

    async def list_legacy_settings(self, prefix: str) -> list[tuple[str, dict[str, Any]]]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                """
                SELECT key, value
                FROM app_meta
                WHERE key LIKE $1
                ORDER BY updated_at DESC
                """,
                f"legacy_setting:{prefix}%",
            )
        return [(str(row["key"]), dict(row["value"])) for row in rows]
