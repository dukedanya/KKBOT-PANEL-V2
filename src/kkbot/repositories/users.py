from __future__ import annotations

import json
from typing import Any

from kkbot.db.postgres import PostgresDatabase


class UserRepository:
    def __init__(self, db: PostgresDatabase):
        self.db = db

    async def get_user_snapshot_by_username(self, username: str) -> dict | None:
        clean_username = str(username or "").strip().lstrip("@").lower()
        if not clean_username:
            return None
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow(
                """
                SELECT
                    u.user_id,
                    u.username,
                    u.first_name,
                    u.last_name,
                    u.language_code,
                    u.is_admin,
                    (
                        SELECT jsonb_build_object(
                            'status', s.status,
                            'plan_code', s.plan_code,
                            'traffic_limit_bytes', s.traffic_limit_bytes,
                            'traffic_used_bytes', s.traffic_used_bytes,
                            'expires_at', s.expires_at,
                            'meta', s.meta
                        )
                        FROM subscriptions s
                        WHERE s.user_id = u.user_id
                        ORDER BY s.id DESC
                        LIMIT 1
                    ) AS subscription
                FROM bot_users u
                WHERE lower(COALESCE(u.username, '')) = $1
                LIMIT 1
                """,
                clean_username,
            )
        return dict(row) if row else None

    async def get_user_snapshot(self, user_id: int) -> dict | None:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow(
                """
                SELECT
                    u.user_id,
                    u.username,
                    u.first_name,
                    u.last_name,
                    u.language_code,
                    u.is_admin,
                    (
                        SELECT jsonb_build_object(
                            'status', s.status,
                            'plan_code', s.plan_code,
                            'traffic_limit_bytes', s.traffic_limit_bytes,
                            'traffic_used_bytes', s.traffic_used_bytes,
                            'expires_at', s.expires_at
                        )
                        FROM subscriptions s
                        WHERE s.user_id = u.user_id
                        ORDER BY s.id DESC
                        LIMIT 1
                    ) AS subscription
                FROM bot_users u
                WHERE u.user_id = $1
                """,
                user_id,
            )
        return dict(row) if row else None

    async def upsert_basic_user(
        self,
        user_id: int,
        *,
        username: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        language_code: str | None = None,
        is_admin: bool = False,
    ) -> None:
        await self.db.upsert_bot_user(
            user_id,
            username,
            first_name,
            last_name,
            language_code,
            is_admin=is_admin,
        )

    async def upsert_legacy_archive(self, payload: dict[str, Any]) -> None:
        user_id = int(payload.get("user_id") or 0)
        payload_json = json.dumps(payload, ensure_ascii=False, default=str)
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                """
                INSERT INTO legacy_users_archive(user_id, payload)
                VALUES($1, $2::jsonb)
                ON CONFLICT (user_id) DO UPDATE SET
                    payload = EXCLUDED.payload,
                    imported_at = NOW()
                """,
                user_id,
                payload_json,
            )

    async def count_users(self) -> int:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            value = await conn.fetchval("SELECT COUNT(*) FROM bot_users")
        return int(value or 0)

    async def count_banned_users(self) -> int:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            value = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM legacy_users_archive
                WHERE COALESCE((payload->>'banned')::int, 0) = 1
                """
            )
        return int(value or 0)

    async def list_all_legacy_users(self) -> list[dict[str, Any]]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                """
                SELECT payload
                FROM legacy_users_archive
                ORDER BY user_id ASC
                """
            )
        return [dict(row["payload"]) for row in rows]

    async def list_suspicious_referrals(self, *, limit: int = 20) -> list[dict[str, Any]]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                """
                SELECT
                    (payload->>'user_id')::bigint AS user_id,
                    NULLIF(payload->>'ref_by', '')::bigint AS ref_by,
                    payload->>'join_date' AS join_date,
                    payload->>'partner_note' AS partner_note
                FROM legacy_users_archive
                WHERE COALESCE((payload->>'ref_suspicious')::int, 0) = 1
                ORDER BY payload->>'join_date' DESC NULLS LAST
                LIMIT $1
                """,
                int(limit),
            )
        return [dict(row) for row in rows]

    async def get_subscribed_user_ids(self) -> list[int]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                """
                SELECT DISTINCT s.user_id
                FROM subscriptions s
                WHERE s.status IN ('active', 'grace')
                ORDER BY s.user_id
                """
            )
        return [int(row["user_id"]) for row in rows]

    async def list_active_subscribers(self) -> list[dict[str, Any]]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                """
                SELECT
                    u.user_id,
                    u.username,
                    u.first_name,
                    u.last_name,
                    u.language_code,
                    u.is_admin,
                    s.status,
                    s.plan_code,
                    s.expires_at,
                    s.meta
                FROM bot_users u
                JOIN LATERAL (
                    SELECT status, plan_code, expires_at, meta
                    FROM subscriptions
                    WHERE user_id = u.user_id
                    ORDER BY id DESC
                    LIMIT 1
                ) s ON TRUE
                WHERE s.status IN ('active', 'grace')
                ORDER BY u.user_id
                """
            )
        return [dict(row) for row in rows]
