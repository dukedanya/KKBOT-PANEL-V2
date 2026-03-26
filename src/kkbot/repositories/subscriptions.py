from __future__ import annotations

import json
from typing import Any

from kkbot.db.postgres import PostgresDatabase


class SubscriptionRepository:
    def __init__(self, db: PostgresDatabase):
        self.db = db

    async def replace_active_with_new(
        self,
        *,
        user_id: int,
        plan_code: str,
        traffic_limit_bytes: int,
        traffic_used_bytes: int = 0,
        expires_at: str | None = None,
        status: str = "active",
        meta: dict[str, Any] | None = None,
    ) -> int:
        payload = json.dumps(meta or {}, ensure_ascii=False)
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE subscriptions
                    SET status = 'replaced', updated_at = NOW()
                    WHERE user_id = $1 AND status IN ('active', 'grace', 'pending')
                    """,
                    user_id,
                )
                row = await conn.fetchrow(
                    """
                    INSERT INTO subscriptions(
                        user_id, status, plan_code, traffic_limit_bytes, traffic_used_bytes, expires_at, meta
                    )
                    VALUES($1, $2, $3, $4, $5, $6::timestamptz, $7::jsonb)
                    RETURNING id
                    """,
                    user_id,
                    status,
                    plan_code,
                    max(0, int(traffic_limit_bytes)),
                    max(0, int(traffic_used_bytes)),
                    expires_at,
                    payload,
                )
        return int(row["id"])

    async def revoke_active(self, user_id: int, *, reason: str = "") -> int:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            result = await conn.execute(
                """
                UPDATE subscriptions
                SET status = 'revoked',
                    updated_at = NOW(),
                    meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{revoke_reason}', to_jsonb($2::text), true)
                WHERE user_id = $1 AND status IN ('active', 'grace', 'pending')
                """,
                user_id,
                reason,
            )
        return int(result.split()[-1])

    async def get_latest_for_user(self, user_id: int) -> dict[str, Any] | None:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow(
                """
                SELECT id, user_id, status, plan_code, traffic_limit_bytes, traffic_used_bytes, expires_at, meta, created_at, updated_at
                FROM subscriptions
                WHERE user_id = $1
                ORDER BY id DESC
                LIMIT 1
                """,
                user_id,
            )
        return dict(row) if row else None
