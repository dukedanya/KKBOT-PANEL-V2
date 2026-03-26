from __future__ import annotations

from typing import Any

from kkbot.db.postgres import PostgresDatabase


class ReferralRepository:
    def __init__(self, db: PostgresDatabase):
        self.db = db

    async def add_history(self, *, user_id: int, ref_user_id: int, amount: float = 0, bonus_days: int = 0) -> int:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow(
                """
                INSERT INTO ref_history(user_id, ref_user_id, amount, bonus_days)
                VALUES($1, $2, $3, $4)
                RETURNING id
                """,
                user_id,
                ref_user_id,
                float(amount or 0),
                int(bonus_days or 0),
            )
        return int(row["id"])

    async def list_history(self, user_id: int, *, limit: int = 10) -> list[dict[str, Any]]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                """
                SELECT * FROM ref_history
                WHERE user_id = $1
                ORDER BY created_at DESC
                LIMIT $2
                """,
                user_id,
                int(limit),
            )
        return [dict(row) for row in rows]

    async def get_summary(self, user_id: int) -> dict[str, Any]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            refs_row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) AS total_refs,
                    COALESCE(SUM(CASE WHEN COALESCE((payload->>'ref_rewarded')::int, 0) = 1 THEN 1 ELSE 0 END), 0) AS paid_refs
                FROM legacy_users_archive
                WHERE COALESCE((payload->>'ref_by')::bigint, 0) = $1
                """,
                user_id,
            )
            hist_row = await conn.fetchrow(
                """
                SELECT
                    COALESCE(SUM(amount), 0) AS earned_rub,
                    COALESCE(SUM(bonus_days), 0) AS earned_bonus_days
                FROM ref_history
                WHERE user_id = $1
                """,
                user_id,
            )
            withdraw_row = await conn.fetchrow(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END), 0) AS completed_withdraw_rub,
                    COALESCE(SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END), 0) AS pending_withdraw_rub
                FROM withdraw_requests
                WHERE user_id = $1
                """,
                user_id,
            )
        return {
            "total_refs": int((refs_row["total_refs"] if refs_row else 0) or 0),
            "paid_refs": int((refs_row["paid_refs"] if refs_row else 0) or 0),
            "earned_rub": float((hist_row["earned_rub"] if hist_row else 0) or 0),
            "earned_bonus_days": int((hist_row["earned_bonus_days"] if hist_row else 0) or 0),
            "completed_withdraw_rub": float((withdraw_row["completed_withdraw_rub"] if withdraw_row else 0) or 0),
            "pending_withdraw_rub": float((withdraw_row["pending_withdraw_rub"] if withdraw_row else 0) or 0),
        }

    async def list_referrals(self, referrer_id: int) -> list[dict[str, Any]]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                """
                SELECT
                    (payload->>'user_id')::bigint AS user_id,
                    COALESCE((payload->>'ref_rewarded')::int, 0) AS ref_rewarded,
                    payload->>'join_date' AS join_date
                FROM legacy_users_archive
                WHERE COALESCE((payload->>'ref_by')::bigint, 0) = $1
                ORDER BY payload->>'join_date' DESC NULLS LAST
                """,
                referrer_id,
            )
        return [dict(row) for row in rows]

    async def count_recent_referrals(self, referrer_id: int, *, since_hours: int = 24) -> int:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            value = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM legacy_users_archive
                WHERE COALESCE((payload->>'ref_by')::bigint, 0) = $1
                  AND COALESCE((payload->>'join_date')::timestamptz, NOW() - interval '100 years')
                      >= NOW() - make_interval(hours => $2::int)
                """,
                referrer_id,
                int(since_hours),
            )
        return int(value or 0)

    async def list_top_referrers_extended(self, *, limit: int = 10) -> list[dict[str, Any]]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                """
                SELECT
                    COALESCE((u.payload->>'ref_by')::bigint, 0) AS ref_by,
                    COUNT(*) FILTER (WHERE COALESCE((u.payload->>'ref_rewarded')::int, 0) = 1) AS paid_count,
                    COALESCE(SUM(rh.amount), 0) AS earned_rub,
                    COALESCE(SUM(rh.bonus_days), 0) AS earned_bonus_days
                FROM legacy_users_archive u
                LEFT JOIN ref_history rh
                  ON rh.user_id = COALESCE((u.payload->>'ref_by')::bigint, 0)
                 AND rh.ref_user_id = COALESCE((u.payload->>'user_id')::bigint, 0)
                WHERE COALESCE((u.payload->>'ref_by')::bigint, 0) != 0
                GROUP BY ref_by
                ORDER BY paid_count DESC, earned_rub DESC
                LIMIT $1
                """,
                int(limit),
            )
        return [dict(row) for row in rows]
