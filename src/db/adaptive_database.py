from __future__ import annotations

import json
import logging
import secrets
import string
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config import Config
from kkbot.db.legacy_import import import_legacy_sqlite_to_postgres
from kkbot.db.migrations import apply_postgres_migrations
from kkbot.db.postgres import PostgresDatabase
from kkbot.repositories.meta import MetaRepository
from kkbot.repositories.operations import OperationsRepository
from kkbot.repositories.payments import PaymentRepository
from kkbot.repositories.referrals import ReferralRepository
from kkbot.repositories.subscriptions import SubscriptionRepository
from kkbot.repositories.users import UserRepository

logger = logging.getLogger(__name__)


def generate_ref_code() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(8))


def _coerce_payload_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return dict(parsed)
    return {}


def _parse_utc_iso_dt(raw: Any) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


class Database:
    def __init__(self, db_path: str):
        self.legacy = None
        self._legacy_db_path = db_path
        self._sqlite_runtime_enabled = False
        self.postgres: PostgresDatabase | None = None
        if not Config.DATABASE_URL:
            raise RuntimeError("DATABASE_URL is required: SQLite runtime was removed from KKBOT PANEL V2.0")
        self.postgres = PostgresDatabase(
            Config.DATABASE_URL,
            min_size=Config.DATABASE_MIN_POOL,
            max_size=Config.DATABASE_MAX_POOL,
        )

    async def connect(self) -> None:
        if self.postgres is not None:
            await self.postgres.connect()
            migrations_dir = Path(__file__).resolve().parents[2] / "migrations" / "postgres"
            await apply_postgres_migrations(self.postgres, migrations_dir)
            await self._run_legacy_import_if_needed()

    async def close(self) -> None:
        if self.postgres is not None:
            await self.postgres.close()

    def _user_repo(self) -> UserRepository | None:
        if self.postgres is None:
            return None
        return UserRepository(self.postgres)

    def _meta_repo(self) -> MetaRepository | None:
        if self.postgres is None:
            return None
        return MetaRepository(self.postgres)

    def _payment_repo(self) -> PaymentRepository | None:
        if self.postgres is None:
            return None
        return PaymentRepository(self.postgres)

    def _referral_repo(self) -> ReferralRepository | None:
        if self.postgres is None:
            return None
        return ReferralRepository(self.postgres)

    def _subscription_repo(self) -> SubscriptionRepository | None:
        if self.postgres is None:
            return None
        return SubscriptionRepository(self.postgres)

    def _operations_repo(self) -> OperationsRepository | None:
        if self.postgres is None:
            return None
        return OperationsRepository(self.postgres)

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    def _default_user_payload(self, user_id: int) -> dict[str, Any]:
        return {
            "user_id": int(user_id),
            "join_date": self._now_iso(),
            "banned": 0,
            "ban_reason": "",
            "ref_code": "",
            "ref_by": 0,
            "ref_rewarded": 0,
            "bonus_days_pending": 0,
            "trial_used": 0,
            "trial_declined": 0,
            "has_subscription": 0,
            "plan_text": "",
            "ip_limit": 0,
            "traffic_gb": 0,
            "vpn_url": "",
            "balance": 0.0,
            "partner_percent_level1": None,
            "partner_percent_level2": None,
            "partner_percent_level3": None,
            "partner_status": "standard",
            "partner_note": "",
            "ref_suspicious": 0,
            "username": "",
            "first_name": "",
            "last_name": "",
            "language_code": "",
            "notified_3d": 0,
            "notified_1d": 0,
            "notified_1h": 0,
            "frozen_until": None,
        }

    async def _store_user_payload(self, payload: dict[str, Any]) -> None:
        meta = self._meta_repo()
        repo = self._user_repo()
        if meta is None or repo is None:
            return
        user_id = int(payload.get("user_id") or 0)
        await meta.set_legacy_payload("legacy_user", str(user_id), payload)
        await repo.upsert_legacy_archive(payload)
        await repo.upsert_basic_user(
            user_id,
            username=str(payload.get("username") or "") or None,
            first_name=str(payload.get("first_name") or "") or None,
            last_name=str(payload.get("last_name") or "") or None,
            language_code=str(payload.get("language_code") or "") or None,
            is_admin=bool(payload.get("is_admin", False)),
        )

    async def _mutate_user_payload(self, user_id: int, **updates: Any) -> dict[str, Any] | None:
        payload = await self.get_user(user_id)
        if payload is None:
            payload = self._default_user_payload(user_id)
        payload = dict(payload)
        payload.update(updates)
        await self._store_user_payload(payload)
        return payload

    async def _sync_legacy_user_payload(self, user_id: int) -> None:
        meta = self._meta_repo()
        repo = self._user_repo()
        if meta is None or repo is None:
            return
        if not self._sqlite_runtime_enabled:
            payload = await meta.get_legacy_payload("legacy_user", str(user_id))
        else:
            payload = await self.legacy.get_user(user_id)
        if payload is not None:
            await meta.set_legacy_payload("legacy_user", str(user_id), payload)
            await repo.upsert_legacy_archive(payload)

    async def _list_namespace_payloads(self, namespace: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        if self.postgres is None or self.postgres.pool is None:
            return []
        query = """
            SELECT value
            FROM app_meta
            WHERE key LIKE $1
            ORDER BY updated_at DESC
        """
        params: list[Any] = [f"{namespace}:%"]
        if limit is not None:
            params.append(int(limit))
            query += f" LIMIT ${len(params)}"
        async with self.postgres.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        result: list[dict[str, Any]] = []
        for row in rows:
            value = row["value"]
            payload = _coerce_payload_dict(value)
            if payload:
                result.append(payload)
        return result

    async def _subscription_row_to_user_payload(self, user_id: int, row: dict[str, Any] | None) -> dict[str, Any]:
        user = await self.get_user(user_id) or self._default_user_payload(user_id)
        if not row:
            user.update({
                "has_subscription": 0,
                "plan_text": "",
                "vpn_url": "",
                "traffic_gb": 0,
            })
            return user
        meta = row.get("meta") or {}
        if not isinstance(meta, dict):
            meta = {}
        user.update({
            "has_subscription": 1 if row.get("status") in {"active", "grace", "pending"} else 0,
            "plan_text": row.get("plan_code") or "",
            "vpn_url": str(meta.get("vpn_url") or user.get("vpn_url") or ""),
            "traffic_gb": int((row.get("traffic_limit_bytes") or 0) // (1024 * 1024 * 1024)),
            "ip_limit": int(meta.get("legacy_ip_limit") or user.get("ip_limit") or 0),
        })
        return user

    async def _run_legacy_import_if_needed(self) -> None:
        postgres = self.postgres
        if postgres is None or postgres.pool is None:
            return
        sqlite_path = Path(self._legacy_db_path)
        if not sqlite_path.exists():
            return
        status = await postgres.get_meta("legacy_sqlite_import")
        if status and status.get("completed"):
            return
        report = await import_legacy_sqlite_to_postgres(sqlite_path, postgres.pool)
        await postgres.set_meta(
            "legacy_sqlite_import",
            {
                "completed": True,
                "source": str(sqlite_path),
                "rows": report.total_rows,
            },
        )
        logger.info(
            "Legacy SQLite imported into PostgreSQL: %s rows from %s",
            report.total_rows,
            sqlite_path,
        )

    async def add_user(self, user_id: int) -> bool:
        if self._sqlite_runtime_enabled:
            created = await self.legacy.add_user(user_id)
        else:
            existing = await self.get_user(user_id)
            created = existing is None
            if created:
                await self._store_user_payload(self._default_user_payload(user_id))
        repo = self._user_repo()
        if repo is not None:
            await repo.upsert_basic_user(user_id)
            if self._sqlite_runtime_enabled:
                await self._sync_legacy_user_payload(user_id)
        return created

    async def get_user(self, user_id: int) -> dict[str, Any] | None:
        if self._sqlite_runtime_enabled:
            payload = await self.legacy.get_user(user_id)
            if payload is not None:
                return payload
        repo = self._user_repo()
        if repo is None:
            return None
        snapshot = await repo.get_user_snapshot(user_id)
        if not snapshot:
            return None
        meta = self._meta_repo()
        stored: dict[str, Any] = {}
        if meta is not None:
            legacy_payload = await meta.get_legacy_payload("legacy_user", str(user_id))
            if isinstance(legacy_payload, dict):
                stored = dict(legacy_payload)
        raw_subscription = snapshot.get("subscription")
        subscription = raw_subscription if isinstance(raw_subscription, dict) else {}
        subscription_meta = subscription.get("meta") if isinstance(subscription.get("meta"), dict) else {}
        result = dict(stored)
        result.update(
            {
                "user_id": int(snapshot["user_id"]),
                "username": snapshot.get("username") or result.get("username") or "",
                "first_name": snapshot.get("first_name") or result.get("first_name") or "",
                "last_name": snapshot.get("last_name") or result.get("last_name") or "",
                "language_code": snapshot.get("language_code") or result.get("language_code") or "",
                "has_subscription": 1 if subscription and subscription.get("status") in {"active", "grace"} else 0,
                "banned": int(result.get("banned") or 0),
                "plan_text": subscription.get("plan_code") or result.get("plan_text") or "",
                "vpn_url": str(subscription_meta.get("vpn_url") or result.get("vpn_url") or ""),
            }
        )
        return result

    async def get_user_by_username(self, username: str) -> dict[str, Any] | None:
        clean_username = str(username or "").strip().lstrip("@")
        if not clean_username:
            return None
        repo = self._user_repo()
        if repo is None:
            return None
        snapshot = await repo.get_user_snapshot_by_username(clean_username)
        if not snapshot:
            return None
        user_id = int(snapshot["user_id"])
        meta = self._meta_repo()
        stored: dict[str, Any] = {}
        if meta is not None:
            legacy_payload = await meta.get_legacy_payload("legacy_user", str(user_id))
            if isinstance(legacy_payload, dict):
                stored = dict(legacy_payload)
        raw_subscription = snapshot.get("subscription")
        subscription = raw_subscription if isinstance(raw_subscription, dict) else {}
        subscription_meta = subscription.get("meta") if isinstance(subscription.get("meta"), dict) else {}
        result = dict(stored)
        result.update(
            {
                "user_id": user_id,
                "username": snapshot.get("username") or result.get("username") or "",
                "first_name": snapshot.get("first_name") or result.get("first_name") or "",
                "last_name": snapshot.get("last_name") or result.get("last_name") or "",
                "language_code": snapshot.get("language_code") or result.get("language_code") or "",
                "has_subscription": 1 if subscription and subscription.get("status") in {"active", "grace"} else 0,
                "banned": int(result.get("banned") or 0),
                "plan_text": subscription.get("plan_code") or result.get("plan_text") or "",
                "vpn_url": str(subscription_meta.get("vpn_url") or result.get("vpn_url") or ""),
            }
        )
        return result

    async def update_user(self, user_id: int, **kwargs) -> bool:
        if self._sqlite_runtime_enabled:
            updated = await self.legacy.update_user(user_id, **kwargs)
        else:
            payload = await self._mutate_user_payload(user_id, **kwargs)
            updated = payload is not None
        repo = self._user_repo()
        if repo is not None:
            await repo.upsert_basic_user(
                user_id,
                username=kwargs.get("username"),
                first_name=kwargs.get("first_name"),
                last_name=kwargs.get("last_name"),
                language_code=kwargs.get("language_code"),
                is_admin=bool(kwargs.get("is_admin", False)),
            )
            if self._sqlite_runtime_enabled:
                await self._sync_legacy_user_payload(user_id)
        return updated

    async def get_total_users(self) -> int:
        repo = self._user_repo()
        if repo is not None:
            return await repo.count_users()
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_total_users()
        return 0

    async def get_all_users(self) -> list[dict[str, Any]]:
        repo = self._user_repo()
        if repo is not None:
            rows = await repo.list_all_legacy_users()
            if rows:
                return rows
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_all_users()
        return []

    async def get_banned_users_count(self) -> int:
        repo = self._user_repo()
        if repo is not None:
            return await repo.count_banned_users()
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_banned_users_count()
        return 0

    async def get_subscribed_user_ids(self) -> list[int]:
        repo = self._user_repo()
        if repo is not None:
            return await repo.get_subscribed_user_ids()
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_subscribed_user_ids()
        return []

    async def get_all_subscribers(self) -> list[dict[str, Any]]:
        repo = self._user_repo()
        if repo is None:
            if self._sqlite_runtime_enabled:
                return await self.legacy.get_all_subscribers()
            return []
        rows = await repo.list_active_subscribers()
        if not rows:
            if self._sqlite_runtime_enabled:
                return await self.legacy.get_all_subscribers()
            return []
        result: list[dict[str, Any]] = []
        meta = self._meta_repo()
        for row in rows:
            user_id = int(row["user_id"])
            if meta is not None:
                stored = await meta.get_legacy_payload("legacy_user", str(user_id))
                if stored:
                    result.append(stored)
                    continue
            subscription_meta = row.get("meta") or {}
            result.append(
                {
                    "user_id": user_id,
                    "username": row.get("username") or "",
                    "first_name": row.get("first_name") or "",
                    "last_name": row.get("last_name") or "",
                    "language_code": row.get("language_code") or "",
                    "has_subscription": 1,
                    "banned": 0,
                    "plan_text": row.get("plan_code") or "",
                    "vpn_url": (subscription_meta.get("vpn_url") if isinstance(subscription_meta, dict) else "") or "",
                }
            )
        return result

    async def get_setting(self, key: str, default: str | None = None) -> str | None:
        meta = self._meta_repo()
        if meta is not None:
            value = await meta.get_legacy_setting(key)
            if value is not None:
                return value
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_setting(key, default)
        return default

    async def set_setting(self, key: str, value: str) -> bool:
        ok = True
        if self._sqlite_runtime_enabled:
            ok = await self.legacy.set_setting(key, value)
        meta = self._meta_repo()
        if meta is not None:
            await meta.set_legacy_setting(key, value)
        return ok

    async def get_support_restriction(self, user_id: int) -> dict[str, Any]:
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_support_restriction(user_id)
        expires_at = str(await self.get_setting(f"support:blocked_until:{int(user_id)}", "") or "").strip()
        reason = str(await self.get_setting(f"support:block_reason:{int(user_id)}", "") or "").strip()
        return {
            "active": bool(expires_at),
            "expires_at": expires_at,
            "reason": reason,
        }

    async def set_support_restriction(self, user_id: int, expires_at: str, reason: str = "") -> bool:
        await self.set_setting(f"support:blocked_until:{int(user_id)}", str(expires_at or "").strip())
        await self.set_setting(f"support:block_reason:{int(user_id)}", str(reason or "").strip())
        return True

    async def clear_support_restriction(self, user_id: int) -> bool:
        await self.set_setting(f"support:blocked_until:{int(user_id)}", "")
        await self.set_setting(f"support:block_reason:{int(user_id)}", "")
        return True

    async def get_balance(self, user_id: int) -> float:
        user = await self.get_user(user_id)
        if not user:
            return 0.0
        return float(user.get("balance") or 0.0)

    async def add_referral_balance_adjustment(self, user_id: int, admin_user_id: int, amount: float, reason: str = "") -> bool:
        credited = await self.add_balance(user_id, amount)
        if not credited:
            return False
        await self.add_admin_user_action(
            user_id=user_id,
            admin_user_id=admin_user_id,
            action="balance_adjustment",
            details=f"{float(amount or 0):.2f} RUB {reason}".strip(),
        )
        return True

    async def get_referral_balance_adjustments(self, user_id: int, limit: int = 10) -> list[dict[str, Any]]:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT created_at, details, admin_user_id
                    FROM admin_user_actions
                    WHERE user_id = $1 AND action = 'balance_adjustment'
                    ORDER BY created_at DESC, id DESC
                    LIMIT $2
                    """,
                    int(user_id),
                    int(limit),
                )
            result: list[dict[str, Any]] = []
            for row in rows:
                details = str(row["details"] or "").strip()
                amount_text, _, reason = details.partition(" ")
                try:
                    amount = float(amount_text)
                except ValueError:
                    amount = 0.0
                result.append(
                    {
                        "created_at": row["created_at"],
                        "amount": amount,
                        "reason": reason.strip(),
                        "admin_user_id": int(row["admin_user_id"] or 0),
                    }
                )
            return result
        return []

    async def get_user_by_ref_code(self, ref_code: str) -> dict[str, Any] | None:
        normalized = str(ref_code or "").strip().upper()
        if not normalized:
            return None
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_user_by_ref_code(normalized)
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT payload
                    FROM legacy_users_archive
                    WHERE UPPER(COALESCE(payload->>'ref_code', '')) = $1
                    LIMIT 1
                    """,
                    normalized,
                )
            if row and row["payload"]:
                payload = _coerce_payload_dict(row["payload"])
                if payload:
                    return payload
        return None

    async def get_daily_user_acquisition_report(self, *, days_ago: int = 0) -> dict[str, Any]:
        days_ago = max(0, int(days_ago))
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT
                        (CURRENT_DATE - ($1::int || ' days')::interval)::date AS report_date,
                        COUNT(*) AS new_users,
                        COUNT(*) FILTER (WHERE COALESCE((payload->>'ref_by')::bigint, 0) > 0) AS referred_new_users,
                        COUNT(*) FILTER (WHERE COALESCE((payload->>'trial_used')::int, 0) = 1) AS trial_started_new_users
                    FROM legacy_users_archive
                    WHERE DATE(COALESCE(NULLIF(payload->>'join_date', '')::timestamptz, imported_at)) =
                          (CURRENT_DATE - ($1::int || ' days')::interval)::date
                    """,
                    days_ago,
                )
            return {
                "report_date": str(row["report_date"] or ""),
                "new_users": int(row["new_users"] or 0),
                "referred_new_users": int(row["referred_new_users"] or 0),
                "trial_started_new_users": int(row["trial_started_new_users"] or 0),
            }
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_daily_user_acquisition_report(days_ago=days_ago)
        return {"report_date": "", "new_users": 0, "referred_new_users": 0, "trial_started_new_users": 0}

    async def get_period_user_acquisition_report(self, *, days: int, end_days_ago: int = 0) -> dict[str, Any]:
        days = max(1, int(days))
        end_days_ago = max(0, int(end_days_ago))
        start_days_ago = end_days_ago + days - 1
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT
                        (CURRENT_DATE - ($1::int || ' days')::interval)::date AS start_date,
                        (CURRENT_DATE - ($2::int || ' days')::interval)::date AS end_date,
                        COUNT(*) AS new_users,
                        COUNT(*) FILTER (WHERE COALESCE((payload->>'ref_by')::bigint, 0) > 0) AS referred_new_users,
                        COUNT(*) FILTER (WHERE COALESCE((payload->>'trial_used')::int, 0) = 1) AS trial_started_new_users
                    FROM legacy_users_archive
                    WHERE DATE(COALESCE(NULLIF(payload->>'join_date', '')::timestamptz, imported_at))
                          BETWEEN (CURRENT_DATE - ($1::int || ' days')::interval)::date
                              AND (CURRENT_DATE - ($2::int || ' days')::interval)::date
                    """,
                    start_days_ago,
                    end_days_ago,
                )
            return {
                "start_date": str(row["start_date"] or ""),
                "end_date": str(row["end_date"] or ""),
                "days": days,
                "new_users": int(row["new_users"] or 0),
                "referred_new_users": int(row["referred_new_users"] or 0),
                "trial_started_new_users": int(row["trial_started_new_users"] or 0),
            }
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_period_user_acquisition_report(days=days, end_days_ago=end_days_ago)
        return {"start_date": "", "end_date": "", "days": days, "new_users": 0, "referred_new_users": 0, "trial_started_new_users": 0}

    async def get_daily_subscription_sales_report(self, *, days_ago: int = 0) -> dict[str, Any]:
        return await self.get_period_subscription_sales_report(days=1, end_days_ago=max(0, int(days_ago)))

    async def get_period_subscription_sales_report(self, *, days: int, end_days_ago: int = 0) -> dict[str, Any]:
        days = max(1, int(days))
        end_days_ago = max(0, int(end_days_ago))
        start_days_ago = end_days_ago + days - 1
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                accepted_row = await conn.fetchrow(
                    """
                    SELECT
                        COUNT(DISTINCT h.payment_id) FILTER (WHERE p.provider <> 'balance') AS subscriptions_bought,
                        COUNT(DISTINCT h.payment_id) FILTER (WHERE p.provider = 'balance') AS internal_balance_subscriptions,
                        COALESCE(SUM(p.amount) FILTER (WHERE p.provider <> 'balance'), 0) AS gross_revenue,
                        COALESCE(SUM(p.amount) FILTER (WHERE p.provider = 'balance'), 0) AS internal_balance_spent
                    FROM payment_status_history h
                    JOIN payment_intents p ON p.payment_id = h.payment_id
                    WHERE h.to_status = 'accepted'
                      AND DATE(h.created_at) BETWEEN
                          (CURRENT_DATE - ($1::int || ' days')::interval)::date AND
                          (CURRENT_DATE - ($2::int || ' days')::interval)::date
                    """,
                    start_days_ago,
                    end_days_ago,
                )
                refunded_row = await conn.fetchrow(
                    """
                    SELECT COALESCE(SUM(p.amount) FILTER (WHERE p.provider <> 'balance'), 0) AS refunded_revenue
                    FROM payment_status_history h
                    JOIN payment_intents p ON p.payment_id = h.payment_id
                    WHERE h.to_status = 'refunded'
                      AND DATE(h.created_at) BETWEEN
                          (CURRENT_DATE - ($1::int || ' days')::interval)::date AND
                          (CURRENT_DATE - ($2::int || ' days')::interval)::date
                    """,
                    start_days_ago,
                    end_days_ago,
                )
                referral_row = await conn.fetchrow(
                    """
                    SELECT COALESCE(SUM(amount), 0) AS referral_cost
                    FROM ref_history
                    WHERE DATE(created_at) BETWEEN
                        (CURRENT_DATE - ($1::int || ' days')::interval)::date AND
                        (CURRENT_DATE - ($2::int || ' days')::interval)::date
                    """,
                    start_days_ago,
                    end_days_ago,
                )
                admin_balance_row = await conn.fetchrow(
                    """
                    SELECT COALESCE(
                        SUM(
                            CASE
                                WHEN split_part(details, ' ', 1) ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'
                                     AND split_part(details, ' ', 1)::numeric > 0
                                THEN split_part(details, ' ', 1)::numeric
                                ELSE 0
                            END
                        ),
                        0
                    ) AS admin_balance_issued
                    FROM admin_user_actions
                    WHERE action = 'balance_adjustment'
                      AND DATE(created_at) BETWEEN
                          (CURRENT_DATE - ($1::int || ' days')::interval)::date AND
                          (CURRENT_DATE - ($2::int || ' days')::interval)::date
                    """,
                    start_days_ago,
                    end_days_ago,
                )
            subscriptions_bought = int((accepted_row["subscriptions_bought"] if accepted_row else 0) or 0)
            internal_balance_subscriptions = int((accepted_row["internal_balance_subscriptions"] if accepted_row else 0) or 0)
            gross_revenue = float((accepted_row["gross_revenue"] if accepted_row else 0.0) or 0.0)
            internal_balance_spent = float((accepted_row["internal_balance_spent"] if accepted_row else 0.0) or 0.0)
            refunded_revenue = float((refunded_row["refunded_revenue"] if refunded_row else 0.0) or 0.0)
            referral_cost = float((referral_row["referral_cost"] if referral_row else 0.0) or 0.0)
            admin_balance_issued = float((admin_balance_row["admin_balance_issued"] if admin_balance_row else 0.0) or 0.0)
            net_revenue = gross_revenue - refunded_revenue
            return {
                "start_date": (datetime.now(timezone.utc) - timedelta(days=start_days_ago)).date().isoformat(),
                "end_date": (datetime.now(timezone.utc) - timedelta(days=end_days_ago)).date().isoformat(),
                "days": days,
                "subscriptions_bought": subscriptions_bought,
                "internal_balance_subscriptions": internal_balance_subscriptions,
                "gross_revenue": gross_revenue,
                "internal_balance_spent": internal_balance_spent,
                "refunded_revenue": refunded_revenue,
                "net_revenue": net_revenue,
                "referral_cost": referral_cost,
                "admin_balance_issued": admin_balance_issued,
                "estimated_profit": net_revenue - referral_cost,
            }
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_period_subscription_sales_report(days=days, end_days_ago=end_days_ago)
        return {"start_date": "", "end_date": "", "days": days, "subscriptions_bought": 0, "internal_balance_subscriptions": 0, "gross_revenue": 0.0, "internal_balance_spent": 0.0, "refunded_revenue": 0.0, "net_revenue": 0.0, "referral_cost": 0.0, "admin_balance_issued": 0.0, "estimated_profit": 0.0}

    async def get_total_revenue_summary(self) -> dict[str, Any]:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                payment_row = await conn.fetchrow(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE status = ANY($1::text[]) AND provider <> 'balance') AS accepted_payments,
                        COUNT(*) FILTER (WHERE status = ANY($1::text[]) AND provider = 'balance') AS internal_balance_payments,
                        COALESCE(SUM(CASE WHEN status = ANY($1::text[]) AND provider <> 'balance' THEN amount ELSE 0 END), 0) AS gross_revenue,
                        COALESCE(SUM(CASE WHEN status = ANY($1::text[]) AND provider = 'balance' THEN amount ELSE 0 END), 0) AS internal_balance_spent,
                        COALESCE(SUM(CASE WHEN status = 'refunded' AND provider <> 'balance' THEN amount ELSE 0 END), 0) AS refunded_revenue
                    FROM payment_intents
                    """,
                    ["accepted", "refunded"],
                )
                referral_row = await conn.fetchrow("SELECT COALESCE(SUM(amount), 0) AS referral_cost FROM ref_history")
                admin_balance_row = await conn.fetchrow(
                    """
                    SELECT COALESCE(
                        SUM(
                            CASE
                                WHEN split_part(details, ' ', 1) ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'
                                     AND split_part(details, ' ', 1)::numeric > 0
                                THEN split_part(details, ' ', 1)::numeric
                                ELSE 0
                            END
                        ),
                        0
                    ) AS admin_balance_issued
                    FROM admin_user_actions
                    WHERE action = 'balance_adjustment'
                    """
                )
            gross_revenue = float((payment_row["gross_revenue"] if payment_row else 0.0) or 0.0)
            internal_balance_spent = float((payment_row["internal_balance_spent"] if payment_row else 0.0) or 0.0)
            refunded_revenue = float((payment_row["refunded_revenue"] if payment_row else 0.0) or 0.0)
            referral_cost = float((referral_row["referral_cost"] if referral_row else 0.0) or 0.0)
            admin_balance_issued = float((admin_balance_row["admin_balance_issued"] if admin_balance_row else 0.0) or 0.0)
            net_revenue = gross_revenue - refunded_revenue
            return {
                "accepted_payments": int((payment_row["accepted_payments"] if payment_row else 0) or 0),
                "internal_balance_payments": int((payment_row["internal_balance_payments"] if payment_row else 0) or 0),
                "gross_revenue": gross_revenue,
                "internal_balance_spent": internal_balance_spent,
                "refunded_revenue": refunded_revenue,
                "net_revenue": net_revenue,
                "referral_cost": referral_cost,
                "admin_balance_issued": admin_balance_issued,
                "estimated_profit": net_revenue - referral_cost,
            }
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_total_revenue_summary()
        return {"accepted_payments": 0, "internal_balance_payments": 0, "gross_revenue": 0.0, "internal_balance_spent": 0.0, "refunded_revenue": 0.0, "net_revenue": 0.0, "referral_cost": 0.0, "admin_balance_issued": 0.0, "estimated_profit": 0.0}

    async def get_user_card(self, user_id: int) -> dict[str, Any]:
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_user_card(user_id)
        user = await self.get_user(user_id)
        if not user:
            return {}
        return {
            "user": user,
            "referral_summary": await self.get_referral_summary(user_id),
            "partner_settings": await self.get_partner_settings(user_id),
            "support_tickets": await self.list_user_support_tickets(user_id, limit=5),
            "support_restriction": await self.get_support_restriction(user_id),
            "payments": (await self.get_pending_payments_by_user(user_id))[:5],
            "withdraws": (await self.get_withdraw_requests_by_user(user_id, limit=5))[:5],
            "adjustments": (await self.get_referral_balance_adjustments(user_id, limit=5))[:5],
        }

    async def get_recent_user_ids(self, limit: int = 10) -> list[int]:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT user_id
                    FROM bot_users
                    ORDER BY created_at DESC, user_id DESC
                    LIMIT $1
                    """,
                    int(limit),
                )
            return [int(row["user_id"]) for row in rows]
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_recent_user_ids(limit=limit)
        return []

    async def get_user_timeline(self, user_id: int, limit: int = 25) -> list[dict[str, Any]]:
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_user_timeline(user_id, limit=limit)
        items: list[dict[str, Any]] = []
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                admin_rows = await conn.fetch(
                    "SELECT * FROM admin_user_actions WHERE user_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2",
                    int(user_id),
                    int(limit),
                )
            for row in admin_rows:
                item = dict(row)
                items.append({
                    "created_at": item.get("created_at"),
                    "kind": "admin_action",
                    "title": item.get("action") or "admin_action",
                    "details": f"admin={item.get('admin_user_id')} {item.get('details') or ''}".strip(),
                })
        for row in await self.list_user_support_tickets(user_id, limit=10):
            items.append({
                "created_at": row.get("updated_at") or row.get("created_at"),
                "kind": "support_ticket",
                "title": f"ticket#{row.get('id')}:{row.get('status')}",
                "details": "",
            })
        for row in await self.get_pending_payments_by_user(user_id):
            items.append({
                "created_at": row.get("created_at"),
                "kind": "payment",
                "title": f"payment:{row.get('status')}",
                "details": f"{row.get('payment_id')} {float(row.get('amount') or 0):.2f} RUB",
            })
        for row in await self.get_withdraw_requests_by_user(user_id, limit=10):
            items.append({
                "created_at": row.get("created_at"),
                "kind": "withdraw",
                "title": f"withdraw:{row.get('status')}",
                "details": f"#{row.get('id')} {float(row.get('amount') or 0):.2f} RUB",
            })
        for row in await self.get_referral_balance_adjustments(user_id, limit=10):
            items.append({
                "created_at": row.get("created_at"),
                "kind": "balance_adjustment",
                "title": "balance_adjustment",
                "details": f"{float(row.get('amount') or 0):.2f} RUB {row.get('reason') or ''}".strip(),
            })
        items.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
        return items[: max(1, int(limit))]

    async def set_partner_rates(
        self,
        user_id: int,
        level1=None,
        level2=None,
        level3=None,
        status: str | None = None,
        note: str | None = None,
    ) -> bool:
        if self._sqlite_runtime_enabled:
            return await self.legacy.set_partner_rates(user_id, level1, level2, level3, status=status, note=note)
        payload = await self._mutate_user_payload(
            user_id,
            partner_percent_level1=level1,
            partner_percent_level2=level2,
            partner_percent_level3=level3,
            partner_status=status or "standard",
            partner_note=note or "",
        )
        return payload is not None

    async def get_partner_settings(self, user_id: int) -> dict[str, Any]:
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_partner_settings(user_id)
        user = await self.get_user(user_id)
        if not user:
            return {
                "user_id": user_id,
                "custom_percent_level1": None,
                "custom_percent_level2": None,
                "custom_percent_level3": None,
                "status": "standard",
                "note": "",
                "suspicious": False,
            }
        return {
            "user_id": user_id,
            "custom_percent_level1": user.get("partner_percent_level1"),
            "custom_percent_level2": user.get("partner_percent_level2"),
            "custom_percent_level3": user.get("partner_percent_level3"),
            "status": user.get("partner_status") or "standard",
            "note": user.get("partner_note") or "",
            "suspicious": bool(user.get("ref_suspicious")),
        }

    async def support_restriction_notifications_enabled(self) -> bool:
        raw = str(await self.get_setting("support:restriction_admin_notifications", "1") or "1").strip()
        return raw != "0"

    async def set_support_restriction_notifications_enabled(self, enabled: bool) -> bool:
        return await self.set_setting("support:restriction_admin_notifications", "1" if enabled else "0")

    async def ensure_ref_code(self, user_id: int) -> str | None:
        if self._sqlite_runtime_enabled:
            code = await self.legacy.ensure_ref_code(user_id)
        else:
            user = await self.get_user(user_id)
            code = str((user or {}).get("ref_code") or "").strip().upper()
            if not code:
                code = generate_ref_code()
                await self._mutate_user_payload(user_id, ref_code=code)
        if code:
            if self._sqlite_runtime_enabled:
                await self._sync_legacy_user_payload(user_id)
        return code

    async def set_ref_by(self, user_id: int, ref_by: int) -> bool:
        if self._sqlite_runtime_enabled:
            updated = await self.legacy.set_ref_by(user_id, ref_by)
        else:
            updated = await self._mutate_user_payload(user_id, ref_by=int(ref_by)) is not None
        if updated:
            if self._sqlite_runtime_enabled:
                await self._sync_legacy_user_payload(user_id)
        return updated

    async def ensure_panel_client_key(self, user_id: int) -> str:
        user = await self.get_user(user_id) or {}
        existing = str(user.get("panel_client_key") or "").strip()
        if existing:
            return existing
        key = secrets.token_hex(8)
        await self._mutate_user_payload(user_id, panel_client_key=key)
        return key

    async def get_bonus_days_pending(self, user_id: int) -> int:
        user = await self.get_user(user_id) or {}
        return int(user.get("bonus_days_pending") or 0)

    async def add_bonus_days_pending(self, user_id: int, days: int) -> bool:
        current = await self.get_bonus_days_pending(user_id)
        payload = await self._mutate_user_payload(user_id, bonus_days_pending=current + max(0, int(days)))
        return payload is not None

    async def clear_bonus_days_pending(self, user_id: int) -> bool:
        payload = await self._mutate_user_payload(user_id, bonus_days_pending=0)
        return payload is not None

    async def set_has_subscription(self, user_id: int) -> bool:
        payload = await self._mutate_user_payload(user_id, has_subscription=1)
        return payload is not None

    async def set_subscription(self, *, user_id: int, plan_text: str, ip_limit: int, traffic_gb: int, vpn_url: str) -> bool:
        repo = self._subscription_repo()
        if repo is not None:
            previous = await repo.get_latest_for_user(int(user_id))
            previous_meta = dict((previous or {}).get("meta") or {})
            previous_status = str((previous or {}).get("status") or "active")
            previous_expires = (previous or {}).get("expires_at")
            meta = {
                **previous_meta,
                "vpn_url": str(vpn_url or ""),
                "legacy_ip_limit": int(ip_limit or 0),
            }
            await repo.replace_active_with_new(
                user_id=int(user_id),
                status=previous_status if previous_status in {"active", "grace", "disabled", "pending"} else "active",
                plan_code=str(plan_text or ""),
                traffic_limit_bytes=max(0, int(traffic_gb or 0)) * 1024 * 1024 * 1024,
                traffic_used_bytes=int((previous or {}).get("traffic_used_bytes") or 0),
                expires_at=previous_expires,
                meta=meta,
            )
        payload = await self._mutate_user_payload(
            user_id,
            has_subscription=1,
            plan_text=str(plan_text or ""),
            ip_limit=int(ip_limit or 0),
            traffic_gb=int(traffic_gb or 0),
            vpn_url=str(vpn_url or ""),
        )
        return payload is not None

    async def remove_subscription(self, user_id: int) -> bool:
        repo = self._subscription_repo()
        if repo is not None:
            await repo.revoke_active(int(user_id), reason="runtime_remove_subscription")
        payload = await self._mutate_user_payload(
            user_id,
            has_subscription=0,
            plan_text="",
            traffic_gb=0,
            vpn_url="",
        )
        return payload is not None

    async def set_frozen(self, user_id: int, frozen_until: str) -> bool:
        payload = await self._mutate_user_payload(user_id, frozen_until=str(frozen_until or "").strip())
        return payload is not None

    async def clear_frozen(self, user_id: int) -> bool:
        payload = await self._mutate_user_payload(user_id, frozen_until=None)
        return payload is not None

    async def get_active_user_promo_code(self, user_id: int) -> str:
        return str(await self.get_setting(f"promo:active:{int(user_id)}", "") or "")

    async def set_active_user_promo_code(self, user_id: int, code: str) -> bool:
        return await self.set_setting(f"promo:active:{int(user_id)}", str(code or "").strip().upper())

    async def clear_active_user_promo_code(self, user_id: int) -> bool:
        return await self.set_setting(f"promo:active:{int(user_id)}", "")

    async def delete_user_everywhere(self, user_id: int) -> dict[str, int]:
        stats = await self.legacy.delete_user_everywhere(user_id) if self.legacy is not None else {}
        if self.postgres is not None and self.postgres.pool is not None:
            deleted_total = int(stats.get("deleted", 0) or 0)

            def _add_count(result: str) -> None:
                nonlocal deleted_total
                try:
                    deleted_total += int(str(result or "").split()[-1])
                except Exception:
                    pass

            async with self.postgres.pool.acquire() as conn:
                async with conn.transaction():
                    ticket_ids = await conn.fetch(
                        "SELECT id FROM support_tickets WHERE user_id = $1",
                        int(user_id),
                    )
                    payment_ids = await conn.fetch(
                        "SELECT payment_id FROM payment_intents WHERE user_id = $1 OR COALESCE(recipient_user_id, 0) = $1",
                        int(user_id),
                    )
                    payment_id_values = [str(row["payment_id"]) for row in payment_ids]
                    ticket_id_values = [int(row["id"]) for row in ticket_ids]

                    if ticket_id_values:
                        _add_count(await conn.execute("DELETE FROM support_messages WHERE ticket_id = ANY($1::bigint[])", ticket_id_values))
                    _add_count(await conn.execute("DELETE FROM support_messages WHERE sender_user_id = $1", int(user_id)))
                    if payment_id_values:
                        _add_count(await conn.execute("DELETE FROM payment_status_history WHERE payment_id = ANY($1::text[])", payment_id_values))
                        _add_count(await conn.execute("DELETE FROM payment_admin_actions WHERE payment_id = ANY($1::text[])", payment_id_values))
                        _add_count(await conn.execute("DELETE FROM payment_event_dedup WHERE payment_id = ANY($1::text[])", payment_id_values))
                        _add_count(await conn.execute("DELETE FROM legacy_payment_intents_archive WHERE payment_id = ANY($1::text[])", payment_id_values))

                    _add_count(await conn.execute("DELETE FROM support_tickets WHERE user_id = $1", int(user_id)))
                    _add_count(await conn.execute("UPDATE support_tickets SET assigned_admin_id = NULL WHERE assigned_admin_id = $1", int(user_id)))
                    _add_count(await conn.execute("DELETE FROM withdraw_requests WHERE user_id = $1", int(user_id)))
                    _add_count(await conn.execute("DELETE FROM payment_intents WHERE user_id = $1 OR COALESCE(recipient_user_id, 0) = $1", int(user_id)))
                    _add_count(await conn.execute("DELETE FROM subscriptions WHERE user_id = $1", int(user_id)))
                    _add_count(await conn.execute("DELETE FROM antifraud_events WHERE user_id = $1", int(user_id)))
                    _add_count(await conn.execute("DELETE FROM admin_user_actions WHERE user_id = $1 OR admin_user_id = $1", int(user_id)))
                    _add_count(await conn.execute("DELETE FROM payment_admin_actions WHERE admin_user_id = $1", int(user_id)))
                    _add_count(await conn.execute("DELETE FROM ref_history WHERE user_id = $1 OR ref_user_id = $1", int(user_id)))
                    _add_count(await conn.execute(
                        "DELETE FROM legacy_support_tickets_archive WHERE COALESCE(NULLIF(payload->>'user_id', ''), '0')::bigint = $1",
                        int(user_id),
                    ))
                    _add_count(await conn.execute(
                        "DELETE FROM legacy_support_messages_archive WHERE COALESCE(NULLIF(payload->>'sender_user_id', ''), '0')::bigint = $1",
                        int(user_id),
                    ))
                    _add_count(await conn.execute(
                        "DELETE FROM legacy_withdraw_requests_archive WHERE COALESCE(NULLIF(payload->>'user_id', ''), '0')::bigint = $1",
                        int(user_id),
                    ))
                    _add_count(await conn.execute("DELETE FROM legacy_users_archive WHERE user_id = $1", int(user_id)))
                    _add_count(await conn.execute("DELETE FROM bot_users WHERE user_id = $1", int(user_id)))
                    _add_count(await conn.execute(
                        """
                        UPDATE legacy_users_archive
                        SET payload = jsonb_set(
                            jsonb_set(COALESCE(payload, '{}'::jsonb), '{ref_by}', '0'::jsonb, true),
                            '{ref_rewarded}',
                            '0'::jsonb,
                            true
                        )
                        WHERE COALESCE((payload->>'ref_by')::bigint, 0) = $1
                        """,
                        int(user_id),
                    ))
                    _add_count(await conn.execute(
                        """
                        DELETE FROM app_meta
                        WHERE key = ANY($1::text[])
                           OR key LIKE ANY($2::text[])
                           OR (
                               key LIKE 'gift_link:%'
                               AND (
                                   COALESCE(NULLIF(value->>'buyer_user_id', ''), '0')::bigint = $3
                                   OR COALESCE(NULLIF(value->>'claimed_by_user_id', ''), '0')::bigint = $3
                                   OR COALESCE(NULLIF(value->>'recipient_user_id', ''), '0')::bigint = $3
                               )
                           )
                        """,
                        [
                            f"legacy_setting:promo:active:{int(user_id)}",
                            f"legacy_setting:support:blocked_until:{int(user_id)}",
                            f"legacy_setting:support:block_reason:{int(user_id)}",
                            f"legacy_setting:gift:note:{int(user_id)}",
                            f"legacy_user:{int(user_id)}",
                            f"legacy_payload:legacy_user:{int(user_id)}",
                        ],
                        [
                            f"legacy_setting:pending_ref:{int(user_id)}",
                            f"legacy_setting:%:{int(user_id)}",
                            f"legacy_payload:%:{int(user_id)}",
                        ],
                        int(user_id),
                    ))
            stats["deleted"] = deleted_total
        return stats

    async def mark_ref_rewarded(self, user_id: int) -> bool:
        if self._sqlite_runtime_enabled:
            updated = await self.legacy.mark_ref_rewarded(user_id)
        else:
            updated = await self._mutate_user_payload(user_id, ref_rewarded=1) is not None
        if updated:
            if self._sqlite_runtime_enabled:
                await self._sync_legacy_user_payload(user_id)
        return updated

    async def increment_ref_rewarded_count(self, user_id: int) -> bool:
        user = await self.get_user(user_id) or {}
        current = int(user.get("ref_rewarded_count") or 0)
        payload = await self._mutate_user_payload(user_id, ref_rewarded_count=current + 1)
        return payload is not None

    async def add_ref_history(self, user_id: int, ref_user_id: int, amount: float = 0, bonus_days: int = 0) -> None:
        if self._sqlite_runtime_enabled:
            await self.legacy.add_ref_history(user_id, ref_user_id, amount, bonus_days)
        repo = self._referral_repo()
        if repo is not None:
            await repo.add_history(
                user_id=user_id,
                ref_user_id=ref_user_id,
                amount=amount,
                bonus_days=bonus_days,
            )

    async def get_ref_history(self, user_id: int, limit: int = 10) -> list[dict[str, Any]]:
        repo = self._referral_repo()
        if repo is not None:
            rows = await repo.list_history(user_id, limit=limit)
            if rows:
                return rows
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_ref_history(user_id, limit)
        return []

    async def get_referrals_list(self, user_id: int) -> list[dict[str, Any]]:
        repo = self._referral_repo()
        if repo is not None:
            rows = await repo.list_referrals(user_id)
            if rows:
                return rows
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_referrals_list(user_id)
        return []

    async def count_recent_referrals_by_referrer(self, referrer_id: int, *, since_hours: int = 24) -> int:
        repo = self._referral_repo()
        if repo is not None:
            return await repo.count_recent_referrals(referrer_id, since_hours=since_hours)
        if self._sqlite_runtime_enabled:
            return await self.legacy.count_recent_referrals_by_referrer(referrer_id, since_hours=since_hours)
        return 0

    async def get_referral_summary(self, user_id: int) -> dict[str, Any]:
        repo = self._referral_repo()
        if repo is not None:
            return await repo.get_summary(user_id)
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_referral_summary(user_id)
        return {
            "total_refs": 0,
            "paid_refs": 0,
            "earned_rub": 0.0,
            "earned_bonus_days": 0,
            "completed_withdraw_rub": 0.0,
            "pending_withdraw_rub": 0.0,
        }

    async def get_referral_partner_cabinet(self, user_id: int) -> dict[str, Any]:
        summary = await self.get_referral_summary(user_id)
        if self._sqlite_runtime_enabled:
            settings = await self.legacy.get_partner_settings(user_id)
        else:
            user = await self.get_user(user_id) or {}
            settings = {
                "status": user.get("partner_status", "standard"),
                "custom_percent_level1": user.get("partner_percent_level1"),
                "custom_percent_level2": user.get("partner_percent_level2"),
                "custom_percent_level3": user.get("partner_percent_level3"),
                "note": user.get("partner_note", ""),
                "suspicious": bool(user.get("ref_suspicious")),
            }
        referrals = await self.get_referrals_list(user_id)
        total_referrals = len(referrals)
        paid_referrals = sum(1 for r in referrals if r.get("ref_rewarded"))
        user = await self.get_user(user_id) or {}
        trial_refs = 0
        for row in referrals:
            ref_user = await self.get_user(int(row.get("user_id") or 0))
            if ref_user and int(ref_user.get("trial_used") or 0) == 1:
                trial_refs += 1
        summary.update(
            {
                "status": settings.get("status", "standard"),
                "custom_percent_level1": settings.get("custom_percent_level1"),
                "custom_percent_level2": settings.get("custom_percent_level2"),
                "custom_percent_level3": settings.get("custom_percent_level3"),
                "note": settings.get("note", ""),
                "suspicious": settings.get("suspicious", False) or bool(user.get("ref_suspicious")),
                "trial_refs": trial_refs,
                "conversion_pct": round((paid_referrals / total_referrals * 100), 1) if total_referrals else 0.0,
            }
        )
        return summary

    async def get_top_referrers_extended(self, limit: int = 10) -> list[dict[str, Any]]:
        repo = self._referral_repo()
        if repo is not None:
            rows = await repo.list_top_referrers_extended(limit=limit)
            if rows:
                return rows
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_top_referrers_extended(limit)
        return []

    async def mark_referral_suspicious(self, user_id: int, flag: bool = True, note: str = "") -> bool:
        if self._sqlite_runtime_enabled:
            updated = await self.legacy.mark_referral_suspicious(user_id, flag, note)
        else:
            updated = await self._mutate_user_payload(
                user_id,
                ref_suspicious=1 if flag else 0,
                partner_note=note,
            ) is not None
        if updated:
            if self._sqlite_runtime_enabled:
                await self._sync_legacy_user_payload(user_id)
        return updated

    async def get_suspicious_referrals(self, limit: int = 20) -> list[dict[str, Any]]:
        repo = self._user_repo()
        if repo is not None:
            rows = await repo.list_suspicious_referrals(limit=limit)
            if rows:
                return rows
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_suspicious_referrals(limit)
        return []

    async def add_balance(self, user_id: int, amount: float) -> bool:
        if self._sqlite_runtime_enabled:
            updated = await self.legacy.add_balance(user_id, amount)
        else:
            user = await self.get_user(user_id) or self._default_user_payload(user_id)
            balance = float(user.get("balance") or 0) + float(amount or 0)
            updated = await self._mutate_user_payload(user_id, balance=balance) is not None
        if updated:
            if self._sqlite_runtime_enabled:
                await self._sync_legacy_user_payload(user_id)
        return updated

    async def subtract_balance(self, user_id: int, amount: float) -> bool:
        if self._sqlite_runtime_enabled:
            updated = await self.legacy.subtract_balance(user_id, amount)
        else:
            user = await self.get_user(user_id) or self._default_user_payload(user_id)
            current = float(user.get("balance") or 0)
            if current < float(amount or 0):
                return False
            updated = await self._mutate_user_payload(user_id, balance=current - float(amount or 0)) is not None
        if updated:
            if self._sqlite_runtime_enabled:
                await self._sync_legacy_user_payload(user_id)
        return updated

    async def get_promo_code(self, code: str) -> dict[str, Any] | None:
        if self._sqlite_runtime_enabled:
            promo = await self.legacy.get_promo_code(code)
            if promo is not None:
                return promo
        meta = self._meta_repo()
        if meta is None:
            return None
        normalized = (code or "").strip().upper()
        if not normalized:
            return None
        return await meta.get_legacy_payload("promo_code", normalized)

    async def create_or_update_promo_code(self, code: str, **kwargs) -> bool:
        meta = self._meta_repo()
        normalized = (code or "").strip().upper()
        ok = True
        if self._sqlite_runtime_enabled:
            ok = await self.legacy.create_or_update_promo_code(code, **kwargs)
            if ok and meta is not None and normalized:
                payload = await self.legacy.get_promo_code(normalized)
                if payload is not None:
                    await meta.set_legacy_payload("promo_code", normalized, payload)
        elif meta is not None and normalized:
            payload = {"code": normalized, **kwargs}
            await meta.set_legacy_payload("promo_code", normalized, payload)
        return ok

    async def list_promo_codes(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = await self._list_namespace_payloads("promo_code", limit=limit)
        rows.sort(key=lambda row: str(row.get("code") or ""))
        return rows[: max(1, int(limit))]

    async def validate_promo_code(self, code: str, *, user_id: int, plan_id: str = "") -> dict[str, Any] | None:
        promo = await self.get_promo_code(code)
        if not promo:
            return None
        if int(promo.get("active") or 0) != 1:
            return None
        expires_at = str(promo.get("expires_at") or "").strip()
        if expires_at:
            try:
                if datetime.fromisoformat(expires_at.replace("Z", "+00:00")) <= datetime.now(timezone.utc):
                    return None
            except ValueError:
                return None
        max_uses = int(promo.get("max_uses") or 0)
        used_count = int(promo.get("used_count") or 0)
        if max_uses > 0 and used_count >= max_uses:
            return None
        if int(promo.get("only_new_users") or 0) == 1:
            user = await self.get_user(user_id) or {}
            if int(user.get("has_subscription") or 0) == 1:
                return None
        plan_ids = [item.strip() for item in str(promo.get("plan_ids") or "").split(",") if item.strip()]
        if plan_ids and plan_id and plan_id not in plan_ids:
            return None
        user_limit = int(promo.get("user_limit") or 0)
        if user_limit > 0:
            usage = await self.get_promo_code_usage_details(str(promo.get("code") or code), limit=200)
            target = next((row for row in usage if int(row.get("user_id") or 0) == int(user_id)), None)
            if target and int(target.get("used_count") or 0) >= user_limit:
                return None
        return promo

    async def mark_promo_code_used(self, code: str, *, user_id: int) -> bool:
        promo = await self.get_promo_code(code)
        normalized = str(code or "").strip().upper()
        if not promo or not normalized:
            return False
        stats = await self.get_promo_code_usage_details(normalized, limit=500)
        current = next((row for row in stats if int(row.get("user_id") or 0) == int(user_id)), None)
        payload = dict(promo)
        payload["used_count"] = int(payload.get("used_count") or 0) + 1
        await self.create_or_update_promo_code(normalized, **{k: v for k, v in payload.items() if k != "code"})
        meta = self._meta_repo()
        if meta is not None:
            key = f"{normalized}:{int(user_id)}"
            usage_payload = {
                "code": normalized,
                "user_id": int(user_id),
                "used_count": int((current or {}).get("used_count") or 0) + 1,
                "last_used_at": self._now_iso(),
            }
            await meta.set_legacy_payload("promo_usage", key, usage_payload)
        return True

    async def get_promo_code_usage_details(self, code: str, limit: int = 20) -> list[dict[str, Any]]:
        normalized = str(code or "").strip().upper()
        rows = await self._list_namespace_payloads("promo_usage")
        filtered = [row for row in rows if str(row.get("code") or "").strip().upper() == normalized]
        filtered.sort(key=lambda row: str(row.get("last_used_at") or ""), reverse=True)
        return filtered[: max(1, int(limit))]

    async def get_promo_code_stats(self) -> list[dict[str, Any]]:
        promos = await self.list_promo_codes(limit=500)
        if not promos:
            return []
        payments_by_code: dict[str, dict[str, Any]] = {}
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT promo_code, COUNT(*) AS payments_count, COALESCE(SUM(amount), 0) AS total_amount
                    FROM payment_intents
                    WHERE promo_code != '' AND status = 'accepted'
                    GROUP BY promo_code
                    """
                )
            payments_by_code = {str(row["promo_code"]).upper(): dict(row) for row in rows}
        result: list[dict[str, Any]] = []
        for promo in promos:
            code = str(promo.get("code") or "").upper()
            payments = payments_by_code.get(code, {})
            result.append(
                {
                    **promo,
                    "payments_count": int(payments.get("payments_count") or 0),
                    "total_amount": float(payments.get("total_amount") or 0),
                }
            )
        result.sort(key=lambda row: int(row.get("used_count") or 0), reverse=True)
        return result

    async def add_antifraud_event(self, user_id: int, event_type: str, details: str = "", severity: str = "warning") -> int:
        row_id = 0
        if self._sqlite_runtime_enabled:
            row_id = await self.legacy.add_antifraud_event(user_id, event_type, details, severity)
        repo = self._operations_repo()
        if repo is not None:
            inserted_id = await repo.insert_antifraud_event(
                user_id=user_id,
                event_type=event_type,
                severity=severity,
                details=details[:500],
            )
            if row_id == 0:
                row_id = inserted_id
        return row_id

    async def count_antifraud_events(self, user_id: int, event_type: str, *, since_hours: int = 24) -> int:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                value = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM antifraud_events
                    WHERE user_id = $1
                      AND event_type = $2
                      AND created_at >= NOW() - make_interval(hours => $3::int)
                    """,
                    int(user_id),
                    str(event_type),
                    int(since_hours),
                )
            return int(value or 0)
        return 0

    async def get_recent_antifraud_events(self, limit: int = 20) -> list[dict[str, Any]]:
        repo = self._operations_repo()
        if repo is not None:
            rows = await repo.list_recent_antifraud_events(limit=limit)
            if rows:
                return rows
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_recent_antifraud_events(limit)
        return []

    async def ban_user(self, user_id: int, reason: str = "") -> bool:
        payload = await self._mutate_user_payload(user_id, banned=1, ban_reason=str(reason or ""))
        return payload is not None

    async def unban_user(self, user_id: int) -> bool:
        payload = await self._mutate_user_payload(user_id, banned=0, ban_reason="")
        return payload is not None

    async def get_recent_support_blacklist_hits(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = await self.get_recent_antifraud_events(limit)
        return [row for row in rows if str(row.get("event_type") or "") == "support_blacklist"][:limit]

    async def add_admin_user_action(self, user_id: int, admin_user_id: int, action: str, details: str = "") -> int:
        row_id = 0
        if self._sqlite_runtime_enabled:
            row_id = await self.legacy.add_admin_user_action(user_id, admin_user_id, action, details)
        repo = self._operations_repo()
        if repo is not None:
            inserted_id = await repo.insert_admin_user_action(
                user_id=user_id,
                admin_user_id=admin_user_id,
                action=action[:120],
                details=details[:1000],
            )
            if row_id == 0:
                row_id = inserted_id
        return row_id

    async def add_payment_admin_action(
        self,
        payment_id: str,
        admin_user_id: int,
        action: str,
        *,
        provider: str = "",
        result: str = "",
        details: str = "",
    ) -> int:
        row_id = 0
        if self._sqlite_runtime_enabled:
            row_id = await self.legacy.add_payment_admin_action(
                payment_id,
                admin_user_id,
                action,
                provider=provider,
                result=result,
                details=details,
            )
        repo = self._operations_repo()
        if repo is not None:
            inserted_id = await repo.insert_payment_admin_action(
                payment_id=payment_id,
                admin_user_id=admin_user_id,
                action=action[:120],
                provider=provider[:80],
                result=result[:120],
                details=details[:2000],
            )
            if row_id == 0:
                row_id = inserted_id
        return row_id

    async def get_recent_payment_admin_actions(self, *, limit: int = 20, provider: str | None = None) -> list[dict[str, Any]]:
        repo = self._operations_repo()
        if repo is not None:
            rows = await repo.list_recent_payment_admin_actions(limit=limit, provider=provider)
            if rows:
                return rows
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_recent_payment_admin_actions(limit=limit, provider=provider)
        return []

    async def get_payment_admin_actions(self, payment_id: str, limit: int = 20) -> list[dict[str, Any]]:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT *
                    FROM payment_admin_actions
                    WHERE payment_id = $1
                    ORDER BY created_at DESC, id DESC
                    LIMIT $2
                    """,
                    str(payment_id),
                    int(limit),
                )
            return [dict(row) for row in rows]
        return []

    async def get_auto_resolve_action_stats(self, payment_id: str, action: str) -> dict[str, Any]:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT COUNT(*) AS attempts, MAX(created_at) AS last_created_at
                    FROM payment_admin_actions
                    WHERE payment_id = $1 AND action = $2
                    """,
                    str(payment_id),
                    str(action),
                )
            return {
                "attempts": int((row["attempts"] if row else 0) or 0),
                "last_created_at": row["last_created_at"].isoformat() if row and row["last_created_at"] else "",
            }
        return {"attempts": 0, "last_created_at": ""}

    async def get_payment_status_history(self, payment_id: str, limit: int = 50) -> list[dict[str, Any]]:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT payment_id, from_status, to_status, source, reason, metadata_json, created_at
                    FROM payment_status_history
                    WHERE payment_id = $1
                    ORDER BY created_at DESC, id DESC
                    LIMIT $2
                    """,
                    str(payment_id),
                    int(limit),
                )
            return [dict(row) for row in rows]
        return []

    async def get_payment_provider_counts(self) -> list[dict[str, Any]]:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT
                        provider,
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE status = 'pending') AS pending,
                        COUNT(*) FILTER (WHERE status = 'processing') AS processing,
                        COUNT(*) FILTER (WHERE status = 'accepted') AS accepted,
                        COUNT(*) FILTER (WHERE status IN ('rejected', 'cancelled', 'refunded')) AS rejected
                    FROM payment_intents
                    GROUP BY provider
                    ORDER BY provider
                    """
                )
            return [dict(row) for row in rows]
        return []

    async def get_pending_payment_operations(self, *, limit: int = 20, provider: str = "all", operation: str = "all") -> list[dict[str, Any]]:
        rows = await self.get_overdue_payment_operations(
            minutes=Config.PAYMENT_ATTENTION_OPERATION_AGE_MIN,
            limit=limit,
            provider=provider,
        )
        if operation == "refund":
            return [row for row in rows if str(row.get("requested_status") or "") == "refund_requested"]
        if operation == "cancel":
            return [row for row in rows if str(row.get("requested_status") or "") == "cancel_requested"]
        return rows

    async def register_payment_event(
        self,
        event_key: str,
        *,
        payment_id: str = "",
        source: str = "",
        event_type: str = "",
        payload_excerpt: str = "",
    ) -> bool:
        created = True
        if self._sqlite_runtime_enabled:
            created = await self.legacy.register_payment_event(
                event_key,
                payment_id=payment_id,
                source=source,
                event_type=event_type,
                payload_excerpt=payload_excerpt,
            )
        repo = self._operations_repo()
        if repo is not None:
            repo_created = await repo.register_payment_event(
                event_key=event_key[:255],
                payment_id=payment_id,
                source=source[:120],
                event_type=event_type[:120],
                payload_excerpt=payload_excerpt[:1000],
            )
            if not self._sqlite_runtime_enabled:
                created = repo_created
        return created

    async def get_recent_payment_events(self, payment_id: str, limit: int = 10) -> list[dict[str, Any]]:
        repo = self._operations_repo()
        if repo is not None:
            rows = await repo.list_recent_payment_events(payment_id=payment_id, limit=limit)
            if rows:
                return rows
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_recent_payment_events(payment_id, limit)
        return []

    async def create_withdraw_request(self, user_id: int, amount: float) -> int:
        repo = self._operations_repo()
        if self._sqlite_runtime_enabled:
            request_id = await self.legacy.create_withdraw_request(user_id, amount)
            if repo is not None and request_id:
                payload = await self.legacy.get_withdraw_request(request_id)
                if payload is not None:
                    await repo.upsert_withdraw_request(payload)
            return request_id
        if repo is None:
            return 0
        payload = {
            "user_id": int(user_id),
            "amount": float(amount or 0),
            "status": "pending",
            "created_at": self._now_iso(),
            "processed_at": None,
        }
        request_id = await repo.create_withdraw_request(user_id=int(user_id), amount=float(amount or 0), payload=payload)
        return request_id

    async def get_pending_withdraw_requests(self) -> list[dict[str, Any]]:
        repo = self._operations_repo()
        if repo is not None:
            rows = await repo.list_pending_withdraw_requests()
            if rows:
                result: list[dict[str, Any]] = []
                for row in rows:
                    meta = row.get("meta") or {}
                    legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
                    result.append(legacy_payload if isinstance(legacy_payload, dict) else row)
                return result
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_pending_withdraw_requests()
        return []

    async def get_user_pending_withdraw_request(self, user_id: int) -> dict[str, Any] | None:
        repo = self._operations_repo()
        if repo is not None:
            row = await repo.get_pending_withdraw_request_for_user(user_id)
            if row:
                meta = row.get("meta") or {}
                legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
                return legacy_payload if isinstance(legacy_payload, dict) else row
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_user_pending_withdraw_request(user_id)
        return None

    async def get_withdraw_request(self, request_id: int) -> dict[str, Any] | None:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM withdraw_requests WHERE id = $1", int(request_id))
            if row:
                item = dict(row)
                meta = item.get("meta") or {}
                legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
                return legacy_payload if isinstance(legacy_payload, dict) else item
        return None

    async def get_withdraw_requests_by_user(self, user_id: int, limit: int = 10) -> list[dict[str, Any]]:
        repo = self._operations_repo()
        if repo is not None:
            rows = await repo.list_withdraw_requests_by_user(user_id, limit=limit)
            if rows:
                result: list[dict[str, Any]] = []
                for row in rows:
                    meta = row.get("meta") or {}
                    legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
                    result.append(legacy_payload if isinstance(legacy_payload, dict) else row)
                return result
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_withdraw_requests_by_user(user_id, limit)
        return []

    async def process_withdraw_request(self, request_id: int, accept: bool) -> bool:
        repo = self._operations_repo()
        if self._sqlite_runtime_enabled:
            updated = await self.legacy.process_withdraw_request(request_id, accept)
            if repo is not None:
                payload = await self.legacy.get_withdraw_request(request_id)
                if payload is not None:
                    await repo.upsert_withdraw_request(payload)
                    await self._sync_legacy_user_payload(int(payload.get("user_id") or 0))
            return updated
        if repo is None:
            return False
        rows = await repo.list_pending_withdraw_requests()
        target = next((row for row in rows if int(row.get("id") or 0) == int(request_id)), None)
        if not target:
            return False
        payload = {
            "id": int(request_id),
            "user_id": int(target.get("user_id") or 0),
            "amount": float(target.get("amount") or 0),
            "status": "completed" if accept else "rejected",
            "created_at": str(target.get("created_at") or self._now_iso()),
            "processed_at": self._now_iso(),
        }
        updated = await repo.update_withdraw_request_status(
            int(request_id),
            status="completed" if accept else "rejected",
            processed_at=datetime.now(timezone.utc),
            payload=payload,
        )
        return updated

    async def get_or_create_support_ticket(self, user_id: int) -> int:
        repo = self._operations_repo()
        if self._sqlite_runtime_enabled:
            ticket_id = await self.legacy.get_or_create_support_ticket(user_id)
            if repo is not None and ticket_id:
                payload = await self.legacy.get_support_ticket(ticket_id)
                if payload is not None:
                    await repo.upsert_support_ticket(payload)
            return ticket_id
        if repo is None:
            return 0
        existing = await repo.list_open_support_tickets(limit=100)
        for row in existing:
            if int(row.get("user_id") or 0) == int(user_id):
                return int(row["id"])
        payload = {
            "user_id": int(user_id),
            "status": "open",
            "assigned_admin_id": None,
            "created_at": self._now_iso(),
            "updated_at": self._now_iso(),
        }
        ticket_id = await repo.create_support_ticket(user_id=int(user_id), payload=payload)
        return ticket_id

    async def get_support_ticket(self, ticket_id: int) -> dict[str, Any] | None:
        if self._sqlite_runtime_enabled:
            payload = await self.legacy.get_support_ticket(ticket_id)
            if payload is not None:
                return payload
        repo = self._operations_repo()
        if repo is None:
            return None
        row = await repo.get_support_ticket(ticket_id)
        if not row:
            return None
        meta = row.get("meta") or {}
        legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
        if isinstance(legacy_payload, dict):
            return legacy_payload
        return row

    async def add_support_message(
        self,
        ticket_id: int,
        sender_role: str,
        sender_user_id: int,
        text: str,
        media_type: str = "",
        media_file_id: str = "",
    ) -> int:
        repo = self._operations_repo()
        if self._sqlite_runtime_enabled:
            message_id = await self.legacy.add_support_message(
                ticket_id,
                sender_role,
                sender_user_id,
                text,
                media_type=media_type,
                media_file_id=media_file_id,
            )
            if repo is not None:
                ticket_payload = await self.legacy.get_support_ticket(ticket_id)
                if ticket_payload is not None:
                    await repo.upsert_support_ticket(ticket_payload)
                message_payload = await self.legacy.get_last_support_message(ticket_id)
                if message_payload is not None:
                    await repo.add_support_message(message_payload)
            return message_id
        if repo is None:
            return 0
        payload = {
            "ticket_id": int(ticket_id),
            "sender_role": sender_role,
            "sender_user_id": int(sender_user_id),
            "text": text,
            "media_type": media_type,
            "media_file_id": media_file_id,
            "created_at": self._now_iso(),
        }
        message_id = await repo.create_support_message(payload)
        await repo.update_support_ticket_status(
            int(ticket_id),
            status="in_progress" if sender_role == "admin" else "open",
            assigned_admin_id=sender_user_id if sender_role == "admin" else None,
            payload={
                "id": int(ticket_id),
                "user_id": int((await self.get_support_ticket(ticket_id) or {}).get("user_id") or 0),
                "status": "in_progress" if sender_role == "admin" else "open",
                "assigned_admin_id": sender_user_id if sender_role == "admin" else None,
                "updated_at": self._now_iso(),
            },
        )
        return message_id

    async def get_support_messages(self, ticket_id: int, limit: int = 100) -> list[dict[str, Any]]:
        if self._sqlite_runtime_enabled:
            payload = await self.legacy.get_support_messages(ticket_id, limit)
            if payload:
                return payload
        repo = self._operations_repo()
        if repo is None:
            return []
        rows = await repo.list_support_messages(ticket_id, limit=limit)
        result: list[dict[str, Any]] = []
        for row in rows:
            meta = row.get("meta") or {}
            legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
            result.append(legacy_payload if isinstance(legacy_payload, dict) else row)
        return result

    async def get_last_support_message(self, ticket_id: int, sender_role: str | None = None) -> dict[str, Any] | None:
        if self._sqlite_runtime_enabled:
            payload = await self.legacy.get_last_support_message(ticket_id, sender_role)
            if payload is not None:
                return payload
        repo = self._operations_repo()
        if repo is None:
            return None
        row = await repo.get_last_support_message(ticket_id, sender_role=sender_role)
        if not row:
            return None
        meta = row.get("meta") or {}
        legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
        if isinstance(legacy_payload, dict):
            return legacy_payload
        return row

    async def list_open_support_tickets(self, limit: int = 20) -> list[dict[str, Any]]:
        if self._sqlite_runtime_enabled:
            payload = await self.legacy.list_open_support_tickets(limit)
            if payload:
                return payload
        repo = self._operations_repo()
        if repo is None:
            return []
        rows = await repo.list_open_support_tickets(limit=limit)
        result: list[dict[str, Any]] = []
        for row in rows:
            meta = row.get("meta") or {}
            legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
            result.append(legacy_payload if isinstance(legacy_payload, dict) else row)
        return result

    async def set_support_ticket_status(self, ticket_id: int, status: str, assigned_admin_id: int | None = None) -> bool:
        repo = self._operations_repo()
        if self._sqlite_runtime_enabled:
            updated = await self.legacy.set_support_ticket_status(ticket_id, status, assigned_admin_id)
            if repo is not None:
                payload = await self.legacy.get_support_ticket(ticket_id)
                if payload is not None:
                    await repo.upsert_support_ticket(payload)
            return updated
        if repo is None:
            return False
        current = await self.get_support_ticket(ticket_id) or {}
        payload = {
            "id": int(ticket_id),
            "user_id": int(current.get("user_id") or 0),
            "status": status,
            "assigned_admin_id": assigned_admin_id,
            "created_at": current.get("created_at") or self._now_iso(),
            "updated_at": self._now_iso(),
        }
        updated = await repo.update_support_ticket_status(ticket_id, status=status, assigned_admin_id=assigned_admin_id, payload=payload)
        return updated

    async def close_support_ticket(self, ticket_id: int) -> bool:
        return await self.set_support_ticket_status(int(ticket_id), "closed", None)

    async def list_user_support_tickets(self, user_id: int, limit: int = 10) -> list[dict[str, Any]]:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT * FROM support_tickets
                    WHERE user_id = $1
                    ORDER BY COALESCE(updated_at, created_at) DESC, id DESC
                    LIMIT $2
                    """,
                    int(user_id),
                    int(limit),
                )
            result: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                meta = item.get("meta") or {}
                legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
                result.append(legacy_payload if isinstance(legacy_payload, dict) else item)
            return result
        return []

    async def reset_expiry_notifications(self, user_id: int) -> bool:
        if self._sqlite_runtime_enabled:
            updated = await self.legacy.reset_expiry_notifications(user_id)
        else:
            updated = await self._mutate_user_payload(user_id, notified_3d=0, notified_1d=0, notified_1h=0) is not None
        if updated:
            if self._sqlite_runtime_enabled:
                await self._sync_legacy_user_payload(user_id)
        return updated

    async def list_support_restricted_users(self, limit: int = 50) -> list[dict[str, Any]]:
        meta = self._meta_repo()
        if meta is None:
            if self._sqlite_runtime_enabled:
                return await self.legacy.list_support_restricted_users(limit)
            return []
        rows = await meta.list_legacy_settings("support:blocked_until:")
        result: list[dict[str, Any]] = []
        for key, _value in rows:
            try:
                user_id = int(key.rsplit(":", 1)[-1])
            except ValueError:
                continue
            restriction = await self.get_support_restriction(user_id)
            if restriction.get("active"):
                result.append(
                    {
                        "user_id": user_id,
                        "expires_at": restriction.get("expires_at") or "",
                        "reason": restriction.get("reason") or "",
                    }
                )
            if len(result) >= int(limit):
                break
        return result

    async def add_pending_payment(
        self,
        payment_id,
        user_id,
        plan_id,
        amount,
        msg_id=None,
        provider=None,
        *,
        recipient_user_id=None,
        promo_code: str = "",
        promo_discount_percent: float = 0.0,
        gift_label: str = "",
        gift_note: str = "",
    ) -> bool:
        repo = self._payment_repo()
        if self._sqlite_runtime_enabled:
            created = await self.legacy.add_pending_payment(
                payment_id,
                user_id,
                plan_id,
                amount,
                msg_id,
                provider,
                recipient_user_id=recipient_user_id,
                promo_code=promo_code,
                promo_discount_percent=promo_discount_percent,
                gift_label=gift_label,
            )
            if repo is not None:
                payload = await self.legacy.get_pending_payment(payment_id)
                if payload is not None:
                    if gift_note:
                        payload["gift_note"] = gift_note
                    await repo.upsert_legacy_intent(payload)
            return created
        if repo is None:
            return False
        payload = {
            "payment_id": str(payment_id),
            "user_id": int(user_id),
            "plan_id": str(plan_id),
            "amount": float(amount or 0),
            "status": "pending",
            "msg_id": msg_id,
            "provider": str(provider or ""),
            "provider_payment_id": "",
            "recipient_user_id": recipient_user_id,
            "promo_code": promo_code,
            "promo_discount_percent": float(promo_discount_percent or 0),
            "gift_label": gift_label,
            "gift_note": gift_note,
            "activation_attempts": 0,
            "last_error": "",
        }
        created = await repo.create_intent(payload)
        return created

    async def get_pending_payment(self, payment_id) -> dict[str, Any] | None:
        if self._sqlite_runtime_enabled:
            payload = await self.legacy.get_pending_payment(payment_id)
            if payload is not None:
                return payload
        repo = self._payment_repo()
        if repo is None:
            return None
        row = await repo.get_intent(str(payment_id))
        if not row:
            return None
        meta = row.get("meta") or {}
        legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
        if isinstance(legacy_payload, dict):
            return legacy_payload
        return row

    async def get_pending_payment_by_provider_id(self, provider: str, provider_payment_id: str) -> dict[str, Any] | None:
        if self._sqlite_runtime_enabled:
            payload = await self.legacy.get_pending_payment_by_provider_id(provider, provider_payment_id)
            if payload is not None:
                return payload
        repo = self._payment_repo()
        if repo is None:
            return None
        row = await repo.get_by_provider_payment_id(provider=provider, provider_payment_id=provider_payment_id)
        if not row:
            return None
        meta = row.get("meta") or {}
        legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
        return legacy_payload if isinstance(legacy_payload, dict) else row

    async def set_pending_payment_provider_id(self, payment_id, provider: str, provider_payment_id: str) -> bool:
        updated = True
        if self._sqlite_runtime_enabled:
            updated = await self.legacy.set_pending_payment_provider_id(payment_id, provider, provider_payment_id)
        repo = self._payment_repo()
        if repo is not None and provider_payment_id:
            await repo.set_provider_payment_id(str(payment_id), provider=provider, provider_payment_id=provider_payment_id)
        return updated

    async def claim_pending_payment(self, payment_id: str, *, source: str = "", reason: str = "", metadata: str = "") -> bool:
        repo = self._payment_repo()
        if self._sqlite_runtime_enabled:
            claimed = await self.legacy.claim_pending_payment(payment_id, source=source, reason=reason, metadata=metadata)
            if repo is not None:
                payload = await self.legacy.get_pending_payment(payment_id)
                if payload is not None:
                    await repo.upsert_legacy_intent(payload)
            return claimed
        if repo is None:
            return False
        claimed = await repo.claim_processing(payment_id, source=source, reason=reason)
        return claimed

    async def release_processing_payment(
        self,
        payment_id: str,
        error_text: str | None = None,
        *,
        source: str = "",
        metadata: str = "",
        retry_delay_sec: int = 0,
    ) -> bool:
        repo = self._payment_repo()
        if self._sqlite_runtime_enabled:
            released = await self.legacy.release_processing_payment(
                payment_id,
                error_text,
                source=source,
                metadata=metadata,
                retry_delay_sec=retry_delay_sec,
            )
            if repo is not None:
                payload = await self.legacy.get_pending_payment(payment_id)
                if payload is not None:
                    await repo.upsert_legacy_intent(payload)
            return released
        if repo is None:
            return False
        released = await repo.release_processing(
            payment_id,
            error_text=error_text,
            retry_delay_sec=retry_delay_sec,
            source=source,
            metadata={"legacy_metadata": metadata} if metadata else {},
        )
        return released

    async def mark_payment_error(self, payment_id: str, error_text: str) -> bool:
        repo = self._payment_repo()
        if self._sqlite_runtime_enabled:
            updated = await self.legacy.mark_payment_error(payment_id, error_text)
            if repo is not None:
                await repo.mark_error(payment_id, error_text)
                payload = await self.legacy.get_pending_payment(payment_id)
                if payload is not None:
                    await repo.upsert_legacy_intent(payload)
            return updated
        if repo is None:
            return False
        updated = await repo.mark_error(payment_id, error_text)
        return updated

    async def update_payment_status(
        self,
        payment_id,
        status,
        allowed_current_statuses=None,
        *,
        source: str = "",
        reason: str = "",
        metadata: str = "",
    ) -> bool:
        repo = self._payment_repo()
        if self._sqlite_runtime_enabled:
            updated = await self.legacy.update_payment_status(
                payment_id,
                status,
                allowed_current_statuses=allowed_current_statuses,
                source=source,
                reason=reason,
                metadata=metadata,
            )
            if repo is not None:
                payload = await self.legacy.get_pending_payment(payment_id)
                if payload is not None:
                    await repo.upsert_legacy_intent(payload)
            return updated
        if repo is None:
            return False
        expected_from = None
        if allowed_current_statuses and len(allowed_current_statuses) == 1:
            expected_from = list(allowed_current_statuses)[0]
        updated = await repo.transition_status(
            payment_id,
            expected_from=expected_from,
            to_status=status,
            source=source,
            reason=reason,
            metadata={"legacy_metadata": metadata} if metadata else {},
        )
        return updated

    async def record_payment_status_transition(
        self,
        payment_id: str,
        *,
        from_status,
        to_status: str,
        source: str = "",
        reason: str = "",
        metadata: str = "",
    ) -> int:
        repo = self._payment_repo()
        if self._sqlite_runtime_enabled:
            row_id = await self.legacy.record_payment_status_transition(
                payment_id,
                from_status=from_status,
                to_status=to_status,
                source=source,
                reason=reason,
                metadata=metadata,
            )
            if repo is not None:
                await repo.append_status_history(
                    payment_id,
                    from_status=from_status,
                    to_status=to_status,
                    source=source,
                    reason=reason,
                    metadata={"legacy_metadata": metadata} if metadata else {},
                )
            return row_id
        if repo is None:
            return 0
        row_id = await repo.append_status_history(
            payment_id,
            from_status=from_status,
            to_status=to_status,
            source=source,
            reason=reason,
            metadata={"legacy_metadata": metadata} if metadata else {},
        )
        return row_id

    async def get_processing_payments_count(self) -> int:
        repo = self._payment_repo()
        if repo is not None:
            return await repo.count_by_status("processing")
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_processing_payments_count()
        return 0

    async def get_all_pending_payments(self, statuses: list[str] | None = None) -> list[dict[str, Any]]:
        repo = self._payment_repo()
        if repo is not None:
            rows = await repo.list_by_statuses(statuses or ["pending"])
            if rows:
                result: list[dict[str, Any]] = []
                for row in rows:
                    meta = row.get("meta") or {}
                    legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
                    result.append(legacy_payload if isinstance(legacy_payload, dict) else row)
                return result
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_all_pending_payments(statuses)
        return []

    async def get_pending_payments_by_user(self, user_id: int) -> list[dict[str, Any]]:
        repo = self._payment_repo()
        if repo is not None:
            rows = await repo.list_by_user(user_id)
            if rows:
                result: list[dict[str, Any]] = []
                for row in rows:
                    meta = row.get("meta") or {}
                    legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
                    result.append(legacy_payload if isinstance(legacy_payload, dict) else row)
                return result
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_pending_payments_by_user(user_id)
        return []

    async def get_user_pending_payment(self, user_id: int, *, plan_id: str | None = None, statuses: list[str] | None = None) -> dict[str, Any] | None:
        rows = await self.get_pending_payments_by_user(user_id)
        allowed_statuses = statuses or ["pending", "processing"]
        for row in rows:
            if row.get("status") not in allowed_statuses:
                continue
            if plan_id is not None and str(row.get("plan_id") or "") != plan_id:
                continue
            return row
        return None

    async def get_old_pending_payments(self, minutes: int = 10) -> list[dict[str, Any]]:
        repo = self._payment_repo()
        if repo is not None:
            rows = await repo.list_pending_older_than_minutes(minutes)
            if rows:
                result: list[dict[str, Any]] = []
                for row in rows:
                    meta = row.get("meta") or {}
                    legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
                    result.append(legacy_payload if isinstance(legacy_payload, dict) else row)
                return result
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_old_pending_payments(minutes)
        return []

    async def get_recent_payment_errors(self, hours: int = 24) -> list[dict[str, Any]]:
        repo = self._payment_repo()
        if repo is not None:
            rows = await repo.list_recent_errors(hours)
            if rows:
                result: list[dict[str, Any]] = []
                for row in rows:
                    meta = row.get("meta") or {}
                    legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
                    result.append(legacy_payload if isinstance(legacy_payload, dict) else row)
                return result
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_recent_payment_errors(hours)
        return []

    async def get_stale_processing_payments(self, *, minutes: int = 15, limit: int = 20, provider: str | None = None) -> list[dict[str, Any]]:
        repo = self._payment_repo()
        if repo is not None:
            rows = await repo.list_stale_processing(minutes=minutes, limit=limit, provider=provider)
            if rows:
                result: list[dict[str, Any]] = []
                for row in rows:
                    meta = row.get("meta") or {}
                    legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
                    result.append(legacy_payload if isinstance(legacy_payload, dict) else row)
                return result
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_stale_processing_payments(minutes=minutes, limit=limit, provider=provider)
        return []

    async def get_confirmed_payment_status_mismatches(self, *, hours: int = 24, limit: int = 20, provider: str | None = None) -> list[dict[str, Any]]:
        repo = self._payment_repo()
        if repo is not None:
            rows = await repo.list_confirmed_mismatches(hours=hours, limit=limit, provider=provider)
            if rows:
                return rows
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_confirmed_payment_status_mismatches(hours=hours, limit=limit, provider=provider)
        return []

    async def reclaim_stale_processing_payments(self, timeout_minutes: int = 15, *, source: str = "system/recovery") -> int:
        repo = self._payment_repo()
        if repo is not None:
            stale = await repo.list_stale_processing(minutes=timeout_minutes, limit=1000, provider=None)
            released = 0
            for row in stale:
                ok = await repo.release_processing(
                    str(row.get("payment_id") or ""),
                    error_text="auto-released stale processing lock",
                    retry_delay_sec=0,
                    source=source,
                    metadata={},
                )
                released += int(bool(ok))
            return released
        if self._sqlite_runtime_enabled:
            return await self.legacy.reclaim_stale_processing_payments(timeout_minutes=timeout_minutes, source=source)
        return 0

    async def cleanup_old_pending_payments(self, days: int = 30) -> int:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                result = await conn.execute(
                    """
                    DELETE FROM payment_intents
                    WHERE status IN ('accepted', 'rejected', 'cancelled', 'refunded')
                      AND COALESCE(processed_at, updated_at, created_at) < NOW() - make_interval(days => $1::int)
                    """,
                    int(days),
                )
            return int(result.split()[-1])
        if self._sqlite_runtime_enabled:
            return await self.legacy.cleanup_old_pending_payments(days=days)
        return 0

    async def cleanup_old_payment_events(self, days: int = 30) -> int:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                result = await conn.execute(
                    """
                    DELETE FROM payment_event_dedup
                    WHERE created_at < NOW() - make_interval(days => $1::int)
                    """,
                    int(days),
                )
            return int(result.split()[-1])
        if self._sqlite_runtime_enabled:
            return await self.legacy.cleanup_old_payment_events(days=days)
        return 0

    async def archive_closed_support_tickets(self, days: int = 14) -> int:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                result = await conn.execute(
                    """
                    UPDATE support_tickets
                    SET status = 'archived', updated_at = NOW()
                    WHERE status = 'closed'
                      AND updated_at < NOW() - make_interval(days => $1::int)
                    """,
                    int(days),
                )
            return int(result.split()[-1])
        if self._sqlite_runtime_enabled:
            return await self.legacy.archive_closed_support_tickets(days=days)
        return 0

    async def get_schema_drift_issues(self) -> list[str]:
        if self.postgres is not None:
            return []
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_schema_drift_issues()
        return []

    async def auto_repair_schema_drift(self) -> list[str]:
        if self.postgres is not None:
            return []
        if self._sqlite_runtime_enabled:
            return await self.legacy.auto_repair_schema_drift()
        return []

    async def sync_schema_version_with_migrations(self) -> int:
        if self.postgres is not None and self.postgres.pool is not None:
            try:
                async with self.postgres.pool.acquire() as conn:
                    value = await conn.fetchval("SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
                return int(value or 0)
            except Exception:
                return 0
        if self._sqlite_runtime_enabled:
            return await self.legacy.sync_schema_version_with_migrations()
        return 0

    async def get_schema_version(self) -> int:
        if self.postgres is not None and self.postgres.pool is not None:
            try:
                async with self.postgres.pool.acquire() as conn:
                    value = await conn.fetchval("SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
                return int(value or 0)
            except Exception:
                return 0
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_schema_version()
        return 0

    async def register_transient_message(self, chat_id: int, message_id: int, *, category: str = "", ttl_hours: int = 24) -> int:
        if self._sqlite_runtime_enabled:
            return await self.legacy.register_transient_message(chat_id, message_id, category=category, ttl_hours=ttl_hours)
        return 0

    async def get_expired_transient_messages(self, limit: int = 100) -> list[dict[str, Any]]:
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_expired_transient_messages(limit=limit)
        return []

    async def delete_transient_message_record(self, record_id: int) -> bool:
        if self._sqlite_runtime_enabled:
            return await self.legacy.delete_transient_message_record(record_id)
        return True

    async def count_user_payments_created_since(self, user_id: int, seconds: int) -> int:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                value = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM payment_intents
                    WHERE user_id = $1
                      AND created_at >= NOW() - make_interval(secs => $2::int)
                    """,
                    int(user_id),
                    int(seconds),
                )
            return int(value or 0)
        if self._sqlite_runtime_enabled:
            return await self.legacy.count_user_payments_created_since(user_id, seconds)
        return 0

    async def count_user_pending_payments(self, user_id: int) -> int:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                value = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM payment_intents
                    WHERE user_id = $1 AND status IN ('pending', 'processing')
                    """,
                    int(user_id),
                )
            return int(value or 0)
        if self._sqlite_runtime_enabled:
            return await self.legacy.count_user_pending_payments(user_id)
        return 0

    async def list_unclaimed_gift_links_for_reminder(self, *, hours: int, limit: int = 20) -> list[dict[str, Any]]:
        if self._sqlite_runtime_enabled:
            return await self.legacy.list_unclaimed_gift_links_for_reminder(hours=hours, limit=limit)
        return []

    async def touch_gift_link_reminder(self, token: str) -> bool:
        if self._sqlite_runtime_enabled:
            return await self.legacy.touch_gift_link_reminder(token)
        return True

    async def list_stale_support_tickets(self, *, minutes: int = 45, limit: int = 20) -> list[dict[str, Any]]:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, user_id, status, assigned_admin_id, created_at, updated_at, meta
                    FROM support_tickets
                    WHERE status = ANY($1::text[])
                      AND COALESCE(updated_at, created_at, NOW()) <= NOW() - make_interval(mins => $2::int)
                    ORDER BY COALESCE(updated_at, created_at, NOW()) ASC, id ASC
                    LIMIT $3
                    """,
                    ["open", "in_progress"],
                    int(minutes),
                    int(limit),
                )
            return [dict(row) for row in rows]
        if self._sqlite_runtime_enabled:
            return await self.legacy.list_stale_support_tickets(minutes=minutes, limit=limit)
        return []

    async def get_support_ticket_reminder_state(self, ticket_id: int) -> str:
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_support_ticket_reminder_state(ticket_id)
        return str(await self.get_setting(f"support:reminder:{int(ticket_id)}", "") or "")

    async def set_support_ticket_reminder_state(self, ticket_id: int, value: str) -> bool:
        if self._sqlite_runtime_enabled:
            return await self.legacy.set_support_ticket_reminder_state(ticket_id, value)
        return await self.set_setting(f"support:reminder:{int(ticket_id)}", value)

    async def get_overdue_payment_operations(self, *, minutes: int = 20, limit: int = 20, provider: str | None = None) -> list[dict[str, Any]]:
        if self.postgres is not None and self.postgres.pool is not None:
            query = """
                SELECT
                    p.payment_id,
                    p.user_id,
                    p.plan_id,
                    p.amount,
                    p.provider,
                    p.provider_payment_id,
                    p.status,
                    p.created_at,
                    h.to_status AS requested_status,
                    h.source AS requested_source,
                    h.reason AS requested_reason,
                    h.metadata_json AS requested_metadata,
                    h.created_at AS requested_at
                FROM payment_intents p
                JOIN (
                    SELECT DISTINCT ON (payment_id)
                        payment_id, to_status, source, reason, metadata_json, created_at
                    FROM payment_status_history
                    WHERE to_status = ANY($1::text[])
                    ORDER BY payment_id, id DESC
                ) h ON h.payment_id = p.payment_id
                WHERE p.status <> ALL($2::text[])
                  AND h.created_at < NOW() - make_interval(mins => $3::int)
            """
            params: list[Any] = [
                ["refund_requested", "cancel_requested"],
                ["refunded", "cancelled", "rejected"],
                int(minutes),
            ]
            if provider and provider != "all":
                params.append(provider)
                query += f" AND p.provider = ${len(params)}"
            params.append(int(limit))
            query += f" ORDER BY h.created_at ASC, p.created_at ASC LIMIT ${len(params)}"
            async with self.postgres.pool.acquire() as conn:
                rows = await conn.fetch(query, *params)
            normalized: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                metadata = item.get("requested_metadata")
                if isinstance(metadata, dict):
                    item["requested_metadata"] = ";".join(
                        f"{str(key).strip()}={str(value).strip()}"
                        for key, value in metadata.items()
                        if str(key).strip()
                    )
                elif metadata is None:
                    item["requested_metadata"] = ""
                else:
                    item["requested_metadata"] = str(metadata)
                normalized.append(item)
            return normalized
        if self._sqlite_runtime_enabled:
            return await self.legacy.get_overdue_payment_operations(minutes=minutes, limit=limit, provider=provider)
        return []

    async def get_support_daily_report(self, *, days_ago: int = 0) -> dict[str, Any]:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT
                        (CURRENT_DATE - ($1::int || ' days')::interval)::date AS report_date,
                        COUNT(*) FILTER (
                            WHERE DATE(COALESCE(created_at, NOW())) =
                                  (CURRENT_DATE - ($1::int || ' days')::interval)::date
                        ) AS opened_tickets,
                        COUNT(*) FILTER (
                            WHERE DATE(COALESCE(updated_at, created_at, NOW())) =
                                  (CURRENT_DATE - ($1::int || ' days')::interval)::date
                              AND status IN ('closed', 'archived')
                        ) AS closed_tickets,
                        COUNT(*) FILTER (WHERE status IN ('open', 'in_progress')) AS open_tickets
                    FROM support_tickets
                    """,
                    int(days_ago),
                )
                msg_row = await conn.fetchrow(
                    """
                    SELECT
                        COUNT(*) FILTER (
                            WHERE DATE(COALESCE(created_at, NOW())) =
                                  (CURRENT_DATE - ($1::int || ' days')::interval)::date
                              AND sender_role = 'user'
                        ) AS messages_from_users,
                        COUNT(*) FILTER (
                            WHERE DATE(COALESCE(created_at, NOW())) =
                                  (CURRENT_DATE - ($1::int || ' days')::interval)::date
                              AND sender_role = 'admin'
                        ) AS messages_from_admins
                    FROM support_messages
                    """,
                    int(days_ago),
                )
            return {
                "report_date": str(row["report_date"] or ""),
                "opened_tickets": int(row["opened_tickets"] or 0),
                "closed_tickets": int(row["closed_tickets"] or 0),
                "open_tickets": int(row["open_tickets"] or 0),
                "messages_from_users": int(msg_row["messages_from_users"] or 0),
                "messages_from_admins": int(msg_row["messages_from_admins"] or 0),
            }
        return {"report_date": "", "opened_tickets": 0, "closed_tickets": 0, "open_tickets": 0, "messages_from_users": 0, "messages_from_admins": 0}

    async def get_daily_incident_report(self, *, days_ago: int = 0) -> dict[str, Any]:
        report_date = (datetime.now(timezone.utc) - timedelta(days=max(0, int(days_ago)))).date().isoformat()
        payment_errors = len(await self.get_recent_payment_errors(hours=24))
        support_blacklist_hits = len(await self.get_recent_support_blacklist_hits(limit=100))
        stale_processing = len(await self.get_stale_processing_payments(minutes=Config.STALE_PROCESSING_TIMEOUT_MIN, limit=200, provider="all"))
        old_pending = len(await self.get_old_pending_payments(minutes=10))
        return {
            "report_date": report_date,
            "payment_errors": payment_errors,
            "support_blacklist_hits": support_blacklist_hits,
            "stale_processing": stale_processing,
            "old_pending": old_pending,
        }

    async def get_users_for_broadcast_segment(self, audience: str) -> list[dict[str, Any]]:
        audience = str(audience or "").strip().lower()
        if audience == "active":
            return await self.get_all_subscribers()
        if audience == "banned":
            rows = await self.get_all_users()
            return [row for row in rows if int(row.get("banned") or 0) == 1]
        if audience == "inactive":
            rows = await self.get_all_users()
            return [row for row in rows if int(row.get("has_subscription") or 0) != 1]
        return await self.get_all_users()

    async def create_gift_link(
        self,
        *,
        token: str,
        buyer_user_id: int,
        recipient_user_id: int | None = None,
        plan_id: str,
        note: str = "",
        custom_duration_days: int | None = None,
    ) -> bool:
        meta = self._meta_repo()
        if meta is None:
            return False
        payload = {
            "token": str(token or "").strip(),
            "buyer_user_id": int(buyer_user_id),
            "recipient_user_id": int(recipient_user_id or 0) if recipient_user_id else None,
            "plan_id": str(plan_id or ""),
            "note": str(note or ""),
            "custom_duration_days": int(custom_duration_days or 0) if custom_duration_days else 0,
            "created_at": self._now_iso(),
            "claimed_by_user_id": 0,
            "claimed_at": "",
            "reminded_at": "",
        }
        await meta.set_legacy_payload("gift_link", payload["token"], payload)
        return True

    async def get_gift_link(self, token: str) -> dict[str, Any] | None:
        meta = self._meta_repo()
        if meta is None:
            return None
        payload = await meta.get_legacy_payload("gift_link", str(token or "").strip())
        if not payload:
            return None
        buyer_user_id = int(payload.get("buyer_user_id") or 0)
        claimed_by_user_id = int(payload.get("claimed_by_user_id") or 0)
        created_at = _parse_utc_iso_dt(payload.get("created_at"))
        if (
            buyer_user_id == int(getattr(Config, "ADMIN_GIFT_REFERRER_ID", 794419497))
            and claimed_by_user_id == 0
            and created_at is not None
            and created_at <= datetime.now(timezone.utc) - timedelta(days=int(getattr(Config, "ADMIN_GIFT_EXPIRE_DAYS", 3) or 3))
        ):
            await meta.delete_legacy_payload("gift_link", str(token or "").strip())
            return None
        return payload

    async def claim_gift_link(self, token: str, user_id: int) -> bool:
        gift = await self.get_gift_link(token)
        meta = self._meta_repo()
        if not gift or meta is None:
            return False
        if int(gift.get("claimed_by_user_id") or 0) not in {0, int(user_id)}:
            return False
        gift["claimed_by_user_id"] = int(user_id)
        gift["claimed_at"] = self._now_iso()
        await meta.set_legacy_payload("gift_link", str(token or "").strip(), gift)
        return True

    async def get_gift_links_by_buyer(self, user_id: int, limit: int = 10) -> list[dict[str, Any]]:
        rows = await self._list_namespace_payloads("gift_link")
        filtered = [row for row in rows if int(row.get("buyer_user_id") or 0) == int(user_id)]
        filtered.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
        return filtered[: max(1, int(limit))]

    async def get_user_gift_history(self, user_id: int, limit: int = 10) -> list[dict[str, Any]]:
        rows = await self._list_namespace_payloads("gift_link")
        filtered = [
            row for row in rows
            if int(row.get("buyer_user_id") or 0) == int(user_id)
            or int(row.get("claimed_by_user_id") or 0) == int(user_id)
            or int(row.get("recipient_user_id") or 0) == int(user_id)
        ]
        filtered.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
        return filtered[: max(1, int(limit))]

    async def list_recent_gift_links(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = await self._list_namespace_payloads("gift_link")
        rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
        return rows[: max(1, int(limit))]

    async def list_recent_claimed_gift_links(self, limit: int = 10) -> list[dict[str, Any]]:
        rows = await self._list_namespace_payloads("gift_link")
        filtered = [row for row in rows if int(row.get("claimed_by_user_id") or 0) > 0]
        filtered.sort(key=lambda row: str(row.get("claimed_at") or row.get("created_at") or ""), reverse=True)
        return filtered[: max(1, int(limit))]

    async def get_gift_links_stats(self) -> dict[str, int]:
        rows = await self._list_namespace_payloads("gift_link")
        claimed = sum(1 for row in rows if int(row.get("claimed_by_user_id") or 0) > 0)
        total = len(rows)
        return {"total": total, "claimed": claimed, "unclaimed": max(0, total - claimed)}

    async def cleanup_expired_admin_gift_links(self, *, days: int = 3, buyer_user_id: int = 794419497) -> int:
        meta = self._meta_repo()
        if meta is None:
            return 0
        rows = await self._list_namespace_payloads("gift_link")
        threshold = datetime.now(timezone.utc) - timedelta(days=max(1, int(days)))
        deleted = 0
        for row in rows:
            token = str(row.get("token") or "").strip()
            if not token:
                continue
            if int(row.get("buyer_user_id") or 0) != int(buyer_user_id):
                continue
            if int(row.get("claimed_by_user_id") or 0) > 0:
                continue
            created_at = _parse_utc_iso_dt(row.get("created_at"))
            if created_at is None or created_at > threshold:
                continue
            await meta.delete_legacy_payload("gift_link", token)
            deleted += 1
        return deleted

    async def get_meta(self, key: str) -> dict[str, Any] | None:
        if self.postgres is None:
            return None
        return await self.postgres.get_meta(key)

    async def set_meta(self, key: str, value: dict[str, Any]) -> None:
        if self.postgres is not None:
            await self.postgres.set_meta(key, value)

    async def upsert_bot_user(
        self,
        user_id: int,
        username: str | None,
        first_name: str | None,
        last_name: str | None,
        language_code: str | None,
        *,
        is_admin: bool = False,
    ) -> None:
        if self.postgres is not None:
            await self.postgres.upsert_bot_user(user_id, username, first_name, last_name, language_code, is_admin=is_admin)

    async def ping(self) -> bool:
        if self.postgres is None:
            return False
        return await self.postgres.ping()

    async def execute_script(self, path: Path) -> None:
        if self.postgres is not None:
            await self.postgres.execute_script(path)

    async def executescript(self, sql: str) -> None:
        if self.postgres is not None and self.postgres.pool is not None:
            async with self.postgres.pool.acquire() as conn:
                await conn.execute(str(sql))

    async def record_migration(self, version: int, name: str) -> None:
        await self.set_meta(f"migration:{int(version)}", {"version": int(version), "name": str(name), "applied_at": self._now_iso()})

    async def get_applied_migration_versions(self) -> list[int]:
        rows = await self._list_namespace_payloads("migration")
        versions = sorted({int(row.get("version") or 0) for row in rows if int(row.get("version") or 0) > 0})
        return versions

    def __getattr__(self, item: str) -> Any:
        if not self._sqlite_runtime_enabled:
            raise AttributeError(f"Database has no attribute {item!r} in PostgreSQL-only runtime")
        return getattr(self.legacy, item)
