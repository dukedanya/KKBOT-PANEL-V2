from __future__ import annotations

import logging
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
from kkbot.repositories.users import UserRepository

from .database import Database as LegacyDatabase

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path: str):
        self.legacy = LegacyDatabase(db_path)
        self.postgres: PostgresDatabase | None = None
        if Config.DATABASE_URL:
            self.postgres = PostgresDatabase(
                Config.DATABASE_URL,
                min_size=Config.DATABASE_MIN_POOL,
                max_size=Config.DATABASE_MAX_POOL,
            )

    async def connect(self) -> None:
        await self.legacy.connect()
        if self.postgres is not None:
            await self.postgres.connect()
            migrations_dir = Path(__file__).resolve().parents[2] / "migrations" / "postgres"
            await apply_postgres_migrations(self.postgres, migrations_dir)
            await self._run_legacy_import_if_needed()

    async def close(self) -> None:
        if self.postgres is not None:
            await self.postgres.close()
        await self.legacy.close()

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

    def _operations_repo(self) -> OperationsRepository | None:
        if self.postgres is None:
            return None
        return OperationsRepository(self.postgres)

    async def _sync_legacy_user_payload(self, user_id: int) -> None:
        meta = self._meta_repo()
        repo = self._user_repo()
        if meta is None or repo is None:
            return
        payload = await self.legacy.get_user(user_id)
        if payload is not None:
            await meta.set_legacy_payload("legacy_user", str(user_id), payload)
            await repo.upsert_legacy_archive(payload)

    async def _run_legacy_import_if_needed(self) -> None:
        postgres = self.postgres
        if postgres is None or postgres.pool is None:
            return
        sqlite_path = Path(self.legacy.db_path)
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
        created = await self.legacy.add_user(user_id)
        repo = self._user_repo()
        if repo is not None:
            await repo.upsert_basic_user(user_id)
            await self._sync_legacy_user_payload(user_id)
        return created

    async def get_user(self, user_id: int) -> dict[str, Any] | None:
        payload = await self.legacy.get_user(user_id)
        if payload is not None:
            return payload
        meta = self._meta_repo()
        if meta is not None:
            stored = await meta.get_legacy_payload("legacy_user", str(user_id))
            if stored:
                return stored
        repo = self._user_repo()
        if repo is None:
            return None
        snapshot = await repo.get_user_snapshot(user_id)
        if not snapshot:
            return None
        subscription = snapshot.get("subscription") or {}
        return {
            "user_id": int(snapshot["user_id"]),
            "username": snapshot.get("username") or "",
            "first_name": snapshot.get("first_name") or "",
            "last_name": snapshot.get("last_name") or "",
            "language_code": snapshot.get("language_code") or "",
            "has_subscription": 1 if subscription and subscription.get("status") in {"active", "grace"} else 0,
            "banned": 0,
            "plan_text": subscription.get("plan_code") or "",
            "vpn_url": ((subscription.get("meta") or {}).get("vpn_url") if isinstance(subscription, dict) else "") or "",
        }

    async def update_user(self, user_id: int, **kwargs) -> bool:
        updated = await self.legacy.update_user(user_id, **kwargs)
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
            await self._sync_legacy_user_payload(user_id)
        return updated

    async def get_total_users(self) -> int:
        repo = self._user_repo()
        if repo is not None:
            return await repo.count_users()
        return await self.legacy.get_total_users()

    async def get_all_users(self) -> list[dict[str, Any]]:
        repo = self._user_repo()
        if repo is not None:
            rows = await repo.list_all_legacy_users()
            if rows:
                return rows
        return await self.legacy.get_all_users()

    async def get_banned_users_count(self) -> int:
        repo = self._user_repo()
        if repo is not None:
            return await repo.count_banned_users()
        return await self.legacy.get_banned_users_count()

    async def get_subscribed_user_ids(self) -> list[int]:
        repo = self._user_repo()
        if repo is not None:
            return await repo.get_subscribed_user_ids()
        return await self.legacy.get_subscribed_user_ids()

    async def get_all_subscribers(self) -> list[dict[str, Any]]:
        repo = self._user_repo()
        if repo is None:
            return await self.legacy.get_all_subscribers()
        rows = await repo.list_active_subscribers()
        if not rows:
            return await self.legacy.get_all_subscribers()
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
        return await self.legacy.get_setting(key, default)

    async def set_setting(self, key: str, value: str) -> bool:
        ok = await self.legacy.set_setting(key, value)
        meta = self._meta_repo()
        if meta is not None:
            await meta.set_legacy_setting(key, value)
        return ok

    async def get_support_restriction(self, user_id: int) -> dict[str, Any]:
        return await self.legacy.get_support_restriction(user_id)

    async def support_restriction_notifications_enabled(self) -> bool:
        raw = str(await self.get_setting("support:restriction_admin_notifications", "1") or "1").strip()
        return raw != "0"

    async def set_support_restriction_notifications_enabled(self, enabled: bool) -> bool:
        return await self.set_setting("support:restriction_admin_notifications", "1" if enabled else "0")

    async def ensure_ref_code(self, user_id: int) -> str | None:
        code = await self.legacy.ensure_ref_code(user_id)
        if code:
            await self._sync_legacy_user_payload(user_id)
        return code

    async def set_ref_by(self, user_id: int, ref_by: int) -> bool:
        updated = await self.legacy.set_ref_by(user_id, ref_by)
        if updated:
            await self._sync_legacy_user_payload(user_id)
        return updated

    async def add_ref_history(self, user_id: int, ref_user_id: int, amount: float = 0, bonus_days: int = 0) -> None:
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
        return await self.legacy.get_ref_history(user_id, limit)

    async def get_referrals_list(self, user_id: int) -> list[dict[str, Any]]:
        repo = self._referral_repo()
        if repo is not None:
            rows = await repo.list_referrals(user_id)
            if rows:
                return rows
        return await self.legacy.get_referrals_list(user_id)

    async def count_recent_referrals_by_referrer(self, referrer_id: int, *, since_hours: int = 24) -> int:
        repo = self._referral_repo()
        if repo is not None:
            return await repo.count_recent_referrals(referrer_id, since_hours=since_hours)
        return await self.legacy.count_recent_referrals_by_referrer(referrer_id, since_hours=since_hours)

    async def get_referral_summary(self, user_id: int) -> dict[str, Any]:
        repo = self._referral_repo()
        if repo is not None:
            return await repo.get_summary(user_id)
        return await self.legacy.get_referral_summary(user_id)

    async def get_referral_partner_cabinet(self, user_id: int) -> dict[str, Any]:
        summary = await self.get_referral_summary(user_id)
        settings = await self.legacy.get_partner_settings(user_id)
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
        return await self.legacy.get_top_referrers_extended(limit)

    async def mark_referral_suspicious(self, user_id: int, flag: bool = True, note: str = "") -> bool:
        updated = await self.legacy.mark_referral_suspicious(user_id, flag, note)
        if updated:
            await self._sync_legacy_user_payload(user_id)
        return updated

    async def get_suspicious_referrals(self, limit: int = 20) -> list[dict[str, Any]]:
        repo = self._user_repo()
        if repo is not None:
            rows = await repo.list_suspicious_referrals(limit=limit)
            if rows:
                return rows
        return await self.legacy.get_suspicious_referrals(limit)

    async def add_balance(self, user_id: int, amount: float) -> bool:
        updated = await self.legacy.add_balance(user_id, amount)
        if updated:
            await self._sync_legacy_user_payload(user_id)
        return updated

    async def subtract_balance(self, user_id: int, amount: float) -> bool:
        updated = await self.legacy.subtract_balance(user_id, amount)
        if updated:
            await self._sync_legacy_user_payload(user_id)
        return updated

    async def get_promo_code(self, code: str) -> dict[str, Any] | None:
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
        ok = await self.legacy.create_or_update_promo_code(code, **kwargs)
        meta = self._meta_repo()
        normalized = (code or "").strip().upper()
        if ok and meta is not None and normalized:
            payload = await self.legacy.get_promo_code(normalized)
            if payload is not None:
                await meta.set_legacy_payload("promo_code", normalized, payload)
        return ok

    async def add_antifraud_event(self, user_id: int, event_type: str, details: str = "", severity: str = "warning") -> int:
        row_id = await self.legacy.add_antifraud_event(user_id, event_type, details, severity)
        repo = self._operations_repo()
        if repo is not None:
            await repo.insert_antifraud_event(
                user_id=user_id,
                event_type=event_type,
                severity=severity,
                details=details[:500],
            )
        return row_id

    async def get_recent_antifraud_events(self, limit: int = 20) -> list[dict[str, Any]]:
        repo = self._operations_repo()
        if repo is not None:
            rows = await repo.list_recent_antifraud_events(limit=limit)
            if rows:
                return rows
        return await self.legacy.get_recent_antifraud_events(limit)

    async def get_recent_support_blacklist_hits(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = await self.get_recent_antifraud_events(limit)
        return [row for row in rows if str(row.get("event_type") or "") == "support_blacklist"][:limit]

    async def add_admin_user_action(self, user_id: int, admin_user_id: int, action: str, details: str = "") -> int:
        row_id = await self.legacy.add_admin_user_action(user_id, admin_user_id, action, details)
        repo = self._operations_repo()
        if repo is not None:
            await repo.insert_admin_user_action(
                user_id=user_id,
                admin_user_id=admin_user_id,
                action=action[:120],
                details=details[:1000],
            )
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
            await repo.insert_payment_admin_action(
                payment_id=payment_id,
                admin_user_id=admin_user_id,
                action=action[:120],
                provider=provider[:80],
                result=result[:120],
                details=details[:2000],
            )
        return row_id

    async def get_recent_payment_admin_actions(self, *, limit: int = 20, provider: str | None = None) -> list[dict[str, Any]]:
        repo = self._operations_repo()
        if repo is not None:
            rows = await repo.list_recent_payment_admin_actions(limit=limit, provider=provider)
            if rows:
                return rows
        return await self.legacy.get_recent_payment_admin_actions(limit=limit, provider=provider)

    async def register_payment_event(
        self,
        event_key: str,
        *,
        payment_id: str = "",
        source: str = "",
        event_type: str = "",
        payload_excerpt: str = "",
    ) -> bool:
        created = await self.legacy.register_payment_event(
            event_key,
            payment_id=payment_id,
            source=source,
            event_type=event_type,
            payload_excerpt=payload_excerpt,
        )
        repo = self._operations_repo()
        if repo is not None:
            await repo.register_payment_event(
                event_key=event_key[:255],
                payment_id=payment_id,
                source=source[:120],
                event_type=event_type[:120],
                payload_excerpt=payload_excerpt[:1000],
            )
        return created

    async def get_recent_payment_events(self, payment_id: str, limit: int = 10) -> list[dict[str, Any]]:
        repo = self._operations_repo()
        if repo is not None:
            rows = await repo.list_recent_payment_events(payment_id=payment_id, limit=limit)
            if rows:
                return rows
        return await self.legacy.get_recent_payment_events(payment_id, limit)

    async def create_withdraw_request(self, user_id: int, amount: float) -> int:
        request_id = await self.legacy.create_withdraw_request(user_id, amount)
        repo = self._operations_repo()
        if repo is not None and request_id:
            payload = await self.legacy.get_withdraw_request(request_id)
            if payload is not None:
                await repo.upsert_withdraw_request(payload)
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
        return await self.legacy.get_pending_withdraw_requests()

    async def get_user_pending_withdraw_request(self, user_id: int) -> dict[str, Any] | None:
        repo = self._operations_repo()
        if repo is not None:
            row = await repo.get_pending_withdraw_request_for_user(user_id)
            if row:
                meta = row.get("meta") or {}
                legacy_payload = meta.get("legacy_payload") if isinstance(meta, dict) else None
                return legacy_payload if isinstance(legacy_payload, dict) else row
        return await self.legacy.get_user_pending_withdraw_request(user_id)

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
        return await self.legacy.get_withdraw_requests_by_user(user_id, limit)

    async def process_withdraw_request(self, request_id: int, accept: bool) -> bool:
        updated = await self.legacy.process_withdraw_request(request_id, accept)
        repo = self._operations_repo()
        if repo is not None:
            payload = await self.legacy.get_withdraw_request(request_id)
            if payload is not None:
                await repo.upsert_withdraw_request(payload)
                await self._sync_legacy_user_payload(int(payload.get("user_id") or 0))
        return updated

    async def get_or_create_support_ticket(self, user_id: int) -> int:
        ticket_id = await self.legacy.get_or_create_support_ticket(user_id)
        repo = self._operations_repo()
        if repo is not None and ticket_id:
            payload = await self.legacy.get_support_ticket(ticket_id)
            if payload is not None:
                await repo.upsert_support_ticket(payload)
        return ticket_id

    async def get_support_ticket(self, ticket_id: int) -> dict[str, Any] | None:
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
        message_id = await self.legacy.add_support_message(
            ticket_id,
            sender_role,
            sender_user_id,
            text,
            media_type=media_type,
            media_file_id=media_file_id,
        )
        repo = self._operations_repo()
        if repo is not None:
            ticket_payload = await self.legacy.get_support_ticket(ticket_id)
            if ticket_payload is not None:
                await repo.upsert_support_ticket(ticket_payload)
            message_payload = await self.legacy.get_last_support_message(ticket_id)
            if message_payload is not None:
                await repo.add_support_message(message_payload)
        return message_id

    async def get_support_messages(self, ticket_id: int, limit: int = 100) -> list[dict[str, Any]]:
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
        updated = await self.legacy.set_support_ticket_status(ticket_id, status, assigned_admin_id)
        repo = self._operations_repo()
        if repo is not None:
            payload = await self.legacy.get_support_ticket(ticket_id)
            if payload is not None:
                await repo.upsert_support_ticket(payload)
        return updated

    async def reset_expiry_notifications(self, user_id: int) -> bool:
        updated = await self.legacy.reset_expiry_notifications(user_id)
        if updated:
            await self._sync_legacy_user_payload(user_id)
        return updated

    async def list_support_restricted_users(self, limit: int = 50) -> list[dict[str, Any]]:
        meta = self._meta_repo()
        if meta is None:
            return await self.legacy.list_support_restricted_users(limit)
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
        repo = self._payment_repo()
        if repo is not None:
            payload = await self.legacy.get_pending_payment(payment_id)
            if payload is not None:
                if gift_note:
                    payload["gift_note"] = gift_note
                await repo.upsert_legacy_intent(payload)
        return created

    async def get_pending_payment(self, payment_id) -> dict[str, Any] | None:
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
        updated = await self.legacy.set_pending_payment_provider_id(payment_id, provider, provider_payment_id)
        repo = self._payment_repo()
        if repo is not None and provider_payment_id:
            await repo.set_provider_payment_id(str(payment_id), provider=provider, provider_payment_id=provider_payment_id)
        return updated

    async def claim_pending_payment(self, payment_id: str, *, source: str = "", reason: str = "", metadata: str = "") -> bool:
        claimed = await self.legacy.claim_pending_payment(payment_id, source=source, reason=reason, metadata=metadata)
        repo = self._payment_repo()
        if repo is not None:
            payload = await self.legacy.get_pending_payment(payment_id)
            if payload is not None:
                await repo.upsert_legacy_intent(payload)
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
        released = await self.legacy.release_processing_payment(
            payment_id,
            error_text,
            source=source,
            metadata=metadata,
            retry_delay_sec=retry_delay_sec,
        )
        repo = self._payment_repo()
        if repo is not None:
            payload = await self.legacy.get_pending_payment(payment_id)
            if payload is not None:
                await repo.upsert_legacy_intent(payload)
        return released

    async def mark_payment_error(self, payment_id: str, error_text: str) -> bool:
        updated = await self.legacy.mark_payment_error(payment_id, error_text)
        repo = self._payment_repo()
        if repo is not None:
            await repo.mark_error(payment_id, error_text)
            payload = await self.legacy.get_pending_payment(payment_id)
            if payload is not None:
                await repo.upsert_legacy_intent(payload)
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
        updated = await self.legacy.update_payment_status(
            payment_id,
            status,
            allowed_current_statuses=allowed_current_statuses,
            source=source,
            reason=reason,
            metadata=metadata,
        )
        repo = self._payment_repo()
        if repo is not None:
            payload = await self.legacy.get_pending_payment(payment_id)
            if payload is not None:
                await repo.upsert_legacy_intent(payload)
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
        row_id = await self.legacy.record_payment_status_transition(
            payment_id,
            from_status=from_status,
            to_status=to_status,
            source=source,
            reason=reason,
            metadata=metadata,
        )
        repo = self._payment_repo()
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

    async def get_processing_payments_count(self) -> int:
        repo = self._payment_repo()
        if repo is not None:
            return await repo.count_by_status("processing")
        return await self.legacy.get_processing_payments_count()

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
        return await self.legacy.get_all_pending_payments(statuses)

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
        return await self.legacy.get_pending_payments_by_user(user_id)

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
        return await self.legacy.get_old_pending_payments(minutes)

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
        return await self.legacy.get_recent_payment_errors(hours)

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
        return await self.legacy.get_stale_processing_payments(minutes=minutes, limit=limit, provider=provider)

    async def get_confirmed_payment_status_mismatches(self, *, hours: int = 24, limit: int = 20, provider: str | None = None) -> list[dict[str, Any]]:
        repo = self._payment_repo()
        if repo is not None:
            rows = await repo.list_confirmed_mismatches(hours=hours, limit=limit, provider=provider)
            if rows:
                return rows
        return await self.legacy.get_confirmed_payment_status_mismatches(hours=hours, limit=limit, provider=provider)

    def __getattr__(self, item: str) -> Any:
        return getattr(self.legacy, item)
