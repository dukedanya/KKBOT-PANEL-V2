from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import services.subscriptions as legacy_subscriptions

from kkbot.db.postgres import PostgresDatabase
from kkbot.repositories.subscriptions import SubscriptionRepository


class SubscriptionService:
    def __init__(self, db: PostgresDatabase):
        self.db = db
        self.repo = SubscriptionRepository(db)

    async def create_panel_subscription(
        self,
        *,
        user_id: int,
        plan_code: str,
        traffic_limit_gb: int,
        vpn_url: str,
        panel_email: str,
        panel_sub_id: str,
        panel_client_uuid: str = "",
        created_inbounds: list[int] | None = None,
        expires_at: str | None = None,
        ip_limit: int = 0,
        status: str = "active",
    ) -> int:
        return await self.repo.replace_active_with_new(
            user_id=user_id,
            plan_code=plan_code,
            traffic_limit_bytes=max(0, int(traffic_limit_gb)) * 1024 * 1024 * 1024,
            expires_at=expires_at,
            status=status,
            meta={
                "vpn_url": vpn_url,
                "panel_email": panel_email,
                "panel_sub_id": panel_sub_id,
                "panel_client_uuid": panel_client_uuid,
                "created_inbounds": created_inbounds or [],
                "ip_limit": ip_limit,
            },
        )

    async def revoke_subscription(self, user_id: int, *, reason: str = "") -> bool:
        changed = await self.repo.revoke_active(user_id, reason=reason)
        return changed > 0

    async def get_subscription_status(self, user_id: int) -> dict[str, Any]:
        record = await self.repo.get_latest_for_user(user_id)
        if not record:
            return {"active": False, "record": None, "status": "missing"}
        status = str(record.get("status") or "missing")
        return {
            "active": status in {"active", "grace"},
            "record": record,
            "status": status,
        }


def _resolve_postgres_db(db: object) -> PostgresDatabase | None:
    if isinstance(db, PostgresDatabase):
        return db
    postgres = getattr(db, "postgres", None)
    if isinstance(postgres, PostgresDatabase):
        return postgres
    return None


def _normalize_meta(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


async def panel_base_email(user_id: int, db: object) -> str:
    if _resolve_postgres_db(db):
        return f"{user_id}@kakoitovpn"
    return await legacy_subscriptions.panel_base_email(user_id, db)


def panel_sub_id(user_id: int) -> str:
    return legacy_subscriptions.panel_sub_id(user_id)


def parse_db_datetime(value):
    return legacy_subscriptions.parse_db_datetime(value)


def is_currently_frozen(user):
    return legacy_subscriptions.is_currently_frozen(user)


async def get_remaining_active_days(user_id: int, panel, db) -> int:
    return await legacy_subscriptions.get_remaining_active_days(user_id, panel, db)


async def get_subscription_status(user_id: int, db, panel) -> dict[str, Any]:
    postgres_db = _resolve_postgres_db(db)
    if postgres_db:
        service = SubscriptionService(postgres_db)
        current = await service.get_subscription_status(user_id)
        record = current.get("record") or {}
        meta = _normalize_meta(record.get("meta"))
        expiry_raw = record.get("expires_at")
        expiry_dt = None
        if expiry_raw:
            try:
                expiry_dt = datetime.fromisoformat(str(expiry_raw).replace("Z", "+00:00"))
            except ValueError:
                expiry_dt = None
        return {
            "active": bool(current.get("active")),
            "user": {"plan_text": record.get("plan_code", ""), "vpn_url": meta.get("vpn_url", "")},
            "is_frozen": False,
            "frozen_until": None,
            "expiry_dt": expiry_dt,
            "record": record,
        }
    return await legacy_subscriptions.get_subscription_status(user_id, db=db, panel=panel)


def get_minimal_by_price():
    return legacy_subscriptions.get_minimal_by_price()


async def create_subscription(
    user_id: int,
    plan: dict[str, Any],
    db,
    panel,
    *,
    extra_days: int = 0,
    days_override: int | None = None,
    plan_suffix: str | None = None,
    preserve_active_days: bool = False,
):
    if _resolve_postgres_db(db):
        postgres_db = _resolve_postgres_db(db)
        assert postgres_db is not None
        base_email = await panel_base_email(user_id, db)
        stable_sub_id = panel_sub_id(user_id)
        client = await panel.upsert_client(
            email=base_email,
            limit_ip=int(plan.get("ip_limit", 0) or 0),
            total_gb=int(plan.get("traffic_gb", 0) or 0),
            days=int(days_override or plan.get("duration_days", 30) or 30) + int(extra_days or 0),
            sub_id=stable_sub_id,
        )
        if not client:
            return None
        plan_name = plan.get("name") or plan.get("id") or "Тариф"
        if plan_suffix:
            plan_name = f"{plan_name}{plan_suffix}"
        client_uuid = str(client.get("id") or client.get("clientId") or "").strip()
        vpn_url = legacy_subscriptions.build_primary_subscription_url(
            client_uuid=client_uuid,
            sub_id=str(client.get("subId") or stable_sub_id),
        )
        service = SubscriptionService(postgres_db)
        await service.create_panel_subscription(
            user_id=user_id,
            plan_code=plan_name,
            traffic_limit_gb=int(plan.get("traffic_gb", 0) or 0),
            vpn_url=vpn_url,
            panel_email=base_email,
            panel_sub_id=str(client.get("subId") or stable_sub_id),
            panel_client_uuid=client_uuid,
            created_inbounds=client.get("created_inbounds") or [],
            ip_limit=int(plan.get("ip_limit", 0) or 0),
        )
        return vpn_url
    return await legacy_subscriptions.create_subscription(
        user_id,
        plan,
        db=db,
        panel=panel,
        extra_days=extra_days,
        days_override=days_override,
        plan_suffix=plan_suffix,
        preserve_active_days=preserve_active_days,
    )


async def is_active_subscription(user_id: int, db, panel) -> bool:
    postgres_db = _resolve_postgres_db(db)
    if postgres_db:
        current = await SubscriptionService(postgres_db).get_subscription_status(user_id)
        return bool(current.get("active"))
    return await legacy_subscriptions.is_active_subscription(user_id, db=db, panel=panel)


async def reward_referrer_days(referrer_id: int, bonus_days: int, db, panel) -> None:
    if _resolve_postgres_db(db):
        return
    await legacy_subscriptions.reward_referrer_days(referrer_id, bonus_days, db=db, panel=panel)


async def reward_referrer_percent(user_id: int, amount: float, db) -> None:
    if _resolve_postgres_db(db):
        return
    await legacy_subscriptions.reward_referrer_percent(user_id, amount, db=db)


async def revoke_subscription(user_id: int, db, panel, *, reason: str = "") -> bool:
    postgres_db = _resolve_postgres_db(db)
    if postgres_db:
        try:
            await panel.delete_client(await panel_base_email(user_id, db))
        except Exception:
            pass
        return await SubscriptionService(postgres_db).revoke_subscription(user_id, reason=reason)
    return await legacy_subscriptions.revoke_subscription(user_id, db=db, panel=panel, reason=reason)
