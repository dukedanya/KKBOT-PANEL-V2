import logging
import math
import time
from datetime import datetime
from typing import Any, Dict, Optional

from config import Config
from db import Database
from services.panel import PanelAPI
from utils.helpers import notify_admins, notify_user
from utils.subscription_links import build_primary_subscription_url, render_connection_info

logger = logging.getLogger(__name__)


async def panel_base_email(user_id: int, db: Database) -> str:
    prefix = (Config.PANEL_EMAIL_PREFIX or "").strip()
    if hasattr(db, "ensure_panel_client_key"):
        key = await db.ensure_panel_client_key(user_id)
        if key:
            return f"{prefix}{key}@{Config.PANEL_EMAIL_DOMAIN}"
    return f"{prefix}{user_id}@{Config.PANEL_EMAIL_DOMAIN}"


def panel_sub_id(user_id: int) -> str:
    return f"user{int(user_id)}"


def parse_db_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return None


def is_currently_frozen(user: Optional[Dict[str, Any]]) -> bool:
    frozen_until = parse_db_datetime((user or {}).get("frozen_until"))
    return bool(frozen_until and frozen_until > datetime.utcnow())


async def get_remaining_active_days(user_id: int, panel: PanelAPI, db: Database) -> int:
    """Возвращает остаток активных дней по клиенту, округляя вверх до целого дня."""
    try:
        base_email = await panel_base_email(user_id, db)
        clients = await panel.find_clients_full_by_email(base_email)
        if not clients:
            return 0

        now_ms = int(time.time() * 1000)
        max_expiry = 0
        for client in clients:
            client_expiry = int(client.get("expiryTime", 0) or 0)
            client_obj = client.get("clientObj") or {}
            client_obj_expiry = int(client_obj.get("expiryTime", 0) or 0)
            max_expiry = max(max_expiry, client_expiry, client_obj_expiry)
        if max_expiry <= now_ms:
            return 0

        remaining_ms = max_expiry - now_ms
        return max(0, math.ceil(remaining_ms / 86400000))
    except Exception as e:
        logger.error("Remaining days lookup failed: user=%s error=%s", user_id, e)
        return 0


async def get_subscription_status(user_id: int, db: Database, panel: PanelAPI) -> Dict[str, Any]:
    user = await db.get_user(user_id) or {}
    active = await is_active_subscription(user_id, db=db, panel=panel)
    user = await db.get_user(user_id) or user

    frozen_until = parse_db_datetime(user.get("frozen_until"))
    if frozen_until and frozen_until <= datetime.utcnow():
        await db.clear_frozen(user_id)
        user["frozen_until"] = None
        frozen_until = None

    expiry_dt = None
    if active:
        base_email = await panel_base_email(user_id, db)
        clients = await panel.find_clients_full_by_email(base_email)
        expiry_times = []
        for client in clients:
            expiry_times.append(int(client.get("expiryTime", 0) or 0))
            client_obj = client.get("clientObj") or {}
            expiry_times.append(int(client_obj.get("expiryTime", 0) or 0))
        max_expiry = max(expiry_times) if expiry_times else 0
        if max_expiry > 0:
            expiry_dt = datetime.fromtimestamp(max_expiry / 1000)

    return {
        "active": active,
        "user": user,
        "is_frozen": bool(frozen_until and frozen_until > datetime.utcnow()),
        "frozen_until": frozen_until,
        "expiry_dt": expiry_dt,
    }


def get_minimal_by_price() -> Optional[Dict[str, Any]]:
    try:
        from tariffs import get_all_active, is_trial_plan
        plans = [p for p in get_all_active() if not is_trial_plan(p)]
        if not plans:
            return None
        return min(plans, key=lambda p: float(p.get("price_rub", 0) or 0))
    except Exception as e:
        logger.error("Minimal tariff lookup failed: %s", e)
        return None


async def create_subscription(
    user_id: int,
    plan: Dict[str, Any],
    db: Database,
    panel: PanelAPI,
    *,
    extra_days: int = 0,
    days_override: Optional[int] = None,
    plan_suffix: Optional[str] = None,
    preserve_active_days: bool = False,
) -> Optional[str]:
    if not plan:
        return None

    pending_days = await db.get_bonus_days_pending(user_id)
    carried_days = 0
    if preserve_active_days:
        carried_days = await get_remaining_active_days(user_id, panel, db)

    if days_override is None:
        days = int(plan.get("duration_days", 30) or 30) + extra_days + pending_days + carried_days
    else:
        days = int(days_override) + pending_days
    days = max(days, 1)

    base_email = await panel_base_email(user_id, db)
    stable_sub_id = panel_sub_id(user_id)
    if hasattr(panel, "upsert_client"):
        client = await panel.upsert_client(
            email=base_email,
            limit_ip=int(plan.get("ip_limit", 0) or 0),
            total_gb=int(plan.get("traffic_gb", 0) or 0),
            days=days,
            sub_id=stable_sub_id,
        )
    else:
        client = await panel.create_client(
            email=base_email,
            limit_ip=int(plan.get("ip_limit", 0) or 0),
            total_gb=int(plan.get("traffic_gb", 0) or 0),
            days=days,
            sub_id=stable_sub_id,
        )
    if not client:
        return None

    sub_id = str(client.get("subId") or stable_sub_id)
    client_uuid = str(client.get("id") or client.get("clientId") or "").strip()
    vpn_url = build_primary_subscription_url(client_uuid=client_uuid, sub_id=sub_id)
    if not vpn_url and sub_id:
        vpn_url = f"{Config.SUB_PANEL_BASE}{sub_id}"
    plan_name = plan.get("name") or plan.get("id") or "Тариф"
    if plan_suffix:
        plan_name = f"{plan_name}{plan_suffix}"
    created_inbounds = client.get("created_inbounds") or []

    updated = await db.set_subscription(
        user_id=user_id,
        plan_text=plan_name,
        ip_limit=int(plan.get("ip_limit", 0) or 0),
        traffic_gb=int(plan.get("traffic_gb", 0) or 0),
        vpn_url=vpn_url,
    )
    if not updated:
        logger.error("Subscription save failed | Не удалось записать подписку: user=%s", user_id)
        return None

    if pending_days > 0:
        await db.clear_bonus_days_pending(user_id)
    await db.reset_expiry_notifications(user_id)
    await db.clear_frozen(user_id)
    if created_inbounds:
        logger.info(
            "Subscription created: user=%s sub_id=%s inbounds=%s",
            user_id,
            sub_id,
            ",".join(str(item) for item in created_inbounds),
        )
    return vpn_url


async def is_active_subscription(user_id: int, db: Database, panel: PanelAPI) -> bool:
    user = await db.get_user(user_id)
    if not user:
        return False

    base_email = await panel_base_email(user_id, db)
    clients = await panel.find_clients_by_base_email(base_email)
    if not clients:
        if user.get("vpn_url") or user.get("has_subscription"):
            await db.remove_subscription(user_id)
        return False

    now_ms = int(time.time() * 1000)
    active_clients = [c for c in clients if (c.get("expiryTime", 0) or 0) > now_ms]
    if not active_clients:
        await db.remove_subscription(user_id)
        return False

    if not user.get("has_subscription"):
        await db.set_has_subscription(user_id)
    return True


async def reward_referrer_days(referrer_id: int, bonus_days: int, db: Database, panel: PanelAPI) -> None:
    ref_user = await db.get_user(referrer_id)
    if not ref_user:
        return

    pending = await db.get_bonus_days_pending(referrer_id)
    total_bonus = bonus_days + pending
    base_email = await panel_base_email(referrer_id, db)

    if await is_active_subscription(referrer_id, db=db, panel=panel):
        try:
            success = await panel.extend_client_expiry(base_email, total_bonus)
        except Exception as e:
            logger.error("Referral bonus extend failed: referrer=%s error=%s", referrer_id, e)
            success = False

        if success:
            if pending > 0:
                await db.clear_bonus_days_pending(referrer_id)
            await db.add_ref_history(referrer_id, ref_user_id=0, bonus_days=total_bonus)
            await db.reset_expiry_notifications(referrer_id)
            await notify_user(referrer_id, f"🎉 Вам начислено {total_bonus} дней по реферальной программе!")
            return

        await db.add_bonus_days_pending(referrer_id, bonus_days)
        await notify_admins(
            f"⚠️ Не удалось продлить подписку реферера {referrer_id}. Бонус {bonus_days} дней сохранён в ожидании."
        )
        return

    min_plan = get_minimal_by_price()
    if not min_plan:
        await db.add_bonus_days_pending(referrer_id, bonus_days)
        await notify_admins(
            f"⚠️ Нет доступных тарифов для выдачи бонуса рефереру {referrer_id}. Бонус сохранён."
        )
        return

    vpn_url = await create_subscription(
        referrer_id,
        min_plan,
        db=db,
        panel=panel,
        days_override=total_bonus,
        plan_suffix=" (реферальный бонус)",
    )
    if vpn_url:
        connection_info = render_connection_info(vpn_url, user_id=referrer_id, plan_name=min_plan.get("name"))
        await db.add_ref_history(referrer_id, ref_user_id=0, bonus_days=total_bonus)
        await notify_user(
            referrer_id,
            f"🎉 Вам выдана бесплатная подписка на {total_bonus} дней по реферальной программе!\n\n{connection_info}",
        )
    else:
        await db.add_bonus_days_pending(referrer_id, bonus_days)
        await notify_admins(
            f"⚠️ Не удалось выдать бесплатную подписку рефереру {referrer_id}. Бонус сохранён."
        )


async def reward_referrer_percent(user_id: int, amount: float, db: Database) -> None:
    user = await db.get_user(user_id)
    if not user:
        return

    current_referrer_id = user.get("ref_by")
    if not current_referrer_id:
        return

    visited = {user_id}
    levels = [
        (1, Config.REF_PERCENT_LEVEL1, "за реферала"),
        (2, Config.REF_PERCENT_LEVEL2, "за реферала второго уровня"),
        (3, Config.REF_PERCENT_LEVEL3, "за реферала третьего уровня"),
    ]

    for level, default_percent, label in levels:
        if not current_referrer_id or current_referrer_id in visited:
            break

        partner_settings = await db.get_partner_settings(current_referrer_id) if hasattr(db, "get_partner_settings") else {}
        override_percent = partner_settings.get(f"custom_percent_level{level}") if partner_settings else None
        percent = float(override_percent if override_percent is not None else default_percent)
        payout = round(amount * percent / 100, 2)
        if payout <= 0:
            visited.add(current_referrer_id)
            referrer = await db.get_user(current_referrer_id)
            current_referrer_id = referrer.get("ref_by") if referrer else None
            continue

        if payout > Config.MAX_DAILY_REF_BONUS_RUB:
            payout = Config.MAX_DAILY_REF_BONUS_RUB
        credited = await db.add_balance(current_referrer_id, payout)
        if not credited:
            logger.warning(
                "Referral payout skipped: level=%s referrer=%s user=%s amount=%s",
                level,
                current_referrer_id,
                user_id,
                payout,
            )
        else:
            await db.add_ref_history(current_referrer_id, ref_user_id=user_id, amount=payout)
            await notify_user(current_referrer_id, f"🎉 Вам начислено {payout:.2f} ₽ на баланс {label}!")
            if level == 1 and hasattr(db, "increment_ref_rewarded_count"):
                await db.increment_ref_rewarded_count(current_referrer_id)

        visited.add(current_referrer_id)
        referrer = await db.get_user(current_referrer_id)
        current_referrer_id = referrer.get("ref_by") if referrer else None


async def revoke_subscription(user_id: int, db: Database, panel: PanelAPI, *, reason: str = "") -> bool:
    base_email = await panel_base_email(user_id, db)
    panel_deleted = False
    try:
        panel_deleted = await panel.delete_client(base_email)
    except Exception as exc:
        logger.error("Subscription revoke failed in panel: user=%s error=%s", user_id, exc)

    db_cleared = False
    if hasattr(db, "remove_subscription"):
        db_cleared = await db.remove_subscription(user_id)
    if db_cleared:
        try:
            suffix = f"\nПричина: {reason}" if reason else ""
            await notify_user(user_id, f"↩️ Ваша подписка была деактивирована после возврата платежа.{suffix}")
        except Exception as exc:
            logger.warning("Subscription revoke notify failed: user=%s error=%s", user_id, exc)
    return bool(panel_deleted or db_cleared)
