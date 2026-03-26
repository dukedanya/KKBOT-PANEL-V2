import logging
from typing import Optional

from aiogram import Bot

from config import Config
from db import Database
from utils.helpers import notify_admins

logger = logging.getLogger(__name__)


async def guard_payment_creation(user_id: int, db: Database, bot: Optional[Bot] = None) -> tuple[bool, str]:
    recent_count = await db.count_user_payments_created_since(user_id, Config.PAYMENT_CREATE_COOLDOWN_SEC)
    if Config.PAYMENT_CREATE_COOLDOWN_SEC and recent_count > 0:
        await db.add_antifraud_event(user_id, "payment_cooldown", details=f"recent_count={recent_count}")
        return False, f"Создание платежей слишком частое. Подождите {Config.PAYMENT_CREATE_COOLDOWN_SEC} сек."

    pending_count = await db.count_user_pending_payments(user_id)
    if pending_count >= Config.MAX_PENDING_PAYMENTS_PER_USER:
        await db.add_antifraud_event(user_id, "too_many_pending_payments", details=f"pending_count={pending_count}", severity="high")
        if pending_count >= Config.MAX_PENDING_PAYMENTS_PER_USER + 2:
            await notify_admins(
                f"⚠️ Suspicious activity\n\nuser_id: <code>{user_id}</code>\nслишком много pending payments: {pending_count}",
                bot=bot,
            )
        return False, "Слишком много незавершённых платежей. Завершите или дождитесь обработки текущих."

    return True, ""


async def note_trial_abuse(user_id: int, db: Database, reason: str, bot: Optional[Bot] = None) -> None:
    await db.add_antifraud_event(user_id, "trial_abuse", details=reason, severity="high")
    count = await db.count_antifraud_events(user_id, "trial_abuse", since_hours=24)
    if count >= 2:
        await notify_admins(
            f"⚠️ Suspicious activity\n\nuser_id: <code>{user_id}</code>\nподозрение на abuse пробника\nПричина: {reason}",
            bot=bot,
        )


async def evaluate_referral_link(user_id: int, referrer_id: int, db: Database, bot: Optional[Bot] = None) -> tuple[bool, str]:
    if user_id == referrer_id:
        await db.add_antifraud_event(user_id, "self_referral_attempt", details="self-referral blocked", severity="high")
        return False, "self_referral"

    referrer = await db.get_user(referrer_id)
    referrer_note = str((referrer or {}).get("partner_note") or "").strip().lower()
    hard_suspicious_referrer = (
        bool(referrer)
        and int(referrer.get("ref_suspicious", 0) or 0) == 1
        and "self-referral" not in referrer_note
    )
    if hard_suspicious_referrer:
        await db.add_antifraud_event(
            user_id,
            "referral_from_suspicious_referrer",
            details=f"referrer_id={referrer_id}",
            severity="warning",
        )
        if hasattr(db, "mark_referral_suspicious"):
            await db.mark_referral_suspicious(user_id, True, f"Реферал от подозрительного пользователя {referrer_id}")
        return False, "referrer_suspicious"

    if hasattr(db, "count_recent_referrals_by_referrer"):
        recent_refs = await db.count_recent_referrals_by_referrer(referrer_id, since_hours=24)
    else:
        recent_refs = 0
    threshold = max(1, int(getattr(Config, "REFERRAL_MAX_NEW_INVITES_24H", 30) or 30))
    if recent_refs >= threshold:
        await db.add_antifraud_event(
            referrer_id,
            "referral_invite_spike",
            details=f"recent_refs_24h={recent_refs};threshold={threshold}",
            severity="high",
        )
        if hasattr(db, "mark_referral_suspicious"):
            await db.mark_referral_suspicious(referrer_id, True, f"Всплеск рефералов: {recent_refs}/24h")
            await db.mark_referral_suspicious(user_id, True, f"Привязка к рефереру {referrer_id} во время всплеска")
        await notify_admins(
            "⚠️ <b>Антифрод: всплеск реферальных привязок</b>\n\n"
            f"Реферер: <code>{referrer_id}</code>\n"
            f"Новых рефералов за 24ч: <b>{recent_refs}</b>\n"
            f"Порог: <b>{threshold}</b>\n"
            f"Новый пользователь: <code>{user_id}</code>",
            bot=bot,
        )
        return False, "invite_spike"

    return True, ""
