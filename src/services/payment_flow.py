import logging
from typing import Any, Dict, Optional

from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import Config
from db import Database
from kkbot.services.subscriptions import (
    create_subscription,
    get_subscription_status as get_runtime_subscription_status,
    get_remaining_active_days,
    reward_referrer_percent,
)
from services.antifraud import evaluate_referral_link
from services.panel import PanelAPI
from tariffs import format_duration, get_by_id
from utils.helpers import notify_admins, notify_user
from utils.subscription_links import render_connection_info
from utils.telegram_ui import smart_edit_by_ids
from utils.templates import render_template

logger = logging.getLogger(__name__)


def _pending_ref_key(user_id: int) -> str:
    return f"ref:pending:{int(user_id)}"


async def _increment_setting_counter(db: Database, key: str, delta: int = 1) -> None:
    if not hasattr(db, "get_setting") or not hasattr(db, "set_setting"):
        return
    raw = await db.get_setting(key, "0")
    try:
        current = int(str(raw or "0").strip())
    except ValueError:
        current = 0
    await db.set_setting(key, str(current + int(delta)))


async def _setting_is_recent(db: Database, key: str, *, window_hours: int) -> bool:
    if not hasattr(db, "get_setting"):
        return False
    raw = str(await db.get_setting(key, "") or "").strip()
    if not raw:
        return False
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds()
    return age_seconds <= max(1, int(window_hours)) * 3600


async def _mark_funnel_conversion(*, db: Database, payment_id: str, user_id: int) -> None:
    conversion_key = f"analytics:funnel:conversion:payment:{payment_id}"
    if hasattr(db, "get_setting"):
        already = str(await db.get_setting(conversion_key, "") or "").strip()
        if already:
            return

    user_id = int(user_id)
    attributed = ""
    attribution_window_hours = int(getattr(Config, "FUNNEL_ATTRIBUTION_WINDOW_HOURS", 72) or 72)
    if hasattr(db, "get_setting"):
        if await _setting_is_recent(db, f"marketing:trial_followup:{user_id}", window_hours=attribution_window_hours):
            attributed = "trial_followup"
        if not attributed:
            if await _setting_is_recent(db, f"marketing:inactive_reactivation:{user_id}:trial_offer", window_hours=attribution_window_hours):
                attributed = "trial_promo_offer"
        if not attributed:
            if await _setting_is_recent(db, f"marketing:expired_reactivation:{user_id}", window_hours=attribution_window_hours):
                attributed = "trial_or_expired"
        if not attributed:
            for stage in ("20m", "12h", "24h"):
                if await _setting_is_recent(db, f"payments:abandoned_reminder:{payment_id}:{stage}", window_hours=attribution_window_hours):
                    attributed = f"abandoned_payment_{stage}"
                    break
        if not attributed:
            for stage in ("12h", "3d", "7d"):
                if await _setting_is_recent(db, f"marketing:inactive_reactivation:{user_id}:{stage}", window_hours=attribution_window_hours):
                    attributed = f"reactivation_{stage}"
                    break
        if not attributed:
            if await _setting_is_recent(db, f"funnel:start_prompt:{user_id}", window_hours=attribution_window_hours):
                attributed = "start_prompt"

    if not attributed:
        attributed = "organic"

    await _increment_setting_counter(db, f"analytics:funnel:conversion:{attributed}")
    if hasattr(db, "set_setting"):
        await db.set_setting(conversion_key, attributed)


async def _call_db_method(db, method_name: str, *args, **kwargs):
    method = getattr(db, method_name)
    try:
        return await method(*args, **kwargs)
    except TypeError:
        return await method(*args)


async def _payment_has_live_access(user_id: int, db: Database, panel: PanelAPI) -> bool:
    try:
        status = await get_runtime_subscription_status(user_id, db=db, panel=panel)
    except Exception as exc:
        logger.warning("subscription status check failed during payment recovery user=%s error=%s", user_id, exc)
        return False
    return bool(status.get("active")) and bool((status.get("user") or {}).get("vpn_url"))


async def _maybe_restore_pending_referrer_for_payment(
    *,
    user_id: int,
    user_data: Optional[Dict[str, Any]],
    db: Database,
    bot=None,
) -> Optional[Dict[str, Any]]:
    if (user_data or {}).get("ref_by"):
        return user_data
    if not hasattr(db, "get_setting"):
        return user_data

    raw = str(await db.get_setting(_pending_ref_key(user_id), "") or "").strip()
    try:
        referrer_id = int(raw)
    except Exception:
        referrer_id = 0
    if referrer_id <= 0:
        return user_data

    is_allowed, reason = await evaluate_referral_link(user_id, referrer_id, db=db, bot=bot)
    if not is_allowed:
        logger.warning(
            "pending referral blocked during payment activation user=%s referrer=%s reason=%s",
            user_id,
            referrer_id,
            reason,
        )
        return user_data

    try:
        await db.set_ref_by(user_id, referrer_id)
        await db.set_setting(_pending_ref_key(user_id), "")
    except Exception as exc:
        logger.warning(
            "pending referral restore failed during payment activation user=%s referrer=%s error=%s",
            user_id,
            referrer_id,
            exc,
        )
        return user_data

    restored = await db.get_user(user_id)
    logger.info("pending referral restored during payment activation user=%s referrer=%s", user_id, referrer_id)
    return restored or user_data


def _compute_retry_delay_sec(attempt: int) -> int:
    base = max(5, int(getattr(Config, "PAYMENT_ACTIVATION_RETRY_BASE_SEC", 60) or 60))
    cap = max(base, int(getattr(Config, "PAYMENT_ACTIVATION_RETRY_MAX_SEC", 1800) or 1800))
    safe_attempt = max(1, int(attempt or 1))
    return min(cap, base * (2 ** (safe_attempt - 1)))


def main_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]]
    )


def post_payment_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📱 Подключиться", callback_data="onboarding:start")],
            [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="user_menu:profile")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")],
        ]
    )


def support_inline() -> InlineKeyboardMarkup:
    support_url = (Config.SUPPORT_URL or "").strip()
    rows: list[list[InlineKeyboardButton]] = []
    if support_url:
        rows.append([InlineKeyboardButton(text="Поддержка", url=support_url)])
    else:
        rows.append([InlineKeyboardButton(text="Поддержка", callback_data="support:start")])
    rows.append([InlineKeyboardButton(text="Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_gift_claim_link(*, token: str, bot=None) -> str:
    username = (getattr(Config, "BOT_PUBLIC_USERNAME", "") or "").strip()
    if not username and bot:
        username = getattr(bot, "username", "") or ""
    if username:
        return f"https://t.me/{username}?start=gift_{token}"
    return f"https://t.me/?start=gift_{token}"


async def resolve_bonus_days_for_user(user_data: Optional[Dict[str, Any]], db: Database) -> int:
    if not user_data:
        return 0
    ref_by = user_data.get("ref_by")
    ref_rewarded = user_data.get("ref_rewarded")
    if not ref_by or ref_rewarded:
        return 0

    return int(getattr(Config, 'REFERRED_BONUS_DAYS', 5) or 0)


async def apply_referral_reward(user_id: int, amount: float, user_data: Optional[Dict[str, Any]], db: Database, panel: PanelAPI) -> None:
    if not user_data:
        return

    ref_by = user_data.get("ref_by")
    ref_rewarded = user_data.get("ref_rewarded")
    if not ref_by or ref_rewarded:
        return

    await reward_referrer_percent(user_id, amount, db=db)
    await db.mark_ref_rewarded(user_id)


async def process_successful_payment(
    *,
    payment: Dict[str, Any],
    db: Database,
    panel: PanelAPI,
    bot=None,
    admin_context: Optional[str] = None,
    apply_referral: bool = True,
) -> Dict[str, Any]:
    payment_id = payment["payment_id"]
    buyer_user_id = int(payment["user_id"])
    recipient_user_id = int(payment.get("recipient_user_id") or buyer_user_id)
    plan_id = payment["plan_id"]
    amount = float(payment.get("amount", 0) or 0)
    promo_code = str(payment.get("promo_code") or "").strip().upper()
    promo_discount_percent = float(payment.get("promo_discount_percent") or 0.0)
    gift_label = str(payment.get("gift_label") or "").strip()
    gift_note = str(payment.get("gift_note") or "").strip()
    gift_link_token = gift_label.split(":", 1)[1] if gift_label.startswith("giftlink:") else ""
    plan = get_by_id(plan_id)

    if not plan:
        logger.error("Payment activation failed: plan=%s not found for payment=%s", plan_id, payment_id)
        return {"ok": False, "reason": "plan_not_found", "payment_id": payment_id, "user_id": recipient_user_id}

    current_payment = await db.get_pending_payment(payment_id)
    if current_payment:
        current_status = current_payment.get("status")
        if current_status == "accepted":
            if await _payment_has_live_access(recipient_user_id, db, panel):
                logger.info("Payment already accepted: %s", payment_id)
                return {"ok": True, "already_processed": True, "payment_id": payment_id, "user_id": recipient_user_id, "plan": plan}
            logger.warning("Accepted payment without active access, continuing recovery: %s", payment_id)
        if current_status == "rejected":
            logger.warning("Payment already rejected: %s", payment_id)
            return {"ok": False, "reason": "already_rejected", "payment_id": payment_id, "user_id": recipient_user_id, "plan": plan}
        if current_status == "processing":
            logger.info("Payment already processing: %s", payment_id)
            return {"ok": False, "reason": "already_processing", "payment_id": payment_id, "user_id": recipient_user_id, "plan": plan}

    claimed = await _call_db_method(db, "claim_pending_payment",
        payment_id,
        source=admin_context or "payment_flow/success",
        reason="attempt activation",
        metadata=f"plan_id={plan_id};amount={amount};buyer={buyer_user_id};recipient={recipient_user_id};promo={promo_code}",
    )
    if not claimed:
        refreshed = await db.get_pending_payment(payment_id)
        refreshed_status = (refreshed or {}).get("status")
        logger.warning("Payment claim failed: payment=%s current_status=%s", payment_id, refreshed_status)
        if refreshed_status == "accepted":
            if await _payment_has_live_access(recipient_user_id, db, panel):
                return {"ok": True, "already_processed": True, "payment_id": payment_id, "user_id": recipient_user_id, "plan": plan}
            logger.warning("Accepted payment without active access after claim failure, continuing recovery: %s", payment_id)
        if refreshed_status == "processing":
            return {"ok": False, "reason": "already_processing", "payment_id": payment_id, "user_id": recipient_user_id, "plan": plan}
        return {"ok": False, "reason": "claim_failed", "payment_id": payment_id, "user_id": recipient_user_id, "plan": plan}

    claimed_payment = await db.get_pending_payment(payment_id)
    activation_attempt = int((claimed_payment or {}).get("activation_attempts") or 1)
    max_attempts = max(1, int(getattr(Config, "PAYMENT_ACTIVATION_MAX_ATTEMPTS", 5) or 5))
    if activation_attempt > max_attempts:
        await db.mark_payment_error(payment_id, "activation_attempts_exceeded")
        await _call_db_method(
            db,
            "update_payment_status",
            payment_id,
            "rejected",
            allowed_current_statuses=["processing"],
            source=admin_context or "payment_flow/success",
            reason="activation_attempts_exceeded",
            metadata=f"attempt={activation_attempt};max_attempts={max_attempts}",
        )
        return {
            "ok": False,
            "reason": "activation_attempts_exceeded",
            "payment_id": payment_id,
            "user_id": recipient_user_id,
            "plan": plan,
            "attempt": activation_attempt,
            "max_attempts": max_attempts,
        }

    user_data = await db.get_user(recipient_user_id)
    user_data = await _maybe_restore_pending_referrer_for_payment(
        user_id=recipient_user_id,
        user_data=user_data,
        db=db,
        bot=bot,
    )
    bonus_days_for_user = 0
    carried_days = 0
    pending_bonus_days = 0
    vpn_url = ""

    if gift_link_token:
        gift_created = await _call_db_method(
            db,
            "create_gift_link",
            token=gift_link_token,
            buyer_user_id=buyer_user_id,
            plan_id=plan_id,
            payment_id=payment_id,
            promo_code=promo_code,
            promo_discount_percent=promo_discount_percent,
            note=gift_note,
        )
        if not gift_created:
            await db.mark_payment_error(payment_id, "gift_link_create_failed")
            await _call_db_method(
                db,
                "release_processing_payment",
                payment_id,
                error_text="gift_link_create_failed",
                source=admin_context or "payment_flow/success",
                metadata=f"plan_id={plan_id};buyer={buyer_user_id};token={gift_link_token}",
                retry_delay_sec=_compute_retry_delay_sec(activation_attempt),
            )
            return {
                "ok": False,
                "reason": "gift_link_create_failed",
                "payment_id": payment_id,
                "user_id": buyer_user_id,
                "plan": plan,
            }
    else:
        if buyer_user_id != recipient_user_id and not (user_data or {}).get("ref_by"):
            is_allowed, reason = await evaluate_referral_link(recipient_user_id, buyer_user_id, db=db, bot=bot)
            if is_allowed:
                await db.set_ref_by(recipient_user_id, buyer_user_id)
                await db.update_user(recipient_user_id, ref_origin="gift_purchase")
                user_data = await db.get_user(recipient_user_id)
            else:
                logger.warning(
                    "gift referral blocked recipient=%s buyer=%s reason=%s",
                    recipient_user_id,
                    buyer_user_id,
                    reason,
                )
        bonus_days_for_user = await resolve_bonus_days_for_user(user_data, db)
        carried_days = await get_remaining_active_days(recipient_user_id, panel, db)
        pending_bonus_days = await db.get_bonus_days_pending(recipient_user_id)
        vpn_url = await create_subscription(
            recipient_user_id,
            plan,
            db=db,
            panel=panel,
            extra_days=bonus_days_for_user,
            preserve_active_days=True,
        )
        if not vpn_url:
            retry_delay_sec = _compute_retry_delay_sec(activation_attempt)
            await db.mark_payment_error(payment_id, "subscription_create_failed")
            await _call_db_method(db, "release_processing_payment",
                payment_id,
                error_text="subscription_create_failed",
                source=admin_context or "payment_flow/success",
                metadata=f"plan_id={plan_id};user_id={recipient_user_id};attempt={activation_attempt};retry_delay_sec={retry_delay_sec}",
                retry_delay_sec=retry_delay_sec,
            )
            logger.error("Subscription create failed | Не удалось выдать VPN: user=%s plan=%s", recipient_user_id, plan_id)
            return {
                "ok": False,
                "reason": "subscription_create_failed",
                "payment_id": payment_id,
                "user_id": recipient_user_id,
                "plan": plan,
                "attempt": activation_attempt,
                "retry_delay_sec": retry_delay_sec,
            }

        if apply_referral:
            try:
                await apply_referral_reward(recipient_user_id, amount, user_data, db, panel)
            except Exception as ref_error:
                logger.error("Referral reward failed: payment=%s user=%s error=%s", payment_id, recipient_user_id, ref_error)
                await db.mark_payment_error(payment_id, f"referral_reward_failed: {ref_error}")

    if promo_code:
        await db.mark_promo_code_used(promo_code, user_id=buyer_user_id)
        await db.clear_active_user_promo_code(buyer_user_id)

    status_updated = await _call_db_method(db, "update_payment_status",
        payment_id,
        "accepted",
        allowed_current_statuses=["processing"],
        source=admin_context or "payment_flow/success",
        reason="gift link created" if gift_link_token else "subscription activated",
        metadata=f"plan_id={plan_id};vpn_url={vpn_url};buyer={buyer_user_id};recipient={recipient_user_id};promo={promo_code};discount={promo_discount_percent};gift_link={gift_link_token}",
    )
    if not status_updated:
        logger.warning("Payment status not updated to accepted: %s", payment_id)

    result = {
        "ok": True,
        "payment_id": payment_id,
        "user_id": recipient_user_id,
        "buyer_user_id": buyer_user_id,
        "plan_id": plan_id,
        "plan": plan,
        "amount": amount,
        "vpn_url": vpn_url,
        "bonus_days_for_user": bonus_days_for_user,
        "carried_days": carried_days,
        "pending_bonus_days": pending_bonus_days,
        "msg_id": payment.get("msg_id"),
        "promo_code": promo_code,
        "promo_discount_percent": promo_discount_percent,
        "gift_label": gift_label,
        "gift_note": gift_note,
        "gift_link_token": gift_link_token,
    }

    try:
        await _mark_funnel_conversion(db=db, payment_id=payment_id, user_id=recipient_user_id)
    except Exception as analytics_error:
        logger.warning("Funnel conversion attribution failed: payment=%s user=%s error=%s", payment_id, recipient_user_id, analytics_error)

    if bot:
        connection_info = render_connection_info(vpn_url, user_id=recipient_user_id, plan_name=plan.get("name", plan_id)) if vpn_url else ""
        if gift_link_token:
            gift_link = _build_gift_claim_link(token=gift_link_token, bot=bot)
            notify_text = (
                "🎁 <b>Подарок оплачен</b>\n\n"
                f"Тариф: <b>{plan.get('name', plan_id)}</b>\n"
                f"Сумма: <b>{amount:.2f} ₽</b>\n\n"
                "Отправьте получателю ссылку ниже. После перехода по ней бот активирует подписку автоматически.\n\n"
                f"<code>{gift_link}</code>"
            )
            if gift_note:
                notify_text += f"\n\n✍️ Подпись: <i>{gift_note}</i>"
            if promo_code:
                notify_text += f"\n\n🏷 Промокод: <b>{promo_code}</b>"
            buyer_markup = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🔗 Открыть ссылку", url=gift_link)],
                    [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")],
                ]
            )
        else:
            notify_text = (
                "✅ <b>Оплата прошла успешно</b>\n\n"
                "Подписка уже активирована.\n\n"
                f"📦 Тариф: <b>{plan.get('name', plan_id)}</b>\n"
                f"📱 Устройств: <b>до {int(plan.get('ip_limit', 0) or 0)}</b>\n"
                f"⏳ Срок: <b>{format_duration(int(plan.get('duration_days', 30)) + bonus_days_for_user)}</b>\n"
            )
            if connection_info:
                notify_text += f"\n{connection_info}"
            if promo_code:
                if promo_discount_percent > 0:
                    notify_text += f"\n\n🏷 Промокод: <b>{promo_code}</b> (-{promo_discount_percent:.0f}%)"
                else:
                    notify_text += f"\n\n🏷 Промокод: <b>{promo_code}</b>"
            if buyer_user_id != recipient_user_id:
                notify_text += f"\n🎁 Подарок: <b>{gift_label or f'пользователю {recipient_user_id}'}</b>"
            notify_text += (
                "\n\nЧто дальше:\n"
                "1. Нажмите «Как подключиться?»\n"
                "2. Выберите своё устройство\n"
                "3. Установите клиент и добавьте подписку"
            )
            buyer_markup = post_payment_inline()
        try:
            msg_id = payment.get("msg_id")
            if msg_id:
                await smart_edit_by_ids(
                    bot,
                    chat_id=buyer_user_id,
                    message_id=msg_id,
                    text=notify_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=buyer_markup,
                )
            else:
                await bot.send_message(
                    buyer_user_id,
                    notify_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=buyer_markup,
                )
        except Exception as e:
            logger.warning("Buyer notify failed: user=%s error=%s", buyer_user_id, e)
            try:
                await notify_user(buyer_user_id, notify_text, reply_markup=buyer_markup, bot=bot)
            except Exception as notify_error:
                logger.warning("Buyer fallback notify failed: user=%s error=%s", buyer_user_id, notify_error)

        if not gift_link_token and buyer_user_id != recipient_user_id:
            recipient_text = (
                "🎁 <b>Вам подарили подписку VPN</b>\n\n"
                "Доступ уже активирован.\n\n"
                f"📦 Тариф: <b>{plan.get('name', plan_id)}</b>\n"
                f"📱 Устройств: <b>до {int(plan.get('ip_limit', 0) or 0)}</b>\n"
                f"⏳ Срок: <b>{format_duration(int(plan.get('duration_days', 30)) + bonus_days_for_user)}</b>\n"
            )
            if connection_info:
                recipient_text += f"\n{connection_info}"
            recipient_text += (
                "\n\nЧто дальше:\n"
                "1. Нажмите «Подключиться»\n"
                "2. Выберите своё устройство\n"
                "3. Установите клиент и добавьте подписку"
            )
            try:
                await notify_user(recipient_user_id, recipient_text, reply_markup=post_payment_inline(), bot=bot)
            except Exception as recipient_notify_error:
                logger.warning("Recipient notify failed: %s", recipient_notify_error)

        if not gift_link_token and bonus_days_for_user > 0:
            try:
                bonus_text, _ = await render_template(db, "referral_bonus_user", bonus_days=bonus_days_for_user)
                await notify_user(
                    recipient_user_id,
                    bonus_text,
                    bot=bot,
                )
            except Exception as e:
                logger.warning("Bonus notify failed: user=%s error=%s", recipient_user_id, e)

    context_line = f"\n📍 {admin_context}" if admin_context else ""
    if gift_link_token:
        await notify_admins(
            f"✅ <b>Оплата подарка подтверждена</b>\n"
            f"🧾 Покупатель: <code>{buyer_user_id}</code>\n"
            f"📦 {plan.get('name', plan_id)}\n"
            f"💰 {amount} ₽\n"
            f"🔗 Токен подарка: <code>{gift_link_token}</code>{context_line}"
        )
    else:
        await notify_admins(
            f"✅ <b>Оплата подтверждена</b>\n"
            f"👤 Получатель: <code>{recipient_user_id}</code>\n"
            f"🧾 Покупатель: <code>{buyer_user_id}</code>\n"
            f"📦 {plan.get('name', plan_id)}\n"
            f"💰 {amount} ₽{context_line}"
        )
    return result


async def reject_pending_payment(
    *,
    payment: Dict[str, Any],
    db: Database,
    bot=None,
    reason_text: Optional[str] = None,
    admin_context: Optional[str] = None,
) -> Dict[str, Any]:
    payment_id = payment["payment_id"]
    user_id = int(payment["user_id"])

    current_payment = await db.get_pending_payment(payment_id)
    if current_payment:
        current_status = current_payment.get("status")
        if current_status == "rejected":
            logger.info("Payment already rejected: %s", payment_id)
            return {"ok": True, "already_processed": True, "payment_id": payment_id, "user_id": user_id}
        if current_status == "accepted":
            logger.warning("Reject skipped: payment already accepted: %s", payment_id)
            return {"ok": False, "reason": "already_accepted", "payment_id": payment_id, "user_id": user_id}
        if current_status == "processing":
            logger.info("Reject skipped: payment already processing: %s", payment_id)
            return {"ok": False, "reason": "already_processing", "payment_id": payment_id, "user_id": user_id}

    claimed = await _call_db_method(db, "claim_pending_payment",
        payment_id,
        source=admin_context or "payment_flow/reject",
        reason=reason_text or "reject payment",
        metadata=f"user_id={user_id}",
    )
    if not claimed:
        refreshed = await db.get_pending_payment(payment_id)
        refreshed_status = (refreshed or {}).get("status")
        logger.warning("Reject claim failed: payment=%s current_status=%s", payment_id, refreshed_status)
        if refreshed_status == "rejected":
            return {"ok": True, "already_processed": True, "payment_id": payment_id, "user_id": user_id}
        if refreshed_status == "processing":
            return {"ok": False, "reason": "already_processing", "payment_id": payment_id, "user_id": user_id}
        return {"ok": False, "reason": "claim_failed", "payment_id": payment_id, "user_id": user_id}

    status_updated = await _call_db_method(db, "update_payment_status",
        payment_id,
        "rejected",
        allowed_current_statuses=["processing"],
        source=admin_context or "payment_flow/reject",
        reason=reason_text or "payment rejected",
        metadata=f"user_id={user_id}",
    )
    if not status_updated:
        logger.warning("Payment status not updated to rejected: %s", payment_id)

    if bot:
        default_rejected_text, _ = await render_template(db, "payment_rejected_user")
        text = reason_text or default_rejected_text
        try:
            msg_id = payment.get("msg_id")
            if msg_id:
                await smart_edit_by_ids(
                    bot,
                    chat_id=user_id,
                    message_id=msg_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=support_inline(),
                )
            else:
                await bot.send_message(
                    user_id,
                    text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=support_inline(),
                )
        except Exception as e:
            logger.warning("Reject notify failed: user=%s error=%s", user_id, e)
            try:
                await notify_user(user_id, text, reply_markup=support_inline(), bot=bot)
            except Exception as notify_error:
                logger.warning("Reject fallback notify failed: user=%s error=%s", user_id, notify_error)

    context_line = f"\n📍 {admin_context}" if admin_context else ""
    await notify_admins(
        f"❌ <b>Оплата отклонена</b>\n👤 <code>{user_id}</code>\n💳 <code>{payment_id}</code>{context_line}"
    )
    return {"ok": True, "payment_id": payment_id, "user_id": user_id}
