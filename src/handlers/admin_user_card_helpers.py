from __future__ import annotations

import logging
from datetime import datetime
from html import escape
from typing import Any, Dict, List, Optional

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import Config
from db import Database
from kkbot.services.subscriptions import get_subscription_status as get_runtime_subscription_status
from tariffs import get_all_active, get_by_id
from utils.helpers import notify_admins
from utils.support import format_support_restriction_reason, format_support_status

logger = logging.getLogger(__name__)


def _format_dt(value: Any) -> str:
    if not value:
        return "-"
    if isinstance(value, datetime):
        return value.strftime("%d.%m.%Y %H:%M")
    text = str(value).strip()
    if not text:
        return "-"
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return text


def _format_bool_badge(value: object) -> str:
    return "да" if bool(value) else "нет"


def _trim_text(value: str, limit: int = 80) -> str:
    text = " ".join((value or "").split())
    if len(text) <= limit:
        return text or "—"
    return text[: limit - 1].rstrip() + "…"


def _admin_user_card_url(user_id: int) -> str:
    username = str(getattr(Config, "BOT_PUBLIC_USERNAME", "") or "").strip()
    if not username:
        return ""
    return f"https://t.me/{username}?start=admincard_{int(user_id)}"


def _admin_user_id_html(user_id: int, *, label: str | None = None) -> str:
    safe_label = escape(label or str(int(user_id)))
    if int(user_id) <= 0:
        return f"<code>{safe_label}</code>"
    url = _admin_user_card_url(user_id)
    if not url:
        return f"<code>{safe_label}</code>"
    return f"<a href=\"{url}\">{safe_label}</a>"


def _format_user_timeline(items: List[Dict[str, Any]]) -> str:
    if not items:
        return "—"
    lines = []
    for item in items[:25]:
        details = _trim_text(str(item.get("details") or "-"), 80)
        lines.append(
            f"• <code>{item.get('created_at') or '-'}</code> — <b>{escape(str(item.get('event_type') or '-'))}</b> — <code>{escape(details)}</code>"
        )
    return "\n".join(lines)


async def _build_user_card_text(
    db: Database,
    user_id: int,
    *,
    panel=None,
    display_name_override: str | None = None,
) -> str:
    payload = await db.get_user_card(user_id) if hasattr(db, "get_user_card") else {}
    if not payload:
        return f"👤 <b>Карточка пользователя | Какой-то VPN бот</b>\n\nПользователь <code>{user_id}</code> не найден."
    user = payload.get("user") or {}
    referral = payload.get("referral_summary") or {}
    partner = payload.get("partner_settings") or {}
    support_tickets = payload.get("support_tickets") or []
    support_restriction = payload.get("support_restriction") or {}
    payments = payload.get("payments") or []
    withdraws = payload.get("withdraws") or []
    adjustments = payload.get("adjustments") or []
    support_text = "\n".join(
        f"• <code>#{item.get('id')}</code> — <b>{format_support_status(str(item.get('status') or ''), lowercase=True)}</b> — {item.get('updated_at') or '-'}"
        for item in support_tickets[:4]
    ) or "—"
    payments_text = "\n".join(
        f"• <code>{item.get('payment_id')}</code> — <code>{item.get('status') or '-'}</code> — <b>{float(item.get('amount') or 0):.2f} ₽</b>"
        for item in payments[:4]
    ) or "—"
    withdraws_text = "\n".join(
        f"• <code>#{item.get('id')}</code> — <code>{item.get('status') or '-'}</code> — <b>{float(item.get('amount') or 0):.2f} ₽</b>"
        for item in withdraws[:4]
    ) or "—"
    adjustments_text = "\n".join(
        f"• <b>{float(item.get('amount') or 0):.2f} ₽</b> — {_trim_text(str(item.get('reason') or 'без причины'), 45)}"
        for item in adjustments[:4]
    ) or "—"
    live_has_subscription = bool(user.get("has_subscription"))
    live_vpn_url = str(user.get("vpn_url") or "")
    live_plan_text = str(user.get("plan_text") or "—")
    live_expiry = str(user.get("expiry") or "-")
    frozen_until = user.get("frozen_until")
    if panel is not None:
        try:
            status = await get_runtime_subscription_status(user_id, db=db, panel=panel)
            runtime_user = status.get("user") or {}
            runtime_expiry = status.get("expiry_dt")
            live_has_subscription = bool(status.get("active"))
            live_vpn_url = str(runtime_user.get("vpn_url") or live_vpn_url)
            live_plan_text = str(runtime_user.get("plan_text") or live_plan_text or "—")
            if runtime_expiry:
                live_expiry = _format_dt(runtime_expiry)
            frozen_until = status.get("frozen_until") or frozen_until
        except Exception as exc:
            logger.warning("user card runtime status failed user=%s error=%s", user_id, exc)
    status_label = "активна" if live_has_subscription else "не активна"
    if frozen_until:
        status_label = "заморожена"
    plan_label = live_plan_text
    ref_by = int(user.get("ref_by") or 0)
    source_label = f"ref {ref_by}" if ref_by > 0 else "прямой вход"
    last_payment_line = "—"
    if payments:
        latest_payment = payments[0]
        payment_amount = float(latest_payment.get("amount") or 0)
        payment_status = str(latest_payment.get("status") or "-")
        last_payment_line = (
            f"<code>{latest_payment.get('payment_id') or '-'}</code> — "
            f"<b>{payment_amount:.2f} ₽</b> — <code>{payment_status}</code>"
        )
    username = str(user.get("username") or "").strip()
    first_name = str(user.get("first_name") or "").strip()
    display_parts: List[str] = []
    if username:
        display_parts.append(f"@{username}")
    if first_name:
        display_parts.append(first_name)
    display_name = display_name_override or (" | ".join(display_parts) if display_parts else "—")
    return (
        "👤 <b>Карточка пользователя | Какой-то VPN бот</b>\n\n"
        f"ID: <code>{user_id}</code> • <a href=\"tg://user?id={user_id}\">Открыть чат</a>\n"
        f"Имя: <code>{escape(display_name)}</code>\n"
        f"Дата входа: <code>{user.get('join_date') or '-'}</code>\n"
        f"Статус: <b>{status_label}</b>\n"
        f"Тариф: <b>{escape(plan_label)}</b>\n"
        f"Источник: <code>{source_label}</code>\n"
        f"Последний платёж: {last_payment_line}\n"
        f"Подписка активна: <b>{_format_bool_badge(live_has_subscription)}</b>\n"
        f"VPN URL есть: <b>{_format_bool_badge(live_vpn_url)}</b>\n"
        f"Истекает: <code>{live_expiry}</code>\n"
        f"Заморожено до: <code>{frozen_until or '-'}</code>\n"
        f"Баланс: <b>{float(user.get('balance') or 0):.2f} ₽</b>\n"
        f"Пробный период использован: <b>{_format_bool_badge(user.get('trial_used'))}</b>\n"
        f"Пришёл от ref: {_admin_user_id_html(int(user.get('ref_by') or 0), label=str(int(user.get('ref_by') or 0)))}\n"
        f"Реф. код: <code>{user.get('ref_code') or '-'}</code>\n\n"
        "<b>Ограничения</b>\n"
        f"🧱 Общий бан: <b>{_format_bool_badge(user.get('banned'))}</b>\n"
        f"🚫 Причина бана: <code>{_trim_text(str(user.get('ban_reason') or '-'), 60)}</code>\n"
        f"🆘 Поддержка ограничена: <b>{_format_bool_badge(support_restriction.get('active'))}</b>\n"
        f"⏳ До: <code>{support_restriction.get('expires_at') or '-'}</code>\n"
        f"📝 Причина: <code>{_trim_text(format_support_restriction_reason(str(support_restriction.get('reason') or '-')), 60)}</code>\n\n"
        "<b>Рефералка</b>\n"
        f"👥 Всего рефералов: <b>{int(referral.get('total_refs', 0) or 0)}</b>\n"
        f"💸 Оплативших: <b>{int(referral.get('paid_refs', 0) or 0)}</b>\n"
        f"💰 Заработано: <b>{float(referral.get('earned_rub', 0.0) or 0.0):.2f} ₽</b>\n"
        f"🏷 Статус партнёра: <b>{partner.get('status') or 'standard'}</b>\n"
        f"📝 Заметка: <code>{_trim_text(str(partner.get('note') or '-'), 60)}</code>\n\n"
        f"<b>Поддержка</b>\n{support_text}\n\n"
        f"<b>Платежи</b>\n{payments_text}\n\n"
        f"<b>Выводы</b>\n{withdraws_text}\n\n"
        f"<b>Корректировки</b>\n{adjustments_text}"
    )


async def _build_support_restrictions_list_text(db: Database) -> str:
    rows = await db.list_support_restricted_users(limit=20) if hasattr(db, "list_support_restricted_users") else []
    notify_enabled = await db.support_restriction_notifications_enabled() if hasattr(db, "support_restriction_notifications_enabled") else True
    lines = [
        "🆘 <b>Ограничения поддержки</b>",
        "",
        f"Уведомления админам: <b>{'включены' if notify_enabled else 'выключены'}</b>",
        "",
    ]
    if not rows:
        lines.append("Активных ограничений сейчас нет.")
    else:
        lines.append("Активные ограничения:")
        for row in rows:
            lines.append(
                f"\n• user {_admin_user_id_html(int(row.get('user_id') or 0), label=str(int(row.get('user_id') or 0)))} до <code>{row.get('expires_at') or '-'}</code>"
                f"\n  {escape(format_support_restriction_reason(str(row.get('reason') or '-')))}"
            )
    return "\n".join(lines)


async def _build_user_timeline_text(db: Database, user_id: int) -> str:
    items = await db.get_user_timeline(user_id, limit=25) if hasattr(db, "get_user_timeline") else []
    return (
        "🕓 <b>История пользователя</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n\n"
        f"{_format_user_timeline(items)}"
    )


async def _build_user_referral_menu_text(db: Database, user_id: int) -> str:
    user = await db.get_user(user_id) or {}
    summary = await db.get_referral_summary(user_id) if hasattr(db, "get_referral_summary") else {}
    partner = await db.get_partner_settings(user_id) if hasattr(db, "get_partner_settings") else {}
    custom_l1 = partner.get("custom_percent_level1")
    custom_l2 = partner.get("custom_percent_level2")
    custom_l3 = partner.get("custom_percent_level3")
    return (
        "🤝 <b>Реферальное меню пользователя</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n"
        f"Имя: <code>{escape(' | '.join(part for part in [('@' + str(user.get('username') or '').strip()) if str(user.get('username') or '').strip() else '', str(user.get('first_name') or '').strip()] if part) or '—')}</code>\n\n"
        f"👥 Всего рефералов: <b>{int(summary.get('total_refs', 0) or 0)}</b>\n"
        f"💸 Оплативших: <b>{int(summary.get('paid_refs', 0) or 0)}</b>\n"
        f"💰 Заработано: <b>{float(summary.get('earned_rub', 0.0) or 0.0):.2f} ₽</b>\n"
        f"🏷 Статус партнёра: <b>{partner.get('status') or 'standard'}</b>\n"
        f"📝 Заметка: <code>{_trim_text(str(partner.get('note') or '-'), 80)}</code>\n\n"
        "<b>Специальные условия</b>\n"
        f"1️⃣ Уровень 1: <b>{custom_l1 if custom_l1 is not None else 'стандарт'}</b>\n"
        f"2️⃣ Уровень 2: <b>{custom_l2 if custom_l2 is not None else 'стандарт'}</b>\n"
        f"3️⃣ Уровень 3: <b>{custom_l3 if custom_l3 is not None else 'стандарт'}</b>"
    )


async def _build_user_referrals_list_text(db: Database, user_id: int) -> str:
    rows = await db.get_referrals_list(user_id) if hasattr(db, "get_referrals_list") else []
    lines = []
    for row in rows[:20]:
        paid = "✅ оплатил" if int(row.get("ref_rewarded") or 0) == 1 else "⏳ не оплатил"
        lines.append(
            f"• {_admin_user_id_html(int(row.get('user_id') or 0), label=str(int(row.get('user_id') or 0)))} — {paid}"
            + (f" — <code>{row.get('join_date')}</code>" if row.get("join_date") else "")
        )
    body = "\n".join(lines) if lines else "—"
    return "👥 <b>Рефералы пользователя</b>\n\n" f"Пользователь: <code>{user_id}</code>\n\n" f"{body}"


async def _build_user_referrals_history_text(db: Database, user_id: int) -> str:
    rows = await db.get_ref_history(user_id, limit=20) if hasattr(db, "get_ref_history") else []
    lines = []
    for row in rows[:20]:
        amount = float(row.get("amount") or 0.0)
        bonus_days = int(row.get("bonus_days") or 0)
        parts = []
        if amount > 0:
            parts.append(f"{amount:.2f} ₽")
        if bonus_days > 0:
            parts.append(f"{bonus_days} дн")
        details = " + ".join(parts) if parts else "без суммы"
        lines.append(
            f"• <code>{row.get('created_at') or '-'}</code> — user {_admin_user_id_html(int(row.get('ref_user_id') or 0), label=str(int(row.get('ref_user_id') or 0)))} — <b>{details}</b>"
        )
    body = "\n".join(lines) if lines else "—"
    return "💸 <b>История начислений</b>\n\n" f"Пользователь: <code>{user_id}</code>\n\n" f"{body}"


async def _build_user_referral_last_payment_text(db: Database, user_id: int) -> str:
    referrals = await db.get_referrals_list(user_id) if hasattr(db, "get_referrals_list") else []
    partner = await db.get_partner_settings(user_id) if hasattr(db, "get_partner_settings") else {}
    custom_l1 = partner.get("custom_percent_level1")
    level1_percent = float(custom_l1 if custom_l1 is not None else Config.REF_PERCENT_LEVEL1)

    latest_payment: dict[str, Any] | None = None
    latest_ref_user_id = 0
    for row in referrals:
        ref_user_id = int(row.get("user_id") or 0)
        if ref_user_id <= 0:
            continue
        payments = await db.get_pending_payments_by_user(ref_user_id) if hasattr(db, "get_pending_payments_by_user") else []
        accepted = [payment for payment in payments if str(payment.get("status") or "").strip().lower() == "accepted"]
        if not accepted:
            continue
        accepted.sort(key=lambda payment: str(payment.get("created_at") or ""), reverse=True)
        candidate = accepted[0]
        if latest_payment is None or str(candidate.get("created_at") or "") > str(latest_payment.get("created_at") or ""):
            latest_payment = candidate
            latest_ref_user_id = ref_user_id

    if latest_payment is None:
        return (
            "🧾 <b>Последняя реферальная оплата</b>\n\n"
            f"Пользователь: <code>{user_id}</code>\n\n"
            "У этого пользователя пока нет рефералов с успешной оплатой."
        )

    history_rows = await db.get_ref_history(user_id, limit=100) if hasattr(db, "get_ref_history") else []
    payout_row = next(
        (
            row
            for row in history_rows
            if int(row.get("ref_user_id") or 0) == latest_ref_user_id and float(row.get("amount") or 0.0) > 0
        ),
        None,
    )
    payment_amount = float(latest_payment.get("amount") or 0.0)
    expected_payout = round(payment_amount * level1_percent / 100.0, 2)
    payout_amount = float((payout_row or {}).get("amount") or 0.0)
    payout_status = "✅ начисление найдено" if payout_row else "⚠️ начисление не найдено"

    lines = [
        "🧾 <b>Последняя реферальная оплата</b>",
        "",
        f"Пользователь: <code>{user_id}</code>",
        f"Реферал: {_admin_user_id_html(latest_ref_user_id, label=str(latest_ref_user_id))}",
        f"Платёж: <code>{latest_payment.get('payment_id') or '-'}</code>",
        f"Когда: <b>{_format_dt(latest_payment.get('created_at'))}</b>",
        f"Сумма оплаты: <b>{payment_amount:.2f} ₽</b>",
        f"Уровень 1: <b>{level1_percent:.2f}%</b>",
        f"Ожидаемое начисление: <b>{expected_payout:.2f} ₽</b>",
        f"Статус выплаты: <b>{payout_status}</b>",
    ]
    if payout_row:
        lines.append(f"Фактическое начисление: <b>{payout_amount:.2f} ₽</b>")
        lines.append(f"Начислено: <b>{_format_dt(payout_row.get('created_at'))}</b>")
    return "\n".join(lines)


async def _build_user_subscription_menu_text(db: Database, user_id: int, *, panel=None) -> str:
    user = await db.get_user(user_id) or {}
    plan_text = str(user.get("plan_text") or "—")
    has_subscription = bool(user.get("has_subscription"))
    expiry = str(user.get("expiry") or "-")
    vpn_url = str(user.get("vpn_url") or "")
    if panel is not None:
        try:
            status = await get_runtime_subscription_status(user_id, db=db, panel=panel)
            runtime_user = status.get("user") or {}
            runtime_expiry = status.get("expiry_dt")
            plan_text = str(runtime_user.get("plan_text") or plan_text)
            has_subscription = bool(status.get("active"))
            vpn_url = str(runtime_user.get("vpn_url") or vpn_url)
            if runtime_expiry:
                expiry = _format_dt(runtime_expiry)
        except Exception as exc:
            logger.warning("subscription menu runtime status failed user=%s error=%s", user_id, exc)
    return (
        "📦 <b>Управление подпиской</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n"
        f"Текущий тариф: <b>{escape(plan_text)}</b>\n"
        f"Статус: <b>{'активна' if has_subscription else 'не активна'}</b>\n"
        f"Срок: <code>{expiry}</code>\n"
        f"Ссылка есть: <b>{_format_bool_badge(vpn_url)}</b>\n\n"
        "Здесь можно продлить тариф, сменить план, пересобрать ссылку или отключить подписку."
    )


async def _build_user_payments_menu_text(db: Database, user_id: int) -> str:
    payments = await db.get_pending_payments_by_user(user_id)
    latest = payments[0] if payments else {}
    latest_plan = get_by_id(str(latest.get("plan_id") or "")) or {}
    latest_plan_name = str(latest_plan.get("name") or latest.get("plan_id") or "—")
    return (
        "💳 <b>Платежи пользователя</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n"
        f"Всего платежей: <b>{len(payments)}</b>\n"
        f"Последний платёж: <code>{latest.get('payment_id') or '-'}</code>\n"
        f"Статус: <code>{latest.get('status') or '-'}</code>\n"
        f"Тариф: <b>{escape(latest_plan_name)}</b>\n"
        f"Сумма: <b>{float(latest.get('amount') or 0):.2f} ₽</b>\n\n"
        "Можно открыть все платежи или вручную починить последний подходящий платёж."
    )


async def _build_user_more_menu_text(db: Database, user_id: int) -> str:
    user = await db.get_user(user_id) or {}
    return (
        "⚙️ <b>Дополнительные действия</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n"
        f"Баланс: <b>{float(user.get('balance') or 0):.2f} ₽</b>\n"
        f"Trial использован: <b>{_format_bool_badge(user.get('trial_used'))}</b>\n"
        f"Уведомления о продлении: <code>{user.get('last_expiry_notification_at') or '-'}</code>\n\n"
        "Редкие и сервисные действия вынесены сюда, чтобы не перегружать главную карточку."
    )


def _find_plan_by_user_payload(user: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    plan_text = str(user.get("plan_text") or "").strip().lower()
    if not plan_text:
        return None
    for plan in get_all_active():
        plan_id = str(plan.get("id") or "").strip().lower()
        plan_name = str(plan.get("name") or "").strip().lower()
        if plan_text in {plan_id, plan_name}:
            return plan
    return None


async def _resolve_user_current_plan(db: Database, user_id: int) -> Optional[Dict[str, Any]]:
    user = await db.get_user(user_id) or {}
    plan = _find_plan_by_user_payload(user)
    if plan:
        return plan
    payments = await db.get_pending_payments_by_user(user_id)
    for payment in payments:
        if int(payment.get("recipient_user_id") or payment.get("user_id") or 0) != int(user_id):
            continue
        plan = get_by_id(str(payment.get("plan_id") or ""))
        if plan:
            return plan
    return None


async def _resolve_repairable_payment(db: Database, user_id: int) -> Optional[Dict[str, Any]]:
    payments = await db.get_pending_payments_by_user(user_id)
    preferred_statuses = {"accepted", "pending", "processing"}
    for payment in payments:
        recipient = int(payment.get("recipient_user_id") or payment.get("user_id") or 0)
        if recipient != int(user_id):
            continue
        if str(payment.get("status") or "").lower() not in preferred_statuses:
            continue
        if not get_by_id(str(payment.get("plan_id") or "")):
            continue
        return payment
    return None


async def _build_user_partner_rates_prompt_text(db: Database, user_id: int) -> str:
    partner = await db.get_partner_settings(user_id) if hasattr(db, "get_partner_settings") else {}
    l1 = partner.get("custom_percent_level1")
    l2 = partner.get("custom_percent_level2")
    l3 = partner.get("custom_percent_level3")
    status = partner.get("status") or "standard"
    note = partner.get("note") or ""
    return (
        "🎯 <b>Специальные условия партнёра</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n\n"
        "Отправьте строку в формате:\n"
        "<code>level1 level2 level3 status note</code>\n\n"
        "Пример:\n"
        "<code>30 12 7 vip Сильный партнёр</code>\n\n"
        "Вместо процента можно указать <code>-</code>, чтобы вернуть стандарт.\n"
        "status: <code>standard / partner / vip / ambassador</code>\n\n"
        f"Сейчас:\n"
        f"1️⃣ <b>{l1 if l1 is not None else 'стандарт'}</b>\n"
        f"2️⃣ <b>{l2 if l2 is not None else 'стандарт'}</b>\n"
        f"3️⃣ <b>{l3 if l3 is not None else 'стандарт'}</b>\n"
        f"🏷 <b>{status}</b>\n"
        f"📝 <code>{_trim_text(str(note or '-'), 80)}</code>"
    )


async def _notify_support_restriction_admins(db: Database, bot: Bot, text: str) -> None:
    enabled = await db.support_restriction_notifications_enabled() if hasattr(db, "support_restriction_notifications_enabled") else True
    if not enabled:
        return
    await notify_admins(text, bot=bot)


def _user_card_keyboard(user_id: int, *, banned: bool = False, support_blocked: bool = False) -> InlineKeyboardMarkup:
    ban_label = "✅ Снять бан" if banned else "⛔ Забанить"
    support_label = "🆘 Снять ограничение" if support_blocked else "🆘 Ограничить поддержку"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить карточку", callback_data=f"admin:usercard:{user_id}")],
            [
                InlineKeyboardButton(text="📦 Подписка", callback_data=f"admin:usercard:subscription_menu:{user_id}"),
                InlineKeyboardButton(text="💳 Платежи", callback_data=f"admin:usercard:payments_menu:{user_id}"),
            ],
            [
                InlineKeyboardButton(text=support_label, callback_data=f"admin:usercard:support_menu:{user_id}"),
                InlineKeyboardButton(text=ban_label, callback_data=f"admin:usercard:ban_toggle:{user_id}"),
            ],
            [
                InlineKeyboardButton(text="🕓 История", callback_data=f"admin:usercard:timeline:{user_id}"),
                InlineKeyboardButton(text="📜 Тикеты", callback_data=f"admin:usercard:tickets:{user_id}"),
            ],
            [
                InlineKeyboardButton(text="🤝 Реферальное меню", callback_data=f"admin:usercard:referral_menu:{user_id}"),
                InlineKeyboardButton(text="⚙️ Ещё", callback_data=f"admin:usercard:more_menu:{user_id}"),
            ],
            [InlineKeyboardButton(text="⬅️ К пользователям", callback_data="adminmenu:users")],
        ]
    )


def _user_delete_confirm_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⚠️ Да, удалить полностью", callback_data=f"admin:usercard:delete_confirm:{user_id}")],
            [InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")],
        ]
    )


def _user_card_support_keyboard(user_id: int, *, support_blocked: bool) -> InlineKeyboardMarkup:
    rows = []
    if support_blocked:
        rows.append([InlineKeyboardButton(text="✅ Снять ограничение", callback_data=f"admin:usercard:support_unblock:{user_id}")])
    else:
        rows.append([
            InlineKeyboardButton(text="Спам · 1ч", callback_data=f"admin:usercard:support_block:{user_id}:spam"),
            InlineKeyboardButton(text="Флуд · 24ч", callback_data=f"admin:usercard:support_block:{user_id}:flood"),
        ])
        rows.append([
            InlineKeyboardButton(text="Оскорбления · 7д", callback_data=f"admin:usercard:support_block:{user_id}:abuse"),
            InlineKeyboardButton(text="Мошенничество · 30д", callback_data=f"admin:usercard:support_block:{user_id}:fraud"),
        ])
    rows.append([InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _user_card_referral_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="👥 Рефералы", callback_data=f"admin:usercard:referrals_list:{user_id}"),
                InlineKeyboardButton(text="💸 Начисления", callback_data=f"admin:usercard:referrals_history:{user_id}"),
            ],
            [InlineKeyboardButton(text="🧾 Последняя оплата", callback_data=f"admin:usercard:referral_last_payment:{user_id}")],
            [
                InlineKeyboardButton(text="🎯 Спецусловия", callback_data=f"admin:usercard:partner_rates:{user_id}"),
                InlineKeyboardButton(text="🔁 Перепривязать", callback_data=f"admin:usercard:rebind_referrer:{user_id}"),
            ],
            [InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")],
        ]
    )


def _user_card_subscription_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⏫ Продлить", callback_data=f"admin:usercard:extend_tariff:{user_id}"),
                InlineKeyboardButton(text="🔄 Сменить тариф", callback_data=f"admin:usercard:change_tariff:{user_id}"),
            ],
            [
                InlineKeyboardButton(text="🎁 Выдать тариф", callback_data=f"admin:usercard:grant_tariff:{user_id}"),
                InlineKeyboardButton(text="➕ Бонусные дни", callback_data=f"admin:usercard:bonus_days:{user_id}"),
            ],
            [
                InlineKeyboardButton(text="♻️ Пересобрать ссылку", callback_data=f"admin:usercard:rebuild_subscription:{user_id}"),
                InlineKeyboardButton(text="⛔ Отключить", callback_data=f"admin:usercard:revoke_subscription:{user_id}"),
            ],
            [InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")],
        ]
    )


def _user_card_payments_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="💳 Все платежи", callback_data=f"admin:usercard:payments:{user_id}"),
                InlineKeyboardButton(text="🩺 Repair платёж", callback_data=f"admin:usercard:repair_payment:{user_id}"),
            ],
            [InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")],
        ]
    )


def _user_card_more_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="💰 Скорректировать баланс", callback_data=f"admin:usercard:balance_prompt:{user_id}"),
                InlineKeyboardButton(text="♻️ Сбросить trial", callback_data=f"admin:usercard:reset_trial:{user_id}"),
            ],
            [InlineKeyboardButton(text="🔔 Сбросить уведомления", callback_data=f"admin:usercard:reset_notify:{user_id}")],
            [InlineKeyboardButton(text="🗑 Удалить пользователя", callback_data=f"admin:usercard:delete_prompt:{user_id}")],
            [InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")],
        ]
    )


def _user_card_grant_tariff_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for plan in get_all_active():
        plan_id = str(plan.get("id") or "").strip()
        if not plan_id:
            continue
        plan_name = str(plan.get("name") or plan_id)
        rows.append([InlineKeyboardButton(text=f"🎁 {plan_name}", callback_data=f"admin:usercard:grant_tariff_confirm:{user_id}:{plan_id}")])
    rows.append([InlineKeyboardButton(text="🛠 Выдать вручную", callback_data=f"admin:usercard:grant_custom:{user_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _user_card_grant_custom_plan_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for plan in get_all_active():
        plan_id = str(plan.get("id") or "").strip()
        if not plan_id:
            continue
        plan_name = str(plan.get("name") or plan_id)
        rows.append([InlineKeyboardButton(text=f"🧩 {plan_name}", callback_data=f"admin:usercard:grant_custom_plan:{user_id}:{plan_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ К выдаче тарифа", callback_data=f"admin:usercard:grant_tariff:{user_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _user_card_grant_custom_days_keyboard(user_id: int, plan_id: str) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="7 дней", callback_data=f"admin:usercard:grant_custom_confirm:{user_id}:{plan_id}:7"),
            InlineKeyboardButton(text="14 дней", callback_data=f"admin:usercard:grant_custom_confirm:{user_id}:{plan_id}:14"),
        ],
        [
            InlineKeyboardButton(text="30 дней", callback_data=f"admin:usercard:grant_custom_confirm:{user_id}:{plan_id}:30"),
            InlineKeyboardButton(text="60 дней", callback_data=f"admin:usercard:grant_custom_confirm:{user_id}:{plan_id}:60"),
        ],
        [
            InlineKeyboardButton(text="90 дней", callback_data=f"admin:usercard:grant_custom_confirm:{user_id}:{plan_id}:90"),
            InlineKeyboardButton(text="180 дней", callback_data=f"admin:usercard:grant_custom_confirm:{user_id}:{plan_id}:180"),
        ],
        [InlineKeyboardButton(text="✍️ Ввести вручную", callback_data=f"admin:usercard:grant_custom_input:{user_id}:{plan_id}")],
        [InlineKeyboardButton(text="⬅️ К выбору тарифа", callback_data=f"admin:usercard:grant_custom:{user_id}")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _user_card_extend_tariff_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for plan in get_all_active():
        plan_id = str(plan.get("id") or "").strip()
        if not plan_id:
            continue
        plan_name = str(plan.get("name") or plan_id)
        rows.append([InlineKeyboardButton(text=f"⏫ {plan_name}", callback_data=f"admin:usercard:extend_tariff_confirm:{user_id}:{plan_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _user_card_bonus_days_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="+3 дня", callback_data=f"admin:usercard:bonus_days_confirm:{user_id}:3"),
            InlineKeyboardButton(text="+7 дней", callback_data=f"admin:usercard:bonus_days_confirm:{user_id}:7"),
        ],
        [
            InlineKeyboardButton(text="+14 дней", callback_data=f"admin:usercard:bonus_days_confirm:{user_id}:14"),
            InlineKeyboardButton(text="+30 дней", callback_data=f"admin:usercard:bonus_days_confirm:{user_id}:30"),
        ],
        [InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _support_restrictions_keyboard(rows: List[Dict[str, Any]], *, notify_enabled: bool) -> InlineKeyboardMarkup:
    toggle_label = "🔕 Выключить уведомления" if notify_enabled else "🔔 Включить уведомления"
    keyboard_rows: List[List[InlineKeyboardButton]] = []
    for row in rows[:8]:
        keyboard_rows.append([InlineKeyboardButton(text=f"👤 user {row.get('user_id')}", callback_data=f"admin:usercard:{row.get('user_id')}")])
    keyboard_rows.append([InlineKeyboardButton(text=toggle_label, callback_data="admin:support_restrictions:toggle_notify")])
    keyboard_rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="admin:support_restrictions:list")])
    keyboard_rows.append([InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


async def _resolve_user_display_name(bot: Bot, user_id: int, user: Dict[str, Any] | None = None) -> str:
    payload = user or {}
    username = str(payload.get("username") or "").strip()
    first_name = str(payload.get("first_name") or "").strip()
    parts: List[str] = []
    if username:
        parts.append(f"@{username}")
    if first_name:
        parts.append(first_name)
    if parts:
        return " | ".join(parts)
    try:
        chat = await bot.get_chat(user_id)
    except Exception as exc:
        logger.warning("User card get_chat failed for %s: %s", user_id, exc)
        return "—"
    fresh_parts: List[str] = []
    chat_username = str(getattr(chat, "username", "") or "").strip()
    chat_first_name = str(getattr(chat, "first_name", "") or "").strip()
    if chat_username:
        fresh_parts.append(f"@{chat_username}")
    if chat_first_name:
        fresh_parts.append(chat_first_name)
    return " | ".join(fresh_parts) if fresh_parts else "—"


async def _format_user_id_with_name(bot: Bot, db: Database, user_id: int) -> str:
    user = await db.get_user(user_id)
    display_name = await _resolve_user_display_name(bot, user_id, user)
    if display_name == "—":
        return _admin_user_id_html(user_id)
    return f"{_admin_user_id_html(user_id)} ({escape(display_name)})"
