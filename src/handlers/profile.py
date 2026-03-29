import logging
import json
from datetime import datetime
from typing import Optional

from aiogram import Router, F, Bot
from aiogram.enums import ParseMode
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton

from config import Config
from db import Database
from tariffs import get_by_id, format_duration, format_price
from keyboards import subscriptions_inline_keyboard, profile_inline_keyboard
from utils.helpers import replace_message, get_visible_plans
from kkbot.services.subscriptions import create_subscription, is_active_subscription, get_subscription_status, panel_base_email
from services.panel import PanelAPI
from services.payment_gateway import build_payment_gateway, get_provider_label
from utils.subscription_links import build_primary_subscription_url, render_connection_info
from utils.onboarding import onboarding_keyboard, onboarding_text
from utils.telegram_ui import smart_edit_message
from utils.payments import get_provider_payment_id

logger = logging.getLogger(__name__)
router = Router()


def _payment_status_label(status: str) -> str:
    mapping = {
        "accepted": "✅ оплачен",
        "pending": "⏳ ожидает оплаты",
        "processing": "🔄 обрабатывается",
        "rejected": "❌ отклонён",
        "refunded": "↩️ возврат",
        "cancelled": "❌ отменён",
        "canceled": "❌ отменён",
        "waiting_for_capture": "🕓 ждёт подтверждения",
    }
    return mapping.get(str(status or "").strip().lower(), str(status or "неизвестно"))


def _format_dt(value) -> str:
    if not value:
        return "—"
    if isinstance(value, str):
        raw = value.strip().replace("Z", "+00:00")
        try:
            value = datetime.fromisoformat(raw)
        except ValueError:
            return value
    if isinstance(value, datetime):
        return value.strftime("%d.%m.%Y %H:%M")
    return str(value)


def _payment_status_note(status: str) -> str:
    mapping = {
        "accepted": "Подписка уже активирована автоматически.",
        "pending": "Платёж создан, но оплата ещё не завершена.",
        "processing": "Платёж уже найден. Бот продолжает автоматическую проверку.",
        "waiting_for_capture": "Платёж получен платёжной системой и ждёт подтверждения.",
        "rejected": "Платёж не прошёл или был отклонён.",
        "cancelled": "Платёж отменён.",
        "canceled": "Платёж отменён.",
        "refunded": "По этому платежу оформлен возврат.",
    }
    return mapping.get(str(status or "").strip().lower(), "Статус платежа обновляется автоматически.")


def _provider_label(provider: str) -> str:
    provider_key = str(provider or "").strip().lower()
    if provider_key == "balance":
        return "Баланс"
    if provider_key == "telegram_stars":
        return "Telegram Stars"
    return get_provider_label(provider_key)


def _payment_plan_name(payment: dict) -> str:
    plan_id = str(payment.get("plan_id") or "")
    plan = get_by_id(plan_id) or {}
    return str(plan.get("name") or plan_id or "Тариф")


def _pick_relevant_payment(payments: list[dict]) -> Optional[dict]:
    if not payments:
        return None
    preferred_statuses = {"pending", "processing", "waiting_for_capture"}
    for payment in payments:
        if str(payment.get("status") or "").strip().lower() in preferred_statuses:
            return payment
    return payments[0]


async def _resolve_payment_checkout_url(payment: dict, payment_gateway) -> str:
    provider = str(payment.get("provider") or "").strip().lower()
    if not provider or provider in {"balance", "telegram_stars"}:
        return ""
    provider_payment_id = get_provider_payment_id(payment)
    if not provider_payment_id:
        return ""
    gateway = payment_gateway
    should_close = False
    if str(getattr(payment_gateway, "provider_name", "") or "").strip().lower() != provider:
        gateway = build_payment_gateway(provider)
        should_close = True
    try:
        remote_payment = await gateway.get_payment(provider_payment_id)
        if not remote_payment:
            return ""
        return gateway.get_checkout_url(remote_payment)
    finally:
        if should_close and hasattr(gateway, "close"):
            await gateway.close()


async def _build_payment_status_text(user_id: int, *, db: Database, payment_gateway) -> tuple[str, InlineKeyboardMarkup]:
    payments = await db.get_pending_payments_by_user(user_id)
    payment = _pick_relevant_payment(payments)
    if not payment:
        text = (
            "💳 <b>Статус оплаты</b>\n\n"
            "У вас пока нет платежей.\n\n"
            "Когда вы создадите оплату, здесь будет видно её текущий статус и история изменений."
        )
        markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В кабинет", callback_data="user_menu:profile")]])
        return text, markup

    status = str(payment.get("status") or "").strip().lower()
    history = await db.get_payment_status_history(str(payment.get("payment_id") or ""), limit=5)
    checkout_url = await _resolve_payment_checkout_url(payment, payment_gateway)

    lines = [
        "💳 <b>Статус оплаты</b>",
        "",
        f"📦 Тариф: <b>{_payment_plan_name(payment)}</b>",
        f"🧾 ID: <code>{payment.get('payment_id') or '-'}</code>",
        f"🏦 Способ оплаты: <b>{_provider_label(payment.get('provider') or '')}</b>",
        f"💰 Сумма: <b>{float(payment.get('amount') or 0):.2f} ₽</b>",
        f"📍 Статус: <b>{_payment_status_label(status)}</b>",
        f"🕓 Создан: <b>{_format_dt(payment.get('created_at'))}</b>",
    ]
    processed_at = payment.get("processed_at") or payment.get("updated_at")
    if processed_at:
        lines.append(f"🧷 Обновлён: <b>{_format_dt(processed_at)}</b>")
    lines.extend(["", _payment_status_note(status)])

    if history:
        lines.extend(["", "<b>Последние изменения</b>"])
        for row in history[:4]:
            to_status = _payment_status_label(str(row.get("to_status") or ""))
            source = str(row.get("source") or "").strip()
            source_suffix = f" • {source}" if source else ""
            lines.append(f"• {_format_dt(row.get('created_at'))} — {to_status}{source_suffix}")

    buttons: list[list[InlineKeyboardButton]] = []
    if checkout_url:
        buttons.append([InlineKeyboardButton(text="💳 Перейти к оплате", url=checkout_url)])
    if status in {"pending", "processing", "waiting_for_capture"} and str(payment.get("provider") or "").strip().lower() not in {"balance", "telegram_stars"}:
        buttons.append([InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"check_payment:{payment['payment_id']}")])
    buttons.append([InlineKeyboardButton(text="🧾 Вся история", callback_data="profile:history")])
    buttons.append([InlineKeyboardButton(text="⬅️ В кабинет", callback_data="user_menu:profile")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=buttons)


async def render_profile_text(user_id: int, *, status: dict, panel: PanelAPI, db: Database) -> str:
    active_sub = status["active"]
    user_data = status["user"]
    balance = float(await db.get_balance(user_id))

    summary_lines = [
        "👤 <b>Личный кабинет | Какой-то VPN 🪬</b>",
    ]

    if not user_data or not active_sub:
        text = "\n".join(summary_lines + ["", "У вас нет активной подписки.", "", f"💰 Баланс: <b>{balance:.2f} ₽</b>"])
    else:
        legacy_user = await db.get_user(user_id) if hasattr(db, "get_user") else {}
        legacy_user = legacy_user or {}
        record = status.get("record") or {}
        record_meta = record.get("meta") if isinstance(record, dict) else {}
        if isinstance(record_meta, str) and record_meta.strip():
            try:
                record_meta = json.loads(record_meta)
            except (TypeError, ValueError, json.JSONDecodeError):
                record_meta = {}
        if not isinstance(record_meta, dict):
            record_meta = {}
        base_email = await panel_base_email(user_id, db)
        client_stats = await panel.get_client_stats(base_email)
        full_clients = await panel.find_clients_full_by_email(base_email)
        ip_limit = int(user_data.get("ip_limit") or legacy_user.get("ip_limit") or 0)
        vpn_url = build_primary_subscription_url(
            client_uuid=str(record_meta.get("panel_client_uuid") or "").strip(),
            sub_id=str(record_meta.get("panel_sub_id") or "").strip(),
        )
        if not vpn_url:
            vpn_url = user_data.get("vpn_url") or legacy_user.get("vpn_url") or ""
        connection_info = render_connection_info(vpn_url, user_id=user_id, include_sidr=False)
        expiry_dt = status.get("expiry_dt")
        expiry_date = expiry_dt.strftime("%d.%m.%Y %H:%M") if expiry_dt else "не указана"

        if client_stats or full_clients:
            sub_lines = [
                "",
                "📦 <b>Текущая подписка</b>",
                f"IP-адреса: <b>до {ip_limit}</b>",
                f"Срок действия: <b>до {expiry_date}</b>",
                "",
                connection_info,
                "",
                f"💰 Баланс: <b>{balance:.2f} ₽</b>",
            ]
        else:
            sub_lines = [
                "",
                "📦 <b>Текущая подписка</b>",
                f"IP-адреса: <b>до {ip_limit}</b>",
                "",
                connection_info,
                "",
                f"💰 Баланс: <b>{balance:.2f} ₽</b>",
            ]
        text = "\n".join(summary_lines + sub_lines)

    return text




async def show_profile_menu(user_id: int, *, db: Database, panel: PanelAPI, bot: Optional[Bot] = None, user_msg: Optional[Message] = None):
    status = await get_subscription_status(user_id, db=db, panel=panel)
    text = await render_profile_text(user_id, status=status, panel=panel, db=db)
    await replace_message(
        user_id,
        text,
        reply_markup=profile_inline_keyboard(status["active"], is_frozen=status["is_frozen"], is_admin=user_id in Config.ADMIN_USER_IDS),
        delete_user_msg=user_msg,
        bot=bot,
    )


async def _build_purchase_history_text(user_id: int, db: Database) -> str:
    payments = await db.get_pending_payments_by_user(user_id)
    gifts = await db.get_user_gift_history(user_id, limit=10) if hasattr(db, "get_user_gift_history") else []
    lines = [
        "🧾 <b>История платежей и подарков</b>",
        "",
    ]
    open_statuses = {"pending", "processing", "waiting_for_capture"}
    success_statuses = {"accepted"}
    failed_statuses = {"rejected", "refunded", "cancelled", "canceled"}
    open_payments = [row for row in payments if str(row.get("status") or "").strip().lower() in open_statuses]
    successful_payments = [row for row in payments if str(row.get("status") or "").strip().lower() in success_statuses]
    failed_payments = [row for row in payments if str(row.get("status") or "").strip().lower() in failed_statuses]

    def _append_payment_section(title: str, rows: list[dict]) -> None:
        lines.append(f"<b>{title}</b>")
        if not rows:
            lines.append("• Нет записей")
            return
        for payment in rows[:8]:
            lines.append(
                f"• {_format_dt(payment.get('created_at'))} — <b>{_payment_plan_name(payment)}</b>\n"
                f"  {_payment_status_label(str(payment.get('status') or ''))} • {_provider_label(payment.get('provider') or '')} • {float(payment.get('amount') or 0):.2f} ₽"
            )

    _append_payment_section("Незавершённые", open_payments)
    lines.append("")
    _append_payment_section("Успешные", successful_payments)
    lines.append("")
    _append_payment_section("Неуспешные и возвраты", failed_payments)

    if gifts:
        lines.extend(["", "<b>Подарки</b>"])
        for gift in gifts[:10]:
            plan = get_by_id(str(gift.get("plan_id") or "")) or {}
            plan_name = str(plan.get("name") or gift.get("plan_id") or "Тариф")
            if int(gift.get("buyer_user_id") or 0) == user_id:
                direction = "Вы подарили"
                target = f"получатель {gift.get('claimed_by_user_id')}" if gift.get("claimed_by_user_id") else "ещё не активирован"
            else:
                direction = "Вам подарили"
                target = f"от {gift.get('buyer_user_id')}"
            note = str(gift.get("note") or "").strip()
            line = f"• {direction}: {plan_name} — {target}"
            if note:
                line += f" — «{note[:60]}»"
            lines.append(line)
    return "\n".join(lines)


@router.callback_query(F.data == "profile:history")
async def profile_history(callback: CallbackQuery, db: Database):
    text = await _build_purchase_history_text(callback.from_user.id, db)
    await smart_edit_message(
        callback.message,
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В кабинет", callback_data="user_menu:profile")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "profile:payment_status")
async def profile_payment_status(callback: CallbackQuery, db: Database, payment_gateway):
    text, markup = await _build_payment_status_text(callback.from_user.id, db=db, payment_gateway=payment_gateway)
    await smart_edit_message(
        callback.message,
        text,
        reply_markup=markup,
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(F.text == "📦 Подписки")
async def subscriptions_menu(message: Message, db: Database, panel: PanelAPI):
    user_id = message.from_user.id
    await db.add_user(user_id)
    active = await is_active_subscription(user_id, db=db, panel=panel)
    user_data = await db.get_user(user_id)

    if not active and user_data.get("trial_used") == 0 and user_data.get("trial_declined") == 0:
        trial_plan = get_by_id("trial")
        if trial_plan and trial_plan.get("active"):
            text = (
                "🎁 <b>Пробный период!</b>\n\n"
                "Новым пользователям доступен пробный тариф:\n"
                f"✅ <b>{trial_plan.get('name', 'Пробный')}</b>\n"
                f"📱 Устройств: до {trial_plan.get('ip_limit', 1)}\n"
                f"⏱ Срок: {format_duration(trial_plan.get('duration_days', 3))}\n\n"
                "Хотите попробовать?"
            )
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Попробовать", callback_data="trial_accept")],
                    [InlineKeyboardButton(text="❌ Отказаться", callback_data="trial_decline")],
                ]
            )
            await replace_message(user_id, text, reply_markup=keyboard, delete_user_msg=message, bot=message.bot)
            return

    await show_available_tariffs(user_id, active, db=db, panel=panel, bot=message.bot, user_msg=message)


@router.callback_query(F.data == "user_menu:subscriptions")
async def subscriptions_menu_callback(callback: CallbackQuery, db: Database, panel: PanelAPI):
    user_id = callback.from_user.id
    active = await is_active_subscription(user_id, db=db, panel=panel)
    status_user = await db.get_user(user_id)
    if not active and status_user.get("trial_used") == 0 and status_user.get("trial_declined") == 0:
        trial_plan = get_by_id("trial")
        if trial_plan and trial_plan.get("active"):
            text = (
                "🎁 <b>Пробный период!</b>\n\n"
                f"✅ <b>{trial_plan.get('name', 'Пробный')}</b>\n"
                f"📱 Устройств: до {trial_plan.get('ip_limit', 1)}\n"
                f"⏱ Срок: {format_duration(trial_plan.get('duration_days', 3))}\n\n"
                "Хотите попробовать?"
            )
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Попробовать", callback_data="trial_accept")],
                    [InlineKeyboardButton(text="❌ Отказаться", callback_data="trial_decline")],
                ]
            )
            await smart_edit_message(callback.message, text, reply_markup=keyboard, parse_mode="HTML")
            await callback.answer()
            return
    status = await get_subscription_status(user_id, db=db, panel=panel)
    text = await build_subscriptions_text(user_id, status=status, db=db)
    await smart_edit_message(callback.message, text, reply_markup=subscriptions_inline_keyboard(status["active"], is_admin=callback.from_user.id in Config.ADMIN_USER_IDS), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data == "trial_accept")
async def trial_accept(callback: CallbackQuery, db: Database, panel: PanelAPI):
    user_id = callback.from_user.id
    user_data = await db.get_user(user_id)
    if user_data.get("trial_used"):
        await callback.answer("❌ Пробный период уже использован.", show_alert=True)
        return
    trial_plan = get_by_id("trial")
    if not trial_plan or not trial_plan.get("active"):
        await callback.answer("❌ Пробный тариф недоступен.", show_alert=True)
        return
    try:
        await callback.message.delete()
    except Exception as e:
        logger.warning("Не удалось удалить сообщение trial: %s", e)
    vpn_url = await create_subscription(user_id, trial_plan, db=db, panel=panel, plan_suffix=" (пробный)")
    if vpn_url:
        await db.update_user(user_id, trial_used=1)
        await db.set_has_subscription(user_id)
        connection_info = render_connection_info(vpn_url, user_id=user_id, plan_name=trial_plan.get("name"))
        await callback.message.answer(
            f"✅ <b>Пробный период активирован!</b>\n\n{connection_info}\n\n{onboarding_text()}",
            parse_mode=ParseMode.HTML,
            reply_markup=onboarding_keyboard(),
        )
    else:
        await callback.message.answer("❌ Не удалось активировать пробный период. Попробуйте позже или обратитесь в поддержку.")


@router.callback_query(F.data == "trial_decline")
async def trial_decline(callback: CallbackQuery, db: Database, panel: PanelAPI):
    user_id = callback.from_user.id
    await db.update_user(user_id, trial_declined=1)
    status = await get_subscription_status(user_id, db=db, panel=panel)
    text = await build_subscriptions_text(user_id, status=status, db=db)
    await smart_edit_message(callback.message, text, reply_markup=subscriptions_inline_keyboard(status["active"], is_admin=callback.from_user.id in Config.ADMIN_USER_IDS), parse_mode="HTML")
    await callback.answer()


async def build_subscriptions_text(user_id: int, *, status: dict, db: Database) -> str:
    user_data = status["user"]
    active = bool(status["active"])
    lines = []
    if active:
        plan_text = user_data.get("plan_text", "Неизвестно")
        ip_limit = user_data.get("ip_limit", 0)
        expiry_dt = status.get("expiry_dt")
        expiry_str = expiry_dt.strftime("%d.%m.%Y %H:%M") if expiry_dt else "неизвестно"
        lines.extend([
            "📦 <b>Ваша подписка</b>",
            "",
            f"Тариф: <b>{plan_text}</b>",
            f"Устройств: до {ip_limit}",
            f"Статус: {'❄️ Заморожена' if status.get('is_frozen') else '✅ Активна'}",
        ])
        if status.get("is_frozen") and status.get("frozen_until"):
            lines.append(f"Заморожена до: {status['frozen_until'].strftime('%d.%m.%Y %H:%M')}")
        lines.extend([
            f"Срок действия: до {expiry_str}",
            "",
            "⬇️ <b>Доступные тарифы</b>",
        ])
    else:
        lines.extend(["📦 <b>Доступные тарифы</b>", "", "У вас пока нет активной подписки."])

    plans = await get_visible_plans(user_id, for_admin=False, db=db)
    if not plans:
        return "\n".join(lines + ["", "Тарифы временно недоступны."])

    if not active:
        lines.extend([
            "",
            "Почему выбирают нас:",
            "• быстрое подключение",
            "• понятная инструкция",
            "• поддержка прямо в боте",
            "• можно оплатить с баланса, по промокоду или подарить доступ",
        ])

    for idx, plan in enumerate(plans, 1):
        lines.append("")
        lines.append(f"{idx}. <b>{plan.get('name')}</b>")
        description = str(plan.get("description") or "").strip()
        quote_parts = [
            f"💰 {format_price(plan)}",
            f"📱 до {plan.get('ip_limit')} устройств",
            "∞ Безлимитный трафик",
            f"⏱ {format_duration(int(plan.get('duration_days', 30)))}",
        ]
        if description:
            quote_parts.append(description)
        lines.append("   <blockquote>" + "\n".join(quote_parts) + "</blockquote>")
    return "\n".join(lines)


async def show_available_tariffs(user_id: int, has_active_subscription: bool, db: Database, panel: PanelAPI, bot: Optional[Bot] = None, user_msg: Optional[Message] = None):
    status = await get_subscription_status(user_id, db=db, panel=panel)
    text = await build_subscriptions_text(user_id, status=status, db=db)
    await replace_message(user_id, text, reply_markup=subscriptions_inline_keyboard(bool(has_active_subscription and status["active"]), is_admin=user_id in Config.ADMIN_USER_IDS), delete_user_msg=user_msg, bot=bot)


@router.callback_query(F.data == "back_to_subscriptions")
async def back_to_subscriptions(callback: CallbackQuery, db: Database, panel: PanelAPI):
    status = await get_subscription_status(callback.from_user.id, db=db, panel=panel)
    text = await build_subscriptions_text(callback.from_user.id, status=status, db=db)
    await smart_edit_message(callback.message, text, reply_markup=subscriptions_inline_keyboard(status["active"], is_admin=callback.from_user.id in Config.ADMIN_USER_IDS), parse_mode="HTML")
    await callback.answer()
