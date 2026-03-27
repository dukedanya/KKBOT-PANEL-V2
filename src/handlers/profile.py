import logging
from datetime import datetime, timedelta
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
from utils.subscription_links import render_connection_info
from utils.onboarding import onboarding_keyboard, onboarding_text
from utils.telegram_ui import smart_edit_message

logger = logging.getLogger(__name__)
router = Router()


def _payment_status_label(status: str) -> str:
    mapping = {
        "accepted": "оплачен",
        "pending": "ожидает оплаты",
        "processing": "обрабатывается",
        "rejected": "отклонён",
        "refunded": "возврат",
    }
    return mapping.get(str(status or "").strip().lower(), str(status or "неизвестно"))


async def render_profile_text(user_id: int, *, status: dict, panel: PanelAPI, db: Database) -> str:
    active_sub = status["active"]
    user_data = status["user"]
    balance = float(await db.get_balance(user_id))
    gift_links = await db.get_gift_links_by_buyer(user_id, limit=3) if hasattr(db, "get_gift_links_by_buyer") else []
    payments = await db.get_pending_payments_by_user(user_id)

    summary_lines = [
        "👤 <b>Личный кабинет | Какой-то VPN 🪬</b>",
    ]

    if not user_data or not active_sub:
        text = "\n".join(summary_lines + ["", "У вас нет активной подписки.", "", f"💰 Баланс: <b>{balance:.2f} ₽</b>"])
    else:
        legacy_user = await db.get_user(user_id) if hasattr(db, "get_user") else {}
        legacy_user = legacy_user or {}
        base_email = await panel_base_email(user_id, db)
        client_stats = await panel.get_client_stats(base_email)
        full_clients = await panel.find_clients_full_by_email(base_email)
        ip_limit = int(user_data.get("ip_limit") or legacy_user.get("ip_limit") or 0)
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
                "🔗 <b>Ссылка для подключения:</b>",
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
                "🔗 <b>Ссылка для подключения:</b>",
                "",
                connection_info,
                "",
                f"💰 Баланс: <b>{balance:.2f} ₽</b>",
            ]
        text = "\n".join(summary_lines + sub_lines)

    if payments:
        text += "\n\n🧾 <b>Последние платежи</b>"
        for payment in payments[:3]:
            plan_id = str(payment.get("plan_id") or "")
            plan = get_by_id(plan_id) or {}
            plan_name = str(plan.get("name") or plan_id or "Тариф")
            text += (
                "\n"
                f"• {plan_name} — <b>{float(payment.get('amount') or 0):.2f} ₽</b> "
                f"({ _payment_status_label(payment.get('status')) })"
            )

    if gift_links:
        text += "\n\n🎁 <b>Последние подарки</b>"
        for gift in gift_links[:3]:
            plan = get_by_id(str(gift.get("plan_id") or "")) or {}
            plan_name = str(plan.get("name") or gift.get("plan_id") or "Тариф")
            claimed = int(gift.get("claimed_by_user_id") or 0)
            status_label = f"активирован пользователем {claimed}" if claimed else "ещё не активирован"
            text += f"\n• {plan_name} — {status_label}"

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
        "🧾 <b>История покупок и подарков</b>",
        "",
    ]
    if payments:
        lines.append("<b>Платежи</b>")
        for payment in payments[:10]:
            plan_id = str(payment.get("plan_id") or "")
            plan = get_by_id(plan_id) or {}
            plan_name = str(plan.get("name") or plan_id or "Тариф")
            lines.append(
                f"• {plan_name} — <b>{float(payment.get('amount') or 0):.2f} ₽</b> / {_payment_status_label(payment.get('status'))}"
            )
    else:
        lines.append("Платежей пока нет.")
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
        badge = "🔥 Хит" if idx == 1 else "🎯 Выгодно" if idx == 2 else "✨"
        lines.append("")
        lines.append(f"{idx}. {badge} <b>{plan.get('name')}</b>")
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


@router.message(F.text == "⏸ Заморозить подписку")
@router.callback_query(F.data == "profile:freeze")
async def freeze_subscription(event, db: Database, panel: PanelAPI):
    user_id = event.from_user.id
    status = await get_subscription_status(user_id, db=db, panel=panel)
    if not status.get("active"):
        if isinstance(event, CallbackQuery):
            await event.answer("❌ Заморозка доступна только при активной подписке.", show_alert=True)
        else:
            await event.answer("❌ Заморозка доступна только при активной подписке.")
        return
    if status.get("is_frozen") and status.get("frozen_until"):
        until_text = status["frozen_until"].strftime("%d.%m.%Y %H:%M")
        text = f"❄️ Подписка уже заморожена до <b>{until_text}</b>."
        if isinstance(event, CallbackQuery):
            await smart_edit_message(event.message, text, parse_mode="HTML")
            await event.answer()
        else:
            await event.answer(text, parse_mode="HTML")
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="7 дней", callback_data="freeze:7"), InlineKeyboardButton(text="14 дней", callback_data="freeze:14"), InlineKeyboardButton(text="30 дней", callback_data="freeze:30")],
        [InlineKeyboardButton(text="⬅️ В кабинет", callback_data="user_menu:profile")],
    ])
    text = (
        "⏸ <b>Заморозка подписки</b>\n\n"
        "На сколько дней заморозить?\n"
        "Текущая реализация компенсирует паузу продлением срока подписки и помечает её как замороженную."
    )
    if isinstance(event, CallbackQuery):
        await smart_edit_message(event.message, text, reply_markup=keyboard, parse_mode="HTML")
        await event.answer()
    else:
        await event.answer(text, reply_markup=keyboard, parse_mode="HTML")


@router.callback_query(F.data.startswith("freeze:"))
async def freeze_callback(callback: CallbackQuery, db: Database, panel: PanelAPI):
    user_id = callback.from_user.id
    action = callback.data.split(":")[1]
    if action == "cancel":
        await smart_edit_message(callback.message, "Операция отменена.")
        await callback.answer()
        return
    status = await get_subscription_status(user_id, db=db, panel=panel)
    if not status.get("active"):
        await smart_edit_message(callback.message, "❌ Активная подписка не найдена. Заморозка недоступна.")
        await callback.answer()
        return
    if status.get("is_frozen") and status.get("frozen_until"):
        until_text = status["frozen_until"].strftime("%d.%m.%Y %H:%M")
        await smart_edit_message(callback.message, f"❄️ Подписка уже заморожена до <b>{until_text}</b>.", parse_mode="HTML")
        await callback.answer()
        return
    try:
        days = int(action)
    except (TypeError, ValueError):
        await callback.answer("Некорректный срок", show_alert=True)
        return
    base_email = await panel_base_email(user_id, db)
    success = await panel.extend_client_expiry(base_email, days)
    if success:
        frozen_until_dt = datetime.utcnow() + timedelta(days=days)
        await db.set_frozen(user_id, frozen_until_dt.strftime("%Y-%m-%d %H:%M:%S"))
        await smart_edit_message(callback.message, 
            f"❄️ Подписка помечена как замороженная на <b>{days} дней</b>.\n"
            f"Статус заморозки действует до <b>{frozen_until_dt.strftime('%d.%m.%Y %H:%M')}</b>.\n\n"
            "Срок подписки уже компенсирован продлением в панели.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В кабинет", callback_data="user_menu:profile")]]),
        )
    else:
        await smart_edit_message(callback.message, "❌ Не удалось заморозить подписку. Попробуйте позже.")
    await callback.answer()


@router.message(F.text == "▶️ Разморозить подписку")
@router.callback_query(F.data == "profile:unfreeze")
async def unfreeze_subscription(event, db: Database, panel: PanelAPI):
    user_id = event.from_user.id
    status = await get_subscription_status(user_id, db=db, panel=panel)
    if not status.get("frozen_until"):
        text = "ℹ️ Подписка сейчас не заморожена."
    else:
        await db.clear_frozen(user_id)
        text = (
            "✅ Подписка разморожена.\nДоступ снова считается активным сразу, а компенсированные дни уже сохранены."
            if status.get("active")
            else "ℹ️ Статус заморозки очищен, активная подписка не найдена."
        )
    if isinstance(event, CallbackQuery):
        await smart_edit_message(event.message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В кабинет", callback_data="user_menu:profile")]]))
        await event.answer()
    else:
        await event.answer(text)
