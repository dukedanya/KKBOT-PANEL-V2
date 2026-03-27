from __future__ import annotations

from html import escape

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from db import Database
from handlers.admin_user_card_helpers import (
    _build_user_partner_rates_prompt_text,
    _build_user_payments_menu_text,
    _build_user_referral_last_payment_text,
    _build_user_referral_menu_text,
    _build_user_referrals_history_text,
    _build_user_referrals_list_text,
    _user_card_payments_menu_keyboard,
    _user_card_referral_menu_keyboard,
)
from tariffs import get_by_id


async def build_user_payments_text(db: Database, user_id: int) -> str:
    payments = await db.get_pending_payments_by_user(user_id)
    lines = ["💳 <b>Платежи пользователя</b>", "", f"Пользователь: <code>{user_id}</code>"]
    if not payments:
        lines.append("\nПлатежей пока нет.")
    else:
        for item in payments[:12]:
            plan = get_by_id(str(item.get("plan_id") or "")) or {}
            plan_name = str(plan.get("name") or item.get("plan_id") or "Тариф")
            lines.append(
                "\n"
                f"• <code>{item.get('payment_id') or '-'}</code>\n"
                f"  {escape(plan_name)} • <b>{float(item.get('amount') or 0):.2f} ₽</b>\n"
                f"  <code>{item.get('status') or '-'}</code> • {escape(str(item.get('provider') or '-'))}\n"
                f"  <code>{item.get('created_at') or '-'}</code>"
            )
    return "\n".join(lines)


def user_back_to_card_markup(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")]])


async def build_balance_prompt_text(user_id: int) -> str:
    return (
        "💰 <b>Корректировка баланса</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n"
        "Отправьте сумму и причину одним сообщением.\n"
        "Пример: <code>+150 бонус за кампанию</code>\n"
        "Пример: <code>-50 ручная корректировка</code>"
    )


def user_balance_prompt_markup(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К дополнительным", callback_data=f"admin:usercard:more_menu:{user_id}")]])


async def build_referral_menu_screen(db: Database, user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    return await _build_user_referral_menu_text(db, user_id), _user_card_referral_menu_keyboard(user_id)


async def build_referrals_list_screen(db: Database, user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    return await _build_user_referrals_list_text(db, user_id), _user_card_referral_menu_keyboard(user_id)


async def build_referrals_history_screen(db: Database, user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    return await _build_user_referrals_history_text(db, user_id), _user_card_referral_menu_keyboard(user_id)


async def build_referral_last_payment_screen(db: Database, user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    return await _build_user_referral_last_payment_text(db, user_id), _user_card_referral_menu_keyboard(user_id)


async def build_partner_rates_prompt_screen(db: Database, user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    return (
        await _build_user_partner_rates_prompt_text(db, user_id),
        InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К реферальному меню", callback_data=f"admin:usercard:referral_menu:{user_id}")]]),
    )


async def build_rebind_referrer_prompt_text(db: Database, user_id: int) -> str:
    user = await db.get_user(user_id) or {}
    return (
        "🔁 <b>Перепривязка реферала</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n"
        f"Текущий ref_by: <code>{int(user.get('ref_by') or 0)}</code>\n\n"
        "Отправьте новый <code>ID</code> реферера.\n"
        "Чтобы снять привязку, отправьте <code>0</code>."
    )


def user_rebind_referrer_markup(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К реферальному меню", callback_data=f"admin:usercard:referral_menu:{user_id}")]])


async def build_user_payments_menu_screen(db: Database, user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    return await _build_user_payments_menu_text(db, user_id), _user_card_payments_menu_keyboard(user_id)
