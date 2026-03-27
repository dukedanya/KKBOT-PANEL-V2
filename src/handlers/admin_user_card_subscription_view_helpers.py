from __future__ import annotations

from html import escape

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from db import Database
from handlers.admin_user_card_helpers import (
    _user_card_bonus_days_keyboard,
    _user_card_extend_tariff_keyboard,
    _user_card_grant_custom_days_keyboard,
    _user_card_grant_custom_plan_keyboard,
    _user_card_grant_tariff_keyboard,
)
from tariffs import get_all_active, get_by_id


async def build_change_tariff_prompt(db: Database, user_id: int) -> tuple[bool, str, InlineKeyboardMarkup | None]:
    user = await db.get_user(user_id)
    if not user:
        return False, "Пользователь не найден", None
    active_plans = get_all_active()
    if not active_plans:
        return False, "Нет активных тарифов", None
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            *[
                [
                    InlineKeyboardButton(
                        text=f"🔄 {str(plan.get('name') or plan.get('id') or 'Тариф')}",
                        callback_data=f"admin:usercard:change_tariff_confirm:{user_id}:{str(plan.get('id') or '').strip()}",
                    )
                ]
                for plan in active_plans
                if str(plan.get("id") or "").strip()
            ],
            [InlineKeyboardButton(text="⬅️ К подписке", callback_data=f"admin:usercard:subscription_menu:{user_id}")],
        ]
    )
    text = (
        "🔄 <b>Смена тарифа</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n"
        f"Сейчас: <b>{escape(str(user.get('plan_text') or '—'))}</b>\n\n"
        "Выберите новый тариф. Остаток активных дней будет сохранён."
    )
    return True, text, markup


async def build_grant_tariff_prompt(db: Database, user_id: int) -> tuple[bool, str, InlineKeyboardMarkup | None]:
    user = await db.get_user(user_id)
    if not user:
        return False, "Пользователь не найден", None
    active_plans = get_all_active()
    if not active_plans:
        return False, "Нет активных тарифов", None
    plan_lines = [f"• <b>{escape(str(plan.get('name') or plan.get('id') or 'Тариф'))}</b>" for plan in active_plans[:8]]
    text = (
        "🎁 <b>Выдача тарифа</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n\n"
        "Выберите тариф, который нужно выдать вручную:\n"
        + "\n".join(plan_lines)
    )
    return True, text, _user_card_grant_tariff_keyboard(user_id)


async def build_grant_custom_prompt(db: Database, user_id: int) -> tuple[bool, str, InlineKeyboardMarkup | None]:
    user = await db.get_user(user_id)
    if not user:
        return False, "Пользователь не найден", None
    text = (
        "🛠 <b>Ручная выдача тарифа</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n\n"
        "Сначала выберите базовый тариф. Затем можно будет указать точный срок в днях."
    )
    return True, text, _user_card_grant_custom_plan_keyboard(user_id)


async def build_grant_custom_plan_prompt(db: Database, user_id: int, plan_id: str) -> tuple[bool, str, InlineKeyboardMarkup | None]:
    user = await db.get_user(user_id)
    plan = get_by_id(plan_id)
    if not user:
        return False, "Пользователь не найден", None
    if not plan:
        return False, "Тариф не найден", None
    text = (
        "🛠 <b>Ручная выдача тарифа</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n"
        f"Тариф: <b>{escape(str(plan.get('name') or plan_id))}</b>\n\n"
        "Выберите срок, на который нужно выдать подписку."
    )
    return True, text, _user_card_grant_custom_days_keyboard(user_id, plan_id)


async def build_grant_custom_input_prompt(db: Database, user_id: int, plan_id: str) -> tuple[bool, str, InlineKeyboardMarkup | None]:
    user = await db.get_user(user_id)
    plan = get_by_id(plan_id)
    if not user:
        return False, "Пользователь не найден", None
    if not plan:
        return False, "Тариф не найден", None
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⬅️ К выбору срока",
                    callback_data=f"admin:usercard:grant_custom_plan:{user_id}:{plan_id}",
                )
            ]
        ]
    )
    text = (
        "🛠 <b>Ручная выдача тарифа</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n"
        f"Тариф: <b>{escape(str(plan.get('name') or plan_id))}</b>\n\n"
        "Отправьте срок в днях следующим сообщением.\n"
        "Пример: <code>45</code>"
    )
    return True, text, markup


async def build_extend_tariff_prompt(db: Database, user_id: int) -> tuple[bool, str, InlineKeyboardMarkup | None]:
    user = await db.get_user(user_id)
    if not user:
        return False, "Пользователь не найден", None
    active_plans = get_all_active()
    if not active_plans:
        return False, "Нет активных тарифов", None
    text = (
        "⏫ <b>Продлить тариф</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n\n"
        "Выберите тариф, на срок которого нужно продлить доступ."
    )
    return True, text, _user_card_extend_tariff_keyboard(user_id)


async def build_bonus_days_prompt(db: Database, user_id: int) -> tuple[bool, str, InlineKeyboardMarkup | None]:
    user = await db.get_user(user_id)
    if not user:
        return False, "Пользователь не найден", None
    text = (
        "➕ <b>Бонусные дни</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n\n"
        "Выберите, сколько дней добавить к текущему сроку."
    )
    return True, text, _user_card_bonus_days_keyboard(user_id)
