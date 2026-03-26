import json
import logging
import os
import time

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config import Config
from db import Database
from keyboards import main_menu_keyboard, admin_menu_keyboard
from tariffs import build_buy_text, get_all_active, get_by_id
from utils.helpers import notify_user, replace_message
from utils.telegram_ui import smart_edit_message
from utils.templates import (
    TEMPLATES,
    get_template_content,
    template_allow_photo,
    template_default_text,
    template_title,
    template_variables,
    template_variables_map,
)

logger = logging.getLogger(__name__)
router = Router()
STARS_MULTIPLIER_SETTING_KEY = "system:telegram_stars_price_multiplier"


class TariffEditFSM(StatesGroup):
    choosing = State()
    field = State()
    value = State()


class StarsSettingsFSM(StatesGroup):
    multiplier = State()


class ReferralSettingsFSM(StatesGroup):
    field = State()


class PartnerSettingsFSM(StatesGroup):
    rates = State()
    balance = State()

class MainMessageFSM(StatesGroup):
    content = State()


class TemplateEditFSM(StatesGroup):
    content = State()
    confirm = State()


class PromoCodeFSM(StatesGroup):
    content = State()
    edit_value = State()


def _optional_int_or_none(raw: str):
    value = (raw or "").strip().lower()
    if value in {"", "-", "none", "null", "auto"}:
        return None
    return int(value)


TARIFF_FIELDS = {
    "name": ("Название", str),
    "price_rub": ("Цена (руб)", int),
    "duration_days": ("Дней", int),
    "ip_limit": ("Устройств", int),
    "traffic_gb": ("Трафик ГБ", float),
    "sort": ("Порядок", int),
    "description": ("Описание", str),
    "price_stars": ("Цена в Stars", _optional_int_or_none),
}


def is_admin(user_id: int) -> bool:
    return user_id in Config.ADMIN_USER_IDS


def tariffs_list_keyboard(plans):
    rows = []
    for plan in plans:
        status = "✅" if plan.get("active", True) else "❌"
        rows.append([
            InlineKeyboardButton(text=f"{status} {plan.get('name', plan['id'])}", callback_data=f"tedit:{plan['id']}"),
            InlineKeyboardButton(text="🔀", callback_data=f"ttoggle:{plan['id']}"),
        ])
    rows.append([InlineKeyboardButton(text="➕ Добавить тариф", callback_data="tadd")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_admin")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _dashboard_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")]])


def tariff_fields_keyboard(plan_id: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"✏️ {label}", callback_data=f"tfield:{plan_id}:{field}")]
        for field, (label, _) in TARIFF_FIELDS.items()
    ]
    rows.append([InlineKeyboardButton(text="🗑 Удалить тариф", callback_data=f"tdelete:{plan_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="tlist")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _tariff_editor_text(plan: dict, plan_id: str, *, success_prefix: str = "") -> str:
    lines = []
    if success_prefix:
        lines.extend([success_prefix, ""])
    lines.extend([f"✏️ <b>Тариф: {plan.get('name', plan_id)}</b>", ""])
    for key, (label, _) in TARIFF_FIELDS.items():
        lines.append(f"{label}: <b>{_format_tariff_field_value(plan, key)}</b>")
    return "\n".join(lines)




def _format_tariff_field_value(plan: dict, field: str):
    value = plan.get(field, "—")
    if field == "price_stars" and value in (None, "", 0):
        return f"авто × {Config.TELEGRAM_STARS_PRICE_MULTIPLIER}"
    return value


def _stars_settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить коэффициент", callback_data="admin:stars_multiplier")],
        [InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")],
    ])


def _promo_menu_keyboard(rows: list[dict]) -> InlineKeyboardMarkup:
    keyboard_rows = [
        [InlineKeyboardButton(text="➕ Добавить промокод", callback_data="admin:promo_add_prompt")],
        [InlineKeyboardButton(text="📊 Статистика промо и подарков", callback_data="admin:promo_stats")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin:promo_menu")],
    ]
    for row in rows[:10]:
        state_label = "🟢" if int(row.get("active") or 0) == 1 else "⚪️"
        keyboard_rows.append([
            InlineKeyboardButton(
                text=f"{state_label} {row.get('code')}",
                callback_data=f"admin:promo_view:{row.get('code')}",
            )
        ])
    keyboard_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_admin")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


async def _build_promo_menu_text(db: Database) -> tuple[str, list[dict]]:
    rows = await db.list_promo_codes(limit=20) if hasattr(db, "list_promo_codes") else []
    lines = [
        "🏷 <b>Промокоды</b>",
        "",
        "Создание форматом:",
        "<code>CODE | TYPE | VALUE | MAX_USES | USER_LIMIT | PLAN_IDS | NEW_ONLY | TITLE</code>",
        "TYPE: <code>percent</code> или <code>fixed</code>",
        "Пример: <code>SPRING25 | percent | 25 | 100 | 1 | basic,pro | 0 | Весенняя акция</code>",
        "Пример: <code>WELCOME500 | fixed | 500 | 1 | 1 |  | 1 | Только новым</code>",
        "",
    ]
    if not rows:
        lines.append("Промокодов пока нет.")
    else:
        for row in rows:
            discount_type = str(row.get("discount_type") or "percent")
            if discount_type == "fixed":
                discount_label = f"{float(row.get('fixed_amount') or 0):.0f} ₽"
            else:
                discount_label = f"{float(row.get('discount_percent') or 0):.0f}%"
            flags = []
            if int(row.get("only_new_users") or 0) == 1:
                flags.append("только новым")
            if str(row.get("plan_ids") or "").strip():
                flags.append(f"тарифы: {row.get('plan_ids')}")
            lines.append(
                f"• <code>{row.get('code')}</code> — <b>{discount_label}</b> "
                f"/ использовано <b>{int(row.get('used_count') or 0)}</b>"
                + (f" из {int(row.get('max_uses') or 0)}" if int(row.get('max_uses') or 0) > 0 else "")
                + f" / {'активен' if int(row.get('active') or 0) == 1 else 'выключен'}"
                + (f" / {', '.join(flags)}" if flags else "")
            )
    return "\n".join(lines), rows


def _promo_detail_keyboard(code: str, promo: dict) -> InlineKeyboardMarkup:
    is_active = int(promo.get("active") or 0) == 1
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔴 Выключить" if is_active else "🟢 Включить", callback_data=f"admin:promo_toggle:{code}")],
        [InlineKeyboardButton(text="📆 Срок действия", callback_data=f"admin:promo_edit:expires_at:{code}")],
        [InlineKeyboardButton(text="👤 Лимит на пользователя", callback_data=f"admin:promo_edit:user_limit:{code}")],
        [InlineKeyboardButton(text="🔢 Общий лимит", callback_data=f"admin:promo_edit:max_uses:{code}")],
        [InlineKeyboardButton(text="👁 Кто использовал", callback_data=f"admin:promo_usage:{code}")],
        [InlineKeyboardButton(text="⬅️ К промокодам", callback_data="admin:promo_menu")],
    ])


def _build_promo_detail_text(promo: dict) -> str:
    discount_type = str(promo.get("discount_type") or "percent")
    discount_label = f"{float(promo.get('fixed_amount') or 0):.0f} ₽" if discount_type == "fixed" else f"{float(promo.get('discount_percent') or 0):.0f}%"
    expires_at = str(promo.get("expires_at") or "").strip() or "не ограничен"
    plan_ids = str(promo.get("plan_ids") or "").strip() or "все тарифы"
    user_limit = int(promo.get("user_limit") or 0)
    return (
        f"🏷 <b>Промокод {promo.get('code')}</b>\n\n"
        f"Название: <b>{promo.get('title') or '-'}</b>\n"
        f"Скидка: <b>{discount_label}</b>\n"
        f"Статус: <b>{'активен' if int(promo.get('active') or 0) == 1 else 'выключен'}</b>\n"
        f"Общий лимит: <b>{int(promo.get('max_uses') or 0) or 'без лимита'}</b>\n"
        f"Лимит на пользователя: <b>{user_limit or 'без лимита'}</b>\n"
        f"Только новым: <b>{'да' if int(promo.get('only_new_users') or 0) == 1 else 'нет'}</b>\n"
        f"Тарифы: <b>{plan_ids}</b>\n"
        f"Срок действия: <b>{expires_at}</b>\n"
        f"Использовано: <b>{int(promo.get('used_count') or 0)}</b>\n"
        f"Описание: <i>{promo.get('description') or '-'}</i>"
    )


def _write_env_variable(key: str, value: str) -> None:
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    lines = []
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as file:
            lines = file.read().splitlines()
    replaced = False
    for idx, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[idx] = f"{key}={value}"
            replaced = True
            break
    if not replaced:
        lines.append(f"{key}={value}")
    with open(env_path, "w", encoding="utf-8") as file:
        file.write("\n".join(lines) + "\n")



def _ref_settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🎁 Бонус днями: {Config.REF_BONUS_DAYS}", callback_data="admin:refedit:REF_BONUS_DAYS")],
        [InlineKeyboardButton(text=f"1️⃣ Уровень 1: {Config.REF_PERCENT_LEVEL1}%", callback_data="admin:refedit:REF_PERCENT_LEVEL1")],
        [InlineKeyboardButton(text=f"2️⃣ Уровень 2: {Config.REF_PERCENT_LEVEL2}%", callback_data="admin:refedit:REF_PERCENT_LEVEL2")],
        [InlineKeyboardButton(text=f"3️⃣ Уровень 3: {Config.REF_PERCENT_LEVEL3}%", callback_data="admin:refedit:REF_PERCENT_LEVEL3")],
        [InlineKeyboardButton(text=f"💸 Мин. вывод: {Config.MIN_WITHDRAW} ₽", callback_data="admin:refedit:MIN_WITHDRAW")],
        [InlineKeyboardButton(text="🎯 Индивидуальные условия", callback_data="admin:partner_rates_prompt")],
        [InlineKeyboardButton(text="💰 Корректировка баланса", callback_data="admin:partner_balance_prompt")],
        [InlineKeyboardButton(text="🚨 Suspicious referrals", callback_data="admin:ref_suspicious")],
        [InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")],
    ])


def _set_config_value(key: str, value):
    if key == "REF_BONUS_DAYS":
        Config.REF_BONUS_DAYS = int(value)
    elif key == "REF_PERCENT_LEVEL1":
        Config.REF_PERCENT_LEVEL1 = float(value)
    elif key == "REF_PERCENT_LEVEL2":
        Config.REF_PERCENT_LEVEL2 = float(value)
    elif key == "REF_PERCENT_LEVEL3":
        Config.REF_PERCENT_LEVEL3 = float(value)
    elif key == "MIN_WITHDRAW":
        Config.MIN_WITHDRAW = float(value)


async def _render_ref_settings(message_obj):
    text = (
        "🤝 <b>Настройки реферальной системы</b>\n\n"
        f"🎁 Бонус днями: <b>{Config.REF_BONUS_DAYS}</b>\n"
        f"1️⃣ Уровень 1: <b>{Config.REF_PERCENT_LEVEL1}%</b>\n"
        f"2️⃣ Уровень 2: <b>{Config.REF_PERCENT_LEVEL2}%</b>\n"
        f"3️⃣ Уровень 3: <b>{Config.REF_PERCENT_LEVEL3}%</b>\n"
        f"💸 Минимальный вывод: <b>{Config.MIN_WITHDRAW} ₽</b>"
    )
    await smart_edit_message(message_obj, text, reply_markup=_ref_settings_keyboard(), parse_mode="HTML")


async def _process_withdraw(callback: CallbackQuery, db: Database, *, accept: bool) -> None:
    request_id = int(callback.data.split(":", 1)[1])
    request = await db.get_withdraw_request(request_id)
    success = await db.process_withdraw_request(request_id, accept=accept)
    if not success:
        await callback.answer("Ошибка обработки запроса", show_alert=True)
        return

    status_text = "✅ <b>ВЫВОД ПОДТВЕРЖДЁН</b>" if accept else "❌ <b>ВЫВОД ОТКЛОНЁН</b>"
    await smart_edit_message(callback.message, callback.message.text + f"\n\n{status_text}", parse_mode="HTML")
    if request:
        text = (
            "✅ <b>Ваш запрос на вывод подтверждён.</b>\n\n"
            f"🆔 Запрос: <code>{request_id}</code>\n"
            f"💰 Сумма: <b>{float(request['amount']):.2f} ₽</b>"
        ) if accept else (
            "❌ <b>Ваш запрос на вывод отклонён.</b>\n\n"
            f"🆔 Запрос: <code>{request_id}</code>\n"
            "Средства остались на вашем балансе."
        )
        await notify_user(int(request["user_id"]), text, bot=callback.bot)
    await callback.answer("Вывод подтверждён" if accept else "Вывод отклонён")

def save_tariffs(plans) -> None:
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data",
        "tarifs.json",
    )
    with open(path, "w", encoding="utf-8") as file:
        json.dump({"plans": plans}, file, ensure_ascii=False, indent=2)
    from tariffs.loader import load_tariffs
    load_tariffs()


@router.message(F.text == "🛠️ Админ меню")
@router.message(Command("admin"))
async def admin_menu(message: Message, bot: Bot):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await replace_message(
            user_id,
            "⛔ У вас нет прав администратора.",
            reply_markup=main_menu_keyboard(False),
            delete_user_msg=message,
            bot=bot,
        )
        return
    await replace_message(
        user_id,
        (
            "🛠️ <b>Админ панель</b>\n\n"
            "Разделы собраны по задачам:\n"
            "• 🧭 Сводка\n"
            "• 👥 Пользователи\n"
            "• 💳 Платежи\n"
            "• 📈 Аналитика\n"
            "• 📝 Контент и продажи\n"
            "• ⚙️ Система и панель"
        ),
        reply_markup=admin_menu_keyboard(),
        delete_user_msg=message,
        bot=bot,
    )


@router.message(F.text == "📊 Статистика")
async def admin_stats(message: Message, db: Database, bot: Bot):
    user_id = message.from_user.id
    if not is_admin(user_id):
        return
    total_users = await db.get_total_users()
    subscribed = len(await db.get_subscribed_user_ids())
    banned = await db.get_banned_users_count()
    text = (
        "📊 <b>Статистика бота</b>\n\n"
        f"👥 Всего пользователей: {total_users}\n"
        f"✅ Активных VPN: {subscribed}\n"
        f"⛔ Заблокировано: {banned}"
    )
    await replace_message(user_id, text, reply_markup=admin_menu_keyboard(), delete_user_msg=message, bot=bot)


@router.callback_query(F.data == "admin:promo_menu")
async def admin_promo_menu(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    text, rows = await _build_promo_menu_text(db)
    await smart_edit_message(callback.message, text, reply_markup=_promo_menu_keyboard(rows), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data.startswith("admin:promo_view:"))
async def admin_promo_view(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    code = callback.data.split(":", 3)[3].strip().upper()
    promo = await db.get_promo_code(code)
    if not promo:
        await callback.answer("Промокод не найден", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        _build_promo_detail_text(promo),
        reply_markup=_promo_detail_keyboard(code, promo),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:promo_add_prompt")
async def admin_promo_add_prompt(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await state.set_state(PromoCodeFSM.content)
    await smart_edit_message(
        callback.message,
        (
            "🏷 <b>Новый промокод</b>\n\n"
            "Отправьте строку в формате:\n"
            "<code>CODE | TYPE | VALUE | MAX_USES | USER_LIMIT | PLAN_IDS | NEW_ONLY | TITLE</code>\n\n"
            "Где:\n"
            "• TYPE: <code>percent</code> или <code>fixed</code>\n"
            "• VALUE: процент скидки или фиксированная сумма в рублях\n"
            "• USER_LIMIT: сколько раз один пользователь может применить код\n"
            "• PLAN_IDS: список тарифов через запятую или пусто\n"
            "• NEW_ONLY: <code>1</code> только для новых, <code>0</code> для всех\n\n"
            "Пример:\n"
            "<code>SPRING25 | percent | 25 | 100 | 1 | basic,pro | 0 | Весенняя акция</code>"
        ),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin:promo_menu")]]),
    )
    await callback.answer()


@router.message(PromoCodeFSM.content)
async def admin_promo_add_save(message: Message, state: FSMContext, db: Database):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    raw_text = (message.text or "").strip()
    if "|" in raw_text:
        parts = [part.strip() for part in raw_text.split("|")]
        if len(parts) < 8:
            await message.answer("❌ Формат: CODE | TYPE | VALUE | MAX_USES | USER_LIMIT | PLAN_IDS | NEW_ONLY | TITLE")
            return
        code = parts[0].upper()
        discount_type = parts[1].lower()
        plan_ids = parts[5]
        only_new_users = parts[6] in {"1", "true", "yes", "да"}
        title = parts[7] or f"Промокод {code}"
        try:
            value = float(parts[2].replace(",", "."))
            max_uses = int(parts[3])
            user_limit = int(parts[4])
        except ValueError:
            await message.answer("❌ Не удалось разобрать VALUE, MAX_USES или USER_LIMIT.")
            return
        if discount_type not in {"percent", "fixed"}:
            await message.answer("❌ TYPE должен быть percent или fixed.")
            return
        if value <= 0:
            await message.answer("❌ VALUE должен быть больше 0.")
            return
        discount = value if discount_type == "percent" else 0.0
        fixed_amount = value if discount_type == "fixed" else 0.0
        if discount_type == "percent" and discount >= 100:
            await message.answer("❌ Процентная скидка должна быть меньше 100.")
            return
    else:
        parts = raw_text.split(maxsplit=3)
        if len(parts) < 3:
            await message.answer("❌ Формат: CODE DISCOUNT MAX_USES [TITLE]")
            return
        code = parts[0].strip().upper()
        title = parts[3].strip() if len(parts) > 3 else f"Промокод {code}"
        try:
            discount = float(parts[1].replace(",", "."))
            max_uses = int(parts[2])
        except ValueError:
            await message.answer("❌ Не удалось разобрать скидку или лимит.")
            return
        if discount <= 0 or discount >= 100:
            await message.answer("❌ Скидка должна быть больше 0 и меньше 100.")
            return
        discount_type = "percent"
        fixed_amount = 0.0
        only_new_users = False
        plan_ids = ""
        user_limit = 0
    await db.create_or_update_promo_code(
        code,
        title=title,
        description=f"Создан администратором {message.from_user.id}",
        discount_percent=discount,
        discount_type=discount_type,
        fixed_amount=fixed_amount,
        only_new_users=only_new_users,
        plan_ids=plan_ids,
        user_limit=user_limit,
        max_uses=max_uses,
        active=True,
    )
    await state.clear()
    text, rows = await _build_promo_menu_text(db)
    await message.answer(text, reply_markup=_promo_menu_keyboard(rows), parse_mode="HTML")


@router.callback_query(F.data.startswith("admin:promo_toggle:"))
async def admin_promo_toggle(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    code = callback.data.split(":", 3)[3].strip().upper()
    promo = await db.get_promo_code(code)
    if not promo:
        await callback.answer("Промокод не найден", show_alert=True)
        return
    await db.create_or_update_promo_code(
        code,
        title=str(promo.get("title") or ""),
        description=str(promo.get("description") or ""),
        discount_percent=float(promo.get("discount_percent") or 0.0),
        discount_type=str(promo.get("discount_type") or "percent"),
        fixed_amount=float(promo.get("fixed_amount") or 0.0),
        only_new_users=int(promo.get("only_new_users") or 0) == 1,
        plan_ids=str(promo.get("plan_ids") or ""),
        user_limit=int(promo.get("user_limit") or 0),
        max_uses=int(promo.get("max_uses") or 0),
        active=int(promo.get("active") or 0) != 1,
        expires_at=promo.get("expires_at"),
    )
    updated = await db.get_promo_code(code)
    await smart_edit_message(callback.message, _build_promo_detail_text(updated), reply_markup=_promo_detail_keyboard(code, updated), parse_mode="HTML")
    await callback.answer("Статус промокода обновлён")


@router.callback_query(F.data.startswith("admin:promo_edit:"))
async def admin_promo_edit_prompt(callback: CallbackQuery, state: FSMContext, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    _, _, field, code = callback.data.split(":", 3)
    promo = await db.get_promo_code(code.upper())
    if not promo:
        await callback.answer("Промокод не найден", show_alert=True)
        return
    await state.set_state(PromoCodeFSM.edit_value)
    await state.update_data(promo_code=code.upper(), promo_field=field)
    field_hint = {
        "expires_at": "Отправьте дату в формате YYYY-MM-DD или слово <code>none</code>.",
        "user_limit": "Отправьте число. <code>0</code> = без лимита на пользователя.",
        "max_uses": "Отправьте число. <code>0</code> = без общего лимита.",
    }.get(field, "Отправьте новое значение.")
    await smart_edit_message(
        callback.message,
        f"✏️ <b>Редактирование {code.upper()}</b>\n\n{field_hint}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"admin:promo_view:{code.upper()}")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(PromoCodeFSM.edit_value)
async def admin_promo_edit_save(message: Message, state: FSMContext, db: Database):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    code = str(data.get("promo_code") or "").upper()
    field = str(data.get("promo_field") or "")
    promo = await db.get_promo_code(code)
    if not promo:
        await state.clear()
        await message.answer("❌ Промокод не найден.")
        return
    raw = (message.text or "").strip()
    kwargs = dict(
        title=str(promo.get("title") or ""),
        description=str(promo.get("description") or ""),
        discount_percent=float(promo.get("discount_percent") or 0.0),
        discount_type=str(promo.get("discount_type") or "percent"),
        fixed_amount=float(promo.get("fixed_amount") or 0.0),
        only_new_users=int(promo.get("only_new_users") or 0) == 1,
        plan_ids=str(promo.get("plan_ids") or ""),
        user_limit=int(promo.get("user_limit") or 0),
        max_uses=int(promo.get("max_uses") or 0),
        active=int(promo.get("active") or 0) == 1,
        expires_at=promo.get("expires_at"),
    )
    try:
        if field == "expires_at":
            kwargs["expires_at"] = None if raw.lower() in {"none", "0", "-"} else f"{raw}T23:59:59+00:00"
        elif field == "user_limit":
            kwargs["user_limit"] = max(0, int(raw))
        elif field == "max_uses":
            kwargs["max_uses"] = max(0, int(raw))
        else:
            await message.answer("❌ Неизвестное поле.")
            return
    except ValueError:
        await message.answer("❌ Не удалось разобрать значение.")
        return
    await db.create_or_update_promo_code(code, **kwargs)
    await state.clear()
    updated = await db.get_promo_code(code)
    await message.answer(_build_promo_detail_text(updated), reply_markup=_promo_detail_keyboard(code, updated), parse_mode="HTML")


@router.callback_query(F.data.startswith("admin:promo_usage:"))
async def admin_promo_usage(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    code = callback.data.split(":", 3)[3].strip().upper()
    rows = await db.get_promo_code_usage_details(code, limit=20) if hasattr(db, "get_promo_code_usage_details") else []
    lines = [f"👁 <b>Использование промокода {code}</b>", ""]
    if not rows:
        lines.append("Этот промокод пока никто не использовал.")
    else:
        for row in rows:
            lines.append(
                f"• <code>{row.get('user_id')}</code> — {int(row.get('used_count') or 0)} раз, последнее: <code>{row.get('last_used_at') or '-'}</code>"
            )
    await smart_edit_message(
        callback.message,
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К карточке промокода", callback_data=f"admin:promo_view:{code}")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:promo_stats")
async def admin_promo_stats(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    gift_stats = await db.get_gift_links_stats() if hasattr(db, "get_gift_links_stats") else {"total": 0, "claimed": 0, "unclaimed": 0}
    promo_stats = await db.get_promo_code_stats() if hasattr(db, "get_promo_code_stats") else []
    lines = [
        "📊 <b>Статистика промокодов и подарков</b>",
        "",
        f"🎁 Подарочных ссылок создано: <b>{int(gift_stats.get('total') or 0)}</b>",
        f"✅ Активировано: <b>{int(gift_stats.get('claimed') or 0)}</b>",
        f"⏳ Ещё не активировано: <b>{int(gift_stats.get('unclaimed') or 0)}</b>",
        "",
        "🏷 <b>Промокоды</b>",
    ]
    if not promo_stats:
        lines.append("Статистика по промокодам пока пустая.")
    else:
        for row in promo_stats[:10]:
            if str(row.get("discount_type") or "percent") == "fixed":
                discount_label = f"{float(row.get('fixed_amount') or 0):.0f} ₽"
            else:
                discount_label = f"{float(row.get('discount_percent') or 0):.0f}%"
            lines.append(
                f"• <code>{row.get('code')}</code> — {discount_label}, "
                f"покупок: <b>{int(row.get('payments_count') or 0)}</b>, "
                f"оборот: <b>{float(row.get('total_amount') or 0):.2f} ₽</b>, "
                f"использований: <b>{int(row.get('used_count') or 0)}</b>"
            )
    await smart_edit_message(
        callback.message,
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ К промокодам", callback_data="admin:promo_menu")],
        ]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(F.text == "💸 Запросы на вывод")
async def admin_withdraw_requests(message: Message, db: Database, bot: Bot):
    user_id = message.from_user.id
    if not is_admin(user_id):
        return
    requests = await db.get_pending_withdraw_requests()
    if not requests:
        await replace_message(
            user_id,
            "💸 Нет активных запросов на вывод.",
            reply_markup=admin_menu_keyboard(),
            delete_user_msg=message,
            bot=bot,
        )
        return

    await replace_message(
        user_id,
        "💸 Активные запросы на вывод:",
        reply_markup=admin_menu_keyboard(),
        delete_user_msg=message,
        bot=bot,
    )
    for request in requests:
        text = (
            f"📋 <b>Запрос #{request['id']}</b>\n"
            f"👤 Пользователь: <code>{request['user_id']}</code>\n"
            f"💰 Сумма: {request['amount']} ₽\n"
            f"🕐 Создан: {request['created_at']}"
        )
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"withdraw_accept:{request['id']}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"withdraw_reject:{request['id']}"),
            ]]
        )
        await bot.send_message(user_id, text, reply_markup=keyboard)


@router.message(F.text == "📦 Создать тестовую подписку")
async def admin_test_subscription(message: Message, bot: Bot):
    user_id = message.from_user.id
    if not is_admin(user_id):
        return
    plans = get_all_active()
    text = build_buy_text(plans)
    keyboard = [[InlineKeyboardButton(text=plan.get("name", plan.get("id")), callback_data=f"test:{plan.get('id')}")] for plan in plans]
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_admin")])
    await replace_message(
        user_id,
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
        delete_user_msg=message,
        bot=bot,
    )


@router.callback_query(F.data == "back_to_admin")
async def back_to_admin(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    if is_admin(user_id):
        await callback.message.delete()
        await replace_message(
            user_id,
            (
                "🛠️ <b>Админ панель</b>\n\n"
                "Разделы собраны по задачам:\n"
                "• 🧭 Сводка\n"
                "• 👥 Пользователи\n"
                "• 💳 Платежи\n"
                "• 📈 Аналитика\n"
                "• 📝 Контент и продажи\n"
                "• ⚙️ Система и панель"
            ),
            reply_markup=admin_menu_keyboard(),
            bot=bot,
        )
    await callback.answer()


@router.callback_query(F.data.startswith("withdraw_accept:"))
async def withdraw_accept(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await _process_withdraw(callback, db, accept=True)


@router.callback_query(F.data.startswith("withdraw_reject:"))
async def withdraw_reject(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await _process_withdraw(callback, db, accept=False)


@router.message(F.text == "📋 Тарифы")
async def admin_tariffs_list(message: Message, bot: Bot):
    user_id = message.from_user.id
    if not is_admin(user_id):
        return
    from tariffs.loader import TARIFFS_ALL
    await replace_message(
        user_id,
        "📋 <b>Редактор тарифов</b>\n\nВыберите тариф для редактирования:",
        reply_markup=tariffs_list_keyboard(list(TARIFFS_ALL)),
        delete_user_msg=message,
        bot=bot,
    )


@router.callback_query(F.data == "tlist")
async def tariffs_list_cb(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer()
        return
    from tariffs.loader import TARIFFS_ALL
    await smart_edit_message(callback.message, 
        "📋 <b>Редактор тарифов</b>\n\nВыберите тариф для редактирования:",
        reply_markup=tariffs_list_keyboard(list(TARIFFS_ALL)),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tedit:"))
async def tariff_edit_menu(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer()
        return
    plan_id = callback.data.split(":", 1)[1]
    plan = get_by_id(plan_id)
    if not plan:
        await callback.answer("Тариф не найден", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        _tariff_editor_text(plan, plan_id),
        reply_markup=tariff_fields_keyboard(plan_id),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ttoggle:"))
async def tariff_toggle(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer()
        return
    from tariffs.loader import TARIFFS_ALL
    plan_id = callback.data.split(":", 1)[1]
    plans = list(TARIFFS_ALL)
    for plan in plans:
        if plan.get("id") == plan_id:
            plan["active"] = not plan.get("active", True)
            await callback.answer("Тариф включён" if plan["active"] else "Тариф выключен")
            break
    save_tariffs(plans)
    from tariffs.loader import TARIFFS_ALL as reloaded_plans
    await smart_edit_message(callback.message, 
        "📋 <b>Редактор тарифов</b>\n\nВыберите тариф для редактирования:",
        reply_markup=tariffs_list_keyboard(list(reloaded_plans)),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("tfield:"))
async def tariff_field_select(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer()
        return
    _, plan_id, field = callback.data.split(":", 2)
    label = TARIFF_FIELDS.get(field, (field,))[0]
    await state.set_state(TariffEditFSM.value)
    await state.update_data(plan_id=plan_id, field=field, msg_id=callback.message.message_id)
    await smart_edit_message(callback.message, 
        f"✏️ Введите новое значение для поля <b>{label}</b>:\n(отправьте /cancel для отмены)",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data=f"tedit:{plan_id}")]]
        ),
    )
    await callback.answer()


@router.message(TariffEditFSM.value)
async def tariff_field_value(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    if message.text == "/cancel":
        await state.clear()
        await message.delete()
        return

    data = await state.get_data()
    plan_id = data["plan_id"]
    field = data["field"]
    _, cast = TARIFF_FIELDS[field]
    try:
        value = cast(message.text.strip())
    except (TypeError, ValueError):
        await message.answer("❌ Неверный формат. Попробуйте ещё раз.")
        return

    from tariffs.loader import TARIFFS_ALL
    plans = list(TARIFFS_ALL)
    for plan in plans:
        if plan.get("id") == plan_id:
            plan[field] = value
            break
    save_tariffs(plans)
    await state.clear()
    try:
        await message.delete()
    except Exception as exc:
        logger.debug("Не удалось удалить сообщение редактирования тарифа: %s", exc)

    plan = get_by_id(plan_id)
    await bot.send_message(
        user_id,
        _tariff_editor_text(plan, plan_id, success_prefix="✅ Сохранено!"),
        reply_markup=tariff_fields_keyboard(plan_id),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("tdelete:"))
async def tariff_delete(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer()
        return
    from tariffs.loader import TARIFFS_ALL
    plan_id = callback.data.split(":", 1)[1]
    save_tariffs([plan for plan in TARIFFS_ALL if plan.get("id") != plan_id])
    from tariffs.loader import TARIFFS_ALL as reloaded_plans
    await smart_edit_message(callback.message, 
        "🗑 Тариф удалён.\n\n📋 <b>Редактор тарифов</b>:",
        reply_markup=tariffs_list_keyboard(list(reloaded_plans)),
        parse_mode="HTML",
    )
    await callback.answer("Удалено")


@router.callback_query(F.data == "tadd")
async def tariff_add(callback: CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer()
        return
    new_id = f"plan_{int(time.time())}"
    from tariffs.loader import TARIFFS_ALL
    plans = list(TARIFFS_ALL)
    plans.append(
        {
            "id": new_id,
            "name": "Новый тариф",
            "active": False,
            "price_rub": 0,
            "duration_days": 30,
            "ip_limit": 1,
            "traffic_gb": 50,
            "sort": 999,
            "description": "",
            "price_stars": None,
        }
    )
    save_tariffs(plans)
    await smart_edit_message(callback.message, 
        "➕ Тариф создан (выключен). Отредактируйте его:",
        reply_markup=tariff_fields_keyboard(new_id),
        parse_mode="HTML",
    )
    await callback.answer()




@router.callback_query(F.data == "admin:stars_settings")
async def admin_stars_settings(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    stored_value = await db.get_setting(STARS_MULTIPLIER_SETTING_KEY, str(Config.TELEGRAM_STARS_PRICE_MULTIPLIER))
    try:
        effective_value = float(stored_value or Config.TELEGRAM_STARS_PRICE_MULTIPLIER)
    except (TypeError, ValueError):
        effective_value = float(Config.TELEGRAM_STARS_PRICE_MULTIPLIER)
    Config.set_stars_price_multiplier(effective_value)
    text = (
        "⭐ <b>Настройки Telegram Stars</b>\n\n"
        f"Текущий коэффициент: <b>{effective_value}</b>\n"
        "Если у тарифа не задана отдельная цена в Stars, используется этот коэффициент.\n\n"
        "Формула: price_rub × multiplier → Stars"
    )
    await smart_edit_message(callback.message, text, reply_markup=_stars_settings_keyboard(), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data == "admin:stars_multiplier")
async def admin_stars_multiplier_prompt(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await state.set_state(StarsSettingsFSM.multiplier)
    await smart_edit_message(callback.message, 
        "⭐ Введите новый коэффициент Telegram Stars\n\nНапример: <code>1.0</code> или <code>0.9</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin:stars_settings")]]),
    )
    await callback.answer()


@router.message(StarsSettingsFSM.multiplier)
async def admin_stars_multiplier_save(message: Message, state: FSMContext, bot: Bot, db: Database):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    try:
        value = float((message.text or '').replace(',', '.').strip())
        if value <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введите число больше 0. Например: 1.0")
        return
    Config.set_stars_price_multiplier(value)
    await db.set_setting(STARS_MULTIPLIER_SETTING_KEY, str(value))
    _write_env_variable("TELEGRAM_STARS_PRICE_MULTIPLIER", str(value))
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    await bot.send_message(
        message.from_user.id,
        (
            "✅ Коэффициент Telegram Stars обновлён\n\n"
            f"Новое значение: <b>{value}</b>"
        ),
        reply_markup=_stars_settings_keyboard(),
        parse_mode="HTML",
    )

@router.callback_query(F.data == "admin:stats")
async def admin_stats_callback(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    total_users = await db.get_total_users()
    subscribed = len(await db.get_subscribed_user_ids())
    banned = await db.get_banned_users_count()
    text = (
        "📊 <b>Статистика бота</b>\n\n"
        f"👥 Всего пользователей: {total_users}\n"
        f"✅ Активных VPN: {subscribed}\n"
        f"⛔ Заблокировано: {banned}"
    )
    await smart_edit_message(callback.message, text, reply_markup=_dashboard_back_keyboard(), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data == "admin:withdraw_requests")
async def admin_withdraw_requests_callback(callback: CallbackQuery, db: Database, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    requests = await db.get_pending_withdraw_requests()
    if not requests:
        await smart_edit_message(callback.message, 
            "💸 Нет активных запросов на вывод.",
            reply_markup=_dashboard_back_keyboard(),
        )
        await callback.answer()
        return
    summary = "💸 <b>Запросы на вывод</b>\n\n" + "\n".join(
        f"• <code>#{r['id']}</code> — user <code>{r['user_id']}</code> — <b>{r['amount']} ₽</b>" for r in requests[:20]
    )
    await smart_edit_message(callback.message, summary, reply_markup=_dashboard_back_keyboard(), parse_mode="HTML")
    for request in requests:
        text = (
            f"📋 <b>Запрос #{request['id']}</b>\n"
            f"👤 Пользователь: <code>{request['user_id']}</code>\n"
            f"💰 Сумма: {request['amount']} ₽\n"
            f"🕐 Создан: {request['created_at']}"
        )
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"withdraw_accept:{request['id']}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"withdraw_reject:{request['id']}"),
            ]]
        )
        await bot.send_message(callback.from_user.id, text, reply_markup=keyboard)
    await callback.answer()


@router.callback_query(F.data == "admin:tariffs")
async def admin_tariffs_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    from tariffs.loader import TARIFFS_ALL
    await smart_edit_message(callback.message, 
        "📋 <b>Редактор тарифов</b>\n\nВыберите тариф для редактирования:",
        reply_markup=tariffs_list_keyboard(list(TARIFFS_ALL)),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:test_subscription")
async def admin_test_subscription_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    plans = get_all_active()
    text = build_buy_text(plans)
    keyboard = [[InlineKeyboardButton(text=plan.get("name", plan.get("id")), callback_data=f"test:{plan.get('id')}")] for plan in plans]
    keyboard.append([InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")])
    await smart_edit_message(callback.message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data == "admin:ref_settings")
async def admin_ref_settings(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await _render_ref_settings(callback.message)
    await callback.answer()


@router.callback_query(F.data.startswith("admin:refedit:"))
async def admin_ref_setting_prompt(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    key = callback.data.split(":", 2)[-1]
    prompts = {
        "REF_BONUS_DAYS": "Введите, сколько бонусных дней начислять за оплаченного реферала",
        "REF_PERCENT_LEVEL1": "Введите процент для 1 уровня",
        "REF_PERCENT_LEVEL2": "Введите процент для 2 уровня",
        "REF_PERCENT_LEVEL3": "Введите процент для 3 уровня",
        "MIN_WITHDRAW": "Введите минимальную сумму вывода в ₽",
    }
    await state.set_state(ReferralSettingsFSM.field)
    await state.update_data(ref_field=key)
    await smart_edit_message(callback.message, 
        f"🤝 {prompts.get(key, 'Введите новое значение')}\n\nТекущее значение: <code>{getattr(Config, key)}</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin:ref_settings")]]),
    )
    await callback.answer()


@router.message(ReferralSettingsFSM.field)
async def admin_ref_setting_save(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    key = data.get("ref_field")
    raw = (message.text or "").replace(",", ".").strip()
    try:
        value = float(raw) if key != "REF_BONUS_DAYS" else int(float(raw))
        if value < 0 or (key == "MIN_WITHDRAW" and value <= 0):
            raise ValueError
    except Exception:
        await message.answer("❌ Введите корректное положительное число")
        return
    _set_config_value(key, value)
    _write_env_variable(key, str(int(value) if key == "REF_BONUS_DAYS" else value))
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    msg = await bot.send_message(message.from_user.id, "✅ Настройки реферальной системы обновлены")
    await _render_ref_settings(msg)


@router.callback_query(F.data == "admin:partner_rates_prompt")
async def admin_partner_rates_prompt(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await state.set_state(PartnerSettingsFSM.rates)
    await smart_edit_message(callback.message, 
        """🎯 <b>Индивидуальные условия партнёра</b>

Отправьте строку в формате:
<code>user_id level1 level2 level3 status note</code>

Пример:
<code>123456789 30 12 7 vip Сильный партнёр</code>

Вместо процента можно указать <code>-</code>, чтобы вернуть стандарт.
status: standard / partner / vip / ambassador""",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='❌ Отмена', callback_data='admin:ref_settings')]]),
    )
    await callback.answer()


@router.message(PartnerSettingsFSM.rates)
async def admin_partner_rates_save(message: Message, state: FSMContext, db: Database, bot: Bot):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    parts = (message.text or '').split(maxsplit=5)
    if len(parts) < 5:
        await message.answer('❌ Формат: user_id level1 level2 level3 status [note]')
        return
    try:
        user_id = int(parts[0])
        def parse_pct(raw):
            raw = raw.strip()
            return None if raw in {'-', 'none', 'auto'} else float(raw.replace(',', '.'))
        l1 = parse_pct(parts[1]); l2 = parse_pct(parts[2]); l3 = parse_pct(parts[3])
        status = parts[4].strip().lower()
        note = parts[5].strip() if len(parts) > 5 else ''
    except Exception:
        await message.answer('❌ Не удалось разобрать строку. Пример: 123456789 30 12 7 vip Сильный партнёр')
        return
    await db.add_user(user_id)
    await db.set_partner_rates(user_id, l1, l2, l3, status=status, note=note)
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    msg = await bot.send_message(message.from_user.id, f'✅ Индивидуальные условия обновлены для <code>{user_id}</code>', parse_mode='HTML')
    await _render_ref_settings(msg)


@router.callback_query(F.data == "admin:partner_balance_prompt")
async def admin_partner_balance_prompt(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await state.set_state(PartnerSettingsFSM.balance)
    await smart_edit_message(callback.message, 
        """💰 <b>Ручная корректировка баланса партнёра</b>

Отправьте строку в формате:
<code>user_id amount reason</code>

Пример: <code>123456789 250 бонус за кампанию</code>
Для списания используйте отрицательную сумму.""",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='❌ Отмена', callback_data='admin:ref_settings')]]),
    )
    await callback.answer()


@router.message(PartnerSettingsFSM.balance)
async def admin_partner_balance_save(message: Message, state: FSMContext, db: Database, bot: Bot):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    parts = (message.text or '').split(maxsplit=2)
    if len(parts) < 3:
        await message.answer('❌ Формат: user_id amount reason')
        return
    try:
        user_id = int(parts[0]); amount = float(parts[1].replace(',', '.')); reason = parts[2].strip()
    except Exception:
        await message.answer('❌ Не удалось разобрать строку. Пример: 123456789 250 бонус за кампанию')
        return
    await db.add_user(user_id)
    await db.add_referral_balance_adjustment(user_id, message.from_user.id, amount, reason)
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    msg = await bot.send_message(message.from_user.id, f'✅ Баланс партнёра <code>{user_id}</code> скорректирован на <b>{amount:.2f} ₽</b>', parse_mode='HTML')
    await _render_ref_settings(msg)


@router.callback_query(F.data == "admin:ref_suspicious")
async def admin_ref_suspicious(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer('⛔ Недостаточно прав', show_alert=True)
        return
    rows = await db.get_suspicious_referrals(limit=20) if hasattr(db, 'get_suspicious_referrals') else []
    text = '🚨 <b>Подозрительные реферальные кейсы</b>\n\n'
    if not rows:
        text += 'Подозрительных кейсов пока нет.'
    else:
        for row in rows:
            text += f"• user <code>{row.get('user_id')}</code> ← ref <code>{row.get('ref_by') or 0}</code>\n  {row.get('partner_note') or 'без заметки'}\n"
    await smart_edit_message(callback.message, text, parse_mode='HTML', reply_markup=_ref_settings_keyboard())
    await callback.answer()


@router.callback_query(F.data == "admin:main_message")
async def admin_main_message_prompt(callback: CallbackQuery, state: FSMContext, db: Database):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(MainMessageFSM.content)
    text = await db.get_setting("main_message_text", "") if hasattr(db, "get_setting") else ""
    has_photo = bool(await db.get_setting("main_message_photo", "")) if hasattr(db, "get_setting") else False
    msg = (
        "🖼 <b>Главное сообщение</b>\n\n"
        "Отправьте новый текст или фото с подписью следующим сообщением.\n"
        f"Изображение сейчас: <b>{'есть' if has_photo else 'нет'}</b>\n\n"
        f"Текущий текст:\n{text[:500] if text else 'стандартный'}"
    )
    await smart_edit_message(callback.message, msg, parse_mode="HTML")
    await callback.answer()


@router.message(MainMessageFSM.content)
async def admin_main_message_save_real(message: Message, state: FSMContext, db: Database):
    if not is_admin(message.from_user.id):
        return
    text = message.caption or message.text or ""
    photo = message.photo[-1].file_id if message.photo else ""
    if hasattr(db, "set_setting"):
        await db.set_setting("main_message_text", text)
        if photo:
            await db.set_setting("main_message_photo", photo)
    await state.clear()
    await message.answer("✅ Главное сообщение обновлено.")


def _templates_menu_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=template_title(key), callback_data=f"admin:template:{key}")] for key in TEMPLATES.keys()]
    rows.append([InlineKeyboardButton(text="ℹ️ Переменные шаблонов", callback_data="admin:template_vars")])
    rows.append([InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")])
    return InlineKeyboardMarkup(inline_keyboard=rows)



def _template_preview_keyboard(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить", callback_data=f"admin:template_edit:{key}")],
        [InlineKeyboardButton(text="🗑 Убрать фото", callback_data=f"admin:template_clear_photo:{key}")],
        [InlineKeyboardButton(text="↩️ Сбросить в стандарт", callback_data=f"admin:template_reset:{key}")],
        [InlineKeyboardButton(text="⬅️ К шаблонам", callback_data="admin:templates")],
    ])



def _template_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="admin:template_confirm")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin:template_cancel")],
    ])


async def _render_template_item(message_obj, db: Database, key: str):
    text, photo = await get_template_content(db, key)
    preview = text if len(text) <= 1000 else text[:1000] + "..."
    vars_for_key = template_variables(key)
    vars_text = ", ".join(f"<code>{{{name}}}</code>" for name in vars_for_key) if vars_for_key else "нет"
    msg = (
        f"📝 <b>{template_title(key)}</b>\n\n"
        f"Ключ: <code>{key}</code>\n"
        f"Фото: <b>{'да' if photo else 'нет'}</b>\n"
        f"Можно прикреплять фото: <b>{'да' if template_allow_photo(key) else 'нет'}</b>\n\n"
        f"<b>Переменные:</b> {vars_text}\n\n"
        f"<b>Текущий текст:</b>\n{preview}"
    )
    await smart_edit_message(message_obj, msg, reply_markup=_template_preview_keyboard(key), parse_mode="HTML")


@router.callback_query(F.data == "admin:templates")
async def admin_templates_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await smart_edit_message(
        callback.message,
        "📝 <b>Шаблоны сообщений</b>\n\nВыберите сообщение для редактирования.",
        reply_markup=_templates_menu_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:template_vars")
async def admin_template_variables_menu(callback: CallbackQuery):
    rows = template_variables_map()
    lines = [
        "ℹ️ <b>Переменные в шаблонах</b>",
        "",
        "Используйте только переменные из списка. Если для шаблона указано «нет», подстановок там нет.",
    ]
    for key in TEMPLATES.keys():
        variables = rows.get(key) or []
        title = template_title(key)
        if not variables:
            lines.append(f"\n• <b>{title}</b> (<code>{key}</code>): нет")
            continue
        vars_text = ", ".join(f"<code>{{{name}}}</code>" for name in variables)
        lines.append(f"\n• <b>{title}</b> (<code>{key}</code>): {vars_text}")
    await smart_edit_message(
        callback.message,
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ К шаблонам", callback_data="admin:templates")],
            ]
        ),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin:template:"))
async def admin_template_item(callback: CallbackQuery, db: Database, state: FSMContext):
    await state.clear()
    key = callback.data.split(":", 2)[2]
    await _render_template_item(callback.message, db, key)
    await callback.answer()


@router.callback_query(F.data.startswith("admin:template_edit:"))
async def admin_template_edit_prompt(callback: CallbackQuery, state: FSMContext):
    key = callback.data.split(":", 2)[2]
    await state.set_state(TemplateEditFSM.content)
    await state.update_data(template_key=key)
    await smart_edit_message(
        callback.message,
        (
            f"✏️ <b>{template_title(key)}</b>\n\n"
            "Отправьте новый текст или фото с подписью.\n"
            "HTML-форматирование поддерживается.\n"
            "Напишите <code>/clearphoto</code>, чтобы убрать картинку и оставить только текст.\n\n"
            "После этого бот попросит подтверждение изменения."
        ),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:template_cancel")
async def admin_template_cancel(callback: CallbackQuery, state: FSMContext, db: Database):
    data = await state.get_data()
    key = data.get("template_key")
    await state.clear()
    if key:
        await _render_template_item(callback.message, db, key)
    else:
        await smart_edit_message(
            callback.message,
            "📝 <b>Шаблоны сообщений</b>\n\nВыберите сообщение для редактирования.",
            reply_markup=_templates_menu_keyboard(),
            parse_mode="HTML",
        )
    await callback.answer("Редактирование отменено")


@router.message(TemplateEditFSM.content)
async def admin_template_edit_receive(message: Message, state: FSMContext, db: Database):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    key = data.get("template_key")
    if not key:
        await state.clear()
        return
    current_text, current_photo = await get_template_content(db, key)
    pending_text = (message.text or message.caption or "").strip()
    pending_photo = current_photo
    if (message.text or "").strip() == "/clearphoto":
        pending_text = current_text
        pending_photo = ""
    elif message.photo:
        if not template_allow_photo(key):
            await message.answer("Для этого шаблона картинка не поддерживается.")
            return
        pending_photo = message.photo[-1].file_id
        if not pending_text:
            pending_text = current_text
    elif pending_text:
        pass
    else:
        await message.answer("Отправьте текст или фото с подписью.")
        return

    await state.set_state(TemplateEditFSM.confirm)
    await state.update_data(template_key=key, pending_text=pending_text, pending_photo=pending_photo)
    preview = f"✅ <b>Подтвердите изменение шаблона</b>\n\n<b>{template_title(key)}</b>\n\n{pending_text}"
    if pending_photo and template_allow_photo(key):
        await message.answer_photo(pending_photo, caption=preview, parse_mode="HTML", reply_markup=_template_confirm_keyboard())
    else:
        await message.answer(preview, parse_mode="HTML", reply_markup=_template_confirm_keyboard())


@router.callback_query(F.data == "admin:template_confirm")
async def admin_template_confirm(callback: CallbackQuery, state: FSMContext, db: Database):
    data = await state.get_data()
    key = data.get("template_key")
    if not key:
        await callback.answer("Нет данных шаблона", show_alert=True)
        return
    await db.set_setting(f"template:{key}:text", data.get("pending_text", template_default_text(key)))
    await db.set_setting(f"template:{key}:photo", data.get("pending_photo", ""))
    await state.clear()
    await _render_template_item(callback.message, db, key)
    await callback.answer("Шаблон обновлён")


@router.callback_query(F.data.startswith("admin:template_clear_photo:"))
async def admin_template_clear_photo(callback: CallbackQuery, db: Database):
    key = callback.data.split(":", 3)[3]
    await db.set_setting(f"template:{key}:photo", "")
    await _render_template_item(callback.message, db, key)
    await callback.answer("Фото убрано")


@router.callback_query(F.data.startswith("admin:template_reset:"))
async def admin_template_reset(callback: CallbackQuery, db: Database):
    key = callback.data.split(":", 3)[3]
    await db.set_setting(f"template:{key}:text", template_default_text(key))
    await db.set_setting(f"template:{key}:photo", "")
    await _render_template_item(callback.message, db, key)
    await callback.answer("Сброшено")
