from aiogram import Bot, F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.runtime_settings import REF_SETTING_KEYS, STARS_MULTIPLIER_SETTING_KEY
from config import Config
from db import Database
from handlers.admin import PartnerSettingsFSM, ReferralSettingsFSM, StarsSettingsFSM, is_admin
from handlers.admin_settings_helpers import (
    _build_ref_audit_keyboard,
    _build_ref_audit_text,
    _ref_settings_keyboard,
    _render_ref_settings,
    _set_config_value,
    _stars_settings_keyboard,
    _write_env_variable,
)
from utils.telegram_ui import smart_edit_message

router = Router()


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
    await smart_edit_message(
        callback.message,
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
        "✅ Коэффициент Telegram Stars обновлён\n\n"
        f"Новое значение: <b>{value}</b>",
        reply_markup=_stars_settings_keyboard(),
        parse_mode="HTML",
    )


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
    await smart_edit_message(
        callback.message,
        f"🤝 {prompts.get(key, 'Введите новое значение')}\n\nТекущее значение: <code>{getattr(Config, key)}</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin:ref_settings")]]),
    )
    await callback.answer()


@router.message(ReferralSettingsFSM.field)
async def admin_ref_setting_save(message: Message, state: FSMContext, bot: Bot, db: Database):
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
    setting_key = REF_SETTING_KEYS.get(str(key))
    if setting_key:
        await db.set_setting(setting_key, str(int(value) if key == "REF_BONUS_DAYS" else value))
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
    await smart_edit_message(
        callback.message,
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
    await smart_edit_message(
        callback.message,
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


@router.callback_query(F.data == "admin:ref_audit")
async def admin_ref_audit(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    text = await _build_ref_audit_text(db)
    users = await db.get_all_users() if hasattr(db, "get_all_users") else []
    referred = []
    for row in users:
        ref_by = int(row.get("ref_by") or 0)
        if ref_by > 0:
            referred.append(row)
    referred.sort(key=lambda item: str(item.get("join_date") or ""), reverse=True)
    gifts = await db.list_recent_claimed_gift_links(limit=10) if hasattr(db, "list_recent_claimed_gift_links") else []
    await smart_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=_build_ref_audit_keyboard(referred, gifts),
    )
    await callback.answer()
