import logging
from typing import Optional

from aiogram import Router, F, Bot
from aiogram.enums import ParseMode
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton

from config import Config
from db import Database
from keyboards import referral_inline_keyboard, simple_back_to_referral_keyboard
from utils.helpers import replace_message, notify_admins, get_ref_link
from services.panel import PanelAPI
from utils.telegram_ui import smart_edit_message
from utils.templates import render_template

logger = logging.getLogger(__name__)
router = Router()


def _format_percent(value: float) -> str:
    val = float(value or 0.0)
    return str(int(val)) if val.is_integer() else f"{val:.2f}".rstrip("0").rstrip(".")


def _resolve_referral_rates(summary: dict) -> tuple[float, float, float, bool]:
    level1 = summary.get("custom_percent_level1")
    level2 = summary.get("custom_percent_level2")
    level3 = summary.get("custom_percent_level3")
    rates = (
        float(level1 if level1 is not None else Config.REF_PERCENT_LEVEL1),
        float(level2 if level2 is not None else Config.REF_PERCENT_LEVEL2),
        float(level3 if level3 is not None else Config.REF_PERCENT_LEVEL3),
    )
    has_custom = any(value is not None for value in (level1, level2, level3))
    return rates[0], rates[1], rates[2], has_custom


@router.message(F.text == "🤝 Реферальная система")
async def referral_menu(message: Message, db: Database, panel: PanelAPI):
    user_id = message.from_user.id
    await db.add_user(user_id)
    await show_referral_menu(user_id, db=db, panel=panel, bot=message.bot, user_msg=message)


@router.callback_query(F.data == "user_menu:referral")
async def referral_menu_callback(callback: CallbackQuery, db: Database, panel: PanelAPI):
    text, markup = await build_referral_screen(callback.from_user.id, db=db, panel=panel, bot=callback.bot)
    await smart_edit_message(callback.message, text, reply_markup=markup, parse_mode="HTML")
    await callback.answer()


@router.message(F.text == "💸 Вывести средства")
async def withdraw_money(message: Message, db: Database):
    await _process_withdraw_request(message.from_user.id, db=db, bot=message.bot, user_msg=message)


@router.callback_query(F.data == "referral:withdraw")
async def withdraw_money_callback(callback: CallbackQuery, db: Database):
    await _process_withdraw_request(callback.from_user.id, db=db, bot=callback.bot, callback=callback)


async def _process_withdraw_request(user_id: int, *, db: Database, bot: Bot, user_msg: Optional[Message] = None, callback: Optional[CallbackQuery] = None):
    balance = await db.get_balance(user_id)
    if balance < Config.MIN_WITHDRAW:
        text = f"❌ Минимальная сумма вывода: {Config.MIN_WITHDRAW} ₽. Ваш баланс: {balance:.2f} ₽."
        if callback:
            await smart_edit_message(callback.message, text, reply_markup=simple_back_to_referral_keyboard(), parse_mode="HTML")
            await callback.answer()
        else:
            await replace_message(user_id, text, reply_markup=simple_back_to_referral_keyboard(), delete_user_msg=user_msg, bot=bot)
        return

    existing_request = await db.get_user_pending_withdraw_request(user_id)
    if existing_request:
        text, _ = await render_template(
            db,
            "withdraw_request_exists_user",
            request_id=existing_request["id"],
            amount=float(existing_request["amount"]),
        )
        if callback:
            await smart_edit_message(callback.message, text, reply_markup=simple_back_to_referral_keyboard(), parse_mode="HTML")
            await callback.answer()
        else:
            await replace_message(user_id, text, reply_markup=simple_back_to_referral_keyboard(), delete_user_msg=user_msg, bot=bot)
        return

    request_id = await db.create_withdraw_request(user_id, balance)
    if request_id:
        await notify_admins(
            f"💸 <b>Новый запрос на вывод средств!</b>\n\n👤 Пользователь: <code>{user_id}</code>\n💰 Сумма: {balance:.2f} ₽\n🆔 ID запроса: {request_id}",
            bot=bot,
        )
        text, _ = await render_template(db, "withdraw_request_created_user", amount=balance)
    else:
        text = "❌ Ошибка при создании запроса. Попробуйте позже."
    if callback:
        await smart_edit_message(callback.message, text, reply_markup=simple_back_to_referral_keyboard(), parse_mode="HTML")
        await callback.answer()
    else:
        await replace_message(user_id, text, reply_markup=simple_back_to_referral_keyboard(), delete_user_msg=user_msg, bot=bot)


async def build_referral_screen(user_id: int, *, db: Database, panel: PanelAPI, bot: Optional[Bot] = None):
    user = await db.get_user(user_id)
    balance = float(user.get("balance", 0.0) or 0.0)
    ref_code = await db.ensure_ref_code(user_id)
    if not ref_code:
        return "❌ Не удалось сгенерировать реферальный код.", simple_back_to_referral_keyboard()

    bot_username = getattr(bot, "username", "") if bot else ""
    link = get_ref_link(ref_code, 2, bot_username=bot_username)
    summary = await db.get_referral_partner_cabinet(user_id) if hasattr(db, "get_referral_partner_cabinet") else await db.get_referral_summary(user_id)
    rate_l1, rate_l2, rate_l3, has_custom_rates = _resolve_referral_rates(summary)
    total_refs = int(summary.get("total_refs", 0) or 0)
    paid_refs = int(summary.get("paid_refs", 0) or 0)
    conversion = float(summary.get("conversion_pct", 0.0) or 0.0)

    source_label = "индивидуальные условия" if has_custom_rates else "стандартные условия"
    text = (
        "🤝 <b>Реферальная система VPN</b>\n\n"
        "У нас <b>3-уровневая реферальная система</b>:\n"
        f"• 1 уровень: <b>{_format_percent(rate_l1)}%</b>\n"
        f"• 2 уровень: <b>{_format_percent(rate_l2)}%</b>\n"
        f"• 3 уровень: <b>{_format_percent(rate_l3)}%</b>\n"
        f"Источник ставок: <b>{source_label}</b>\n\n"
        f"Ваш друг при первой покупке получает <b>скидку {int(Config.REF_FIRST_PAYMENT_DISCOUNT_PERCENT)}%</b> и <b>+{Config.REFERRED_BONUS_DAYS} дней</b> к тарифу. Это действует только на <b>первую оплату</b>.\n\n"
        f"🔗 Ваша ссылка:\n<blockquote>{link}</blockquote>\n\n"
        f"Всего приглашено: <b>{total_refs}</b>\n"
        f"Оплатили подписку: <b>{paid_refs}</b>\n"
        f"Конверсия: <b>{conversion:.1f}%</b>\n"
        f"Ваш баланс: <b>{balance:.2f} ₽</b>\n"
    )
    pending_withdraw = await db.get_user_pending_withdraw_request(user_id)
    if pending_withdraw:
        text += (
            "\n⏳ Активный вывод:\n"
            f"• ID: <code>{pending_withdraw['id']}</code>\n"
            f"• Сумма: <b>{float(pending_withdraw['amount']):.2f} ₽</b>\n"
        )

    return text, referral_inline_keyboard(balance=balance, min_withdraw=Config.MIN_WITHDRAW, is_admin=user_id in Config.ADMIN_USER_IDS)


async def show_referral_menu(user_id: int, db: Database, panel: PanelAPI, bot: Optional[Bot] = None, user_msg: Optional[Message] = None):
    text, markup = await build_referral_screen(user_id, db=db, panel=panel, bot=bot)
    await replace_message(user_id, text, reply_markup=markup, delete_user_msg=user_msg, bot=bot)


@router.message(F.text == "🔗 Получить ссылку")
async def get_ref_link_handler(message: Message, db: Database):
    await _show_ref_link(message.from_user.id, db=db, bot=message.bot, user_msg=message)


@router.callback_query(F.data == "referral:get_link")
async def get_ref_link_callback(callback: CallbackQuery, db: Database):
    await _show_ref_link(callback.from_user.id, db=db, bot=callback.bot, callback=callback)


async def _show_ref_link(user_id: int, *, db: Database, bot: Bot, user_msg: Optional[Message] = None, callback: Optional[CallbackQuery] = None):
    ref_code = await db.ensure_ref_code(user_id)
    if not ref_code:
        text = "❌ Не удалось сгенерировать реферальный код."
    else:
        link = get_ref_link(ref_code, 2, bot_username=getattr(bot, "username", ""))
        summary = await db.get_referral_partner_cabinet(user_id) if hasattr(db, "get_referral_partner_cabinet") else await db.get_referral_summary(user_id)
        rate_l1, rate_l2, rate_l3, has_custom_rates = _resolve_referral_rates(summary)
        source_label = "индивидуальные условия" if has_custom_rates else "стандартные условия"
        bonus_text = (
            "У нас 3-уровневая реферальная система:\n"
            f"• 1 уровень: <b>{_format_percent(rate_l1)}%</b>\n"
            f"• 2 уровень: <b>{_format_percent(rate_l2)}%</b>\n"
            f"• 3 уровень: <b>{_format_percent(rate_l3)}%</b>\n"
            f"Источник ставок: <b>{source_label}</b>\n\n"
            f"Ваш друг получит <b>скидку {int(Config.REF_FIRST_PAYMENT_DISCOUNT_PERCENT)}%</b> на первую оплату и <b>+{Config.REFERRED_BONUS_DAYS} дней</b> к тарифу."
        )
        text = f"🕊️ Отправь своему другу ссылку:\n\n<blockquote>{link}</blockquote>\n\n{bonus_text}"
    if callback:
        await smart_edit_message(callback.message, text, parse_mode=ParseMode.HTML, reply_markup=simple_back_to_referral_keyboard())
        await callback.answer()
    else:
        await replace_message(user_id, text, parse_mode=ParseMode.HTML, reply_markup=simple_back_to_referral_keyboard(), delete_user_msg=user_msg, bot=bot)


@router.message(F.text == "👥 Мои рефералы")
async def my_referrals_handler(message: Message, db: Database):
    await _show_my_referrals(message.from_user.id, db=db, bot=message.bot, user_msg=message)


@router.callback_query(F.data == "referral:list")
async def my_referrals_callback(callback: CallbackQuery, db: Database):
    await _show_my_referrals(callback.from_user.id, db=db, bot=callback.bot, callback=callback)


async def _show_my_referrals(user_id: int, *, db: Database, bot: Bot, user_msg: Optional[Message] = None, callback: Optional[CallbackQuery] = None):
    refs = await db.get_referrals_list(user_id)
    text = "😔 Вы ещё никого не пригласили." if not refs else "👥 <b>Ваши рефералы</b>\n\n"
    if refs:
        for r in refs:
            status = "✅ оплатил" if r.get("ref_rewarded") else "⏳ не оплатил"
            uid = r["user_id"]
            joined = str(r.get("join_date", ""))[:10]
            text += f"• <code>{uid}</code> — {status} (вступил {joined})\n"
    if callback:
        await smart_edit_message(callback.message, text, parse_mode=ParseMode.HTML, reply_markup=simple_back_to_referral_keyboard())
        await callback.answer()
    else:
        await replace_message(user_id, text, parse_mode=ParseMode.HTML, reply_markup=simple_back_to_referral_keyboard(), delete_user_msg=user_msg, bot=bot)


@router.message(F.text == "📊 История начислений")
async def ref_history_handler(message: Message, db: Database):
    await _show_ref_history(message.from_user.id, db=db, bot=message.bot, user_msg=message)


@router.callback_query(F.data == "referral:history")
async def ref_history_callback(callback: CallbackQuery, db: Database):
    await _show_ref_history(callback.from_user.id, db=db, bot=callback.bot, callback=callback)


async def _show_ref_history(user_id: int, *, db: Database, bot: Bot, user_msg: Optional[Message] = None, callback: Optional[CallbackQuery] = None):
    history = await db.get_ref_history(user_id, limit=10)
    text = "😔 История начислений пуста." if not history else "📊 <b>История начислений</b>\n\n"
    if history:
        for row in history:
            date = str(row.get("created_at", ""))[:10]
            if row.get("amount"):
                text += f"• {date} — <b>+{float(row['amount']):.2f} ₽</b> на баланс\n"
            elif row.get("bonus_days"):
                text += f"• {date} — <b>+{int(row['bonus_days'])} дней</b> подписки\n"
    if callback:
        await smart_edit_message(callback.message, text, parse_mode=ParseMode.HTML, reply_markup=simple_back_to_referral_keyboard())
        await callback.answer()
    else:
        await replace_message(user_id, text, parse_mode=ParseMode.HTML, reply_markup=simple_back_to_referral_keyboard(), delete_user_msg=user_msg, bot=bot)


@router.message(F.text == "🧾 История выводов")
async def withdraw_history_handler(message: Message, db: Database):
    await _show_withdraw_history(message.from_user.id, db=db, bot=message.bot, user_msg=message)


@router.callback_query(F.data == "referral:withdraw_history")
async def withdraw_history_callback(callback: CallbackQuery, db: Database):
    await _show_withdraw_history(callback.from_user.id, db=db, bot=callback.bot, callback=callback)


async def _show_withdraw_history(user_id: int, *, db: Database, bot: Bot, user_msg: Optional[Message] = None, callback: Optional[CallbackQuery] = None):
    history = await db.get_withdraw_requests_by_user(user_id, limit=10)
    status_map = {"pending": "⏳ на рассмотрении", "completed": "✅ подтверждён", "rejected": "❌ отклонён"}
    text = "😔 История выводов пуста." if not history else "🧾 <b>История выводов</b>\n\n"
    if history:
        for row in history:
            created = str(row.get("created_at", ""))[:16]
            status = status_map.get(row.get("status"), row.get("status", "—"))
            text += f"• Запрос <code>#{row['id']}</code> — <b>{float(row['amount']):.2f} ₽</b>\n  Статус: {status}\n  Создан: {created}\n"
    if callback:
        await smart_edit_message(callback.message, text, parse_mode=ParseMode.HTML, reply_markup=simple_back_to_referral_keyboard())
        await callback.answer()
    else:
        await replace_message(user_id, text, parse_mode=ParseMode.HTML, reply_markup=simple_back_to_referral_keyboard(), delete_user_msg=user_msg, bot=bot)


@router.callback_query(F.data == "referral:cabinet")
async def referral_partner_cabinet(callback: CallbackQuery, db: Database):
    summary = await db.get_referral_partner_cabinet(callback.from_user.id) if hasattr(db, "get_referral_partner_cabinet") else await db.get_referral_summary(callback.from_user.id)
    rate_l1, rate_l2, rate_l3, has_custom_rates = _resolve_referral_rates(summary)
    custom_parts = []
    for idx in (1, 2, 3):
        val = summary.get(f"custom_percent_level{idx}")
        if val is not None:
            custom_parts.append(f"L{idx}: {val}%")
    custom_text = ", ".join(custom_parts) if custom_parts else "стандартные условия"
    text = (
        "🤝 <b>Партнёрский кабинет</b>\n\n"
        f"Статус: <b>{summary.get('status', 'standard')}</b>\n"
        f"Условия: <b>{'индивидуальные' if has_custom_rates else 'стандартные'}</b>\n"
        f"Ставки: <b>{_format_percent(rate_l1)}% / {_format_percent(rate_l2)}% / {_format_percent(rate_l3)}%</b>\n\n"
        f"Всего приглашено: <b>{summary.get('total_refs', 0)}</b>\n"
        f"Пробный период подключили: <b>{summary.get('trial_refs', 0)}</b>\n"
        f"Оплатили: <b>{summary.get('paid_refs', 0)}</b>\n"
        f"Конверсия: <b>{summary.get('conversion_pct', 0.0)}%</b>\n"
        f"Заработано всего: <b>{summary.get('earned_rub', 0.0):.2f} ₽</b>\n"
        f"Доступно к выводу: <b>{summary.get('balance', 0.0):.2f} ₽</b>\n"
        f"Индивидуальные условия: <b>{custom_text}</b>\n"
        "\n\nЧто можно делать из кабинета:\n"
        "• смотреть историю выводов\n"
        "• быстро брать свою реферальную ссылку\n"
        "• подавать запрос на вывод при нужном балансе"
    )
    if summary.get('note'):
        text += f"\n📝 Заметка администратора: <i>{summary.get('note')}</i>"
    if summary.get('suspicious'):
        text += "\n⚠️ По аккаунту есть флаг проверки администратора."
    rows = [
        [InlineKeyboardButton(text="🔗 Моя ссылка", callback_data="referral:get_link")],
        [InlineKeyboardButton(text="🧾 История выводов", callback_data="referral:withdraw_history")],
    ]
    if float(summary.get("balance", 0.0) or 0.0) >= float(Config.MIN_WITHDRAW):
        rows.append([InlineKeyboardButton(text="💸 Вывести средства", callback_data="referral:withdraw")])
    rows.append([InlineKeyboardButton(text="⬅️ К реферальной системе", callback_data="user_menu:referral")])
    markup = InlineKeyboardMarkup(inline_keyboard=rows)
    await smart_edit_message(callback.message, text, parse_mode='HTML', reply_markup=markup)
    await callback.answer()
