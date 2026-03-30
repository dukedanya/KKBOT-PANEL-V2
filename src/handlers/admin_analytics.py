from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from db import Database
from handlers.admin_analytics_helpers import (
    _admin_analytics_menu_keyboard,
    _admin_section_keyboard,
    _build_bot_stats_detail,
    _build_funnel_conversion_detail,
    _build_daily_report_detail,
    _build_funnel_offers_detail,
    _build_health_detail,
    _build_incident_report_detail,
    _build_period_report_detail,
    _build_referral_detail,
    _build_top_referrers_detail,
    _build_whitelist_slots_detail,
    _daily_report_keyboard,
    _incident_report_keyboard,
    _period_report_keyboard,
)
from handlers.payment_diagnostics import (
    is_admin,
    logger,
)
from utils.helpers import replace_message
from utils.telegram_ui import smart_edit_message

router = Router()


@router.message(F.text == "🏆 Топ-10 рефералов")
async def admin_top_referrers_message(message: Message, db: Database, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    text = await _build_top_referrers_detail(db, limit=10)
    await replace_message(
        message.from_user.id,
        text,
        reply_markup=_admin_section_keyboard(),
        delete_user_msg=message,
        bot=bot,
    )


@router.callback_query(F.data == "adminmenu:analytics")
async def admin_analytics_menu_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        "📈 <b>Аналитика и отчёты</b>\n\nЕжедневные и периодные отчёты, воронка и состояние системы.",
        reply_markup=_admin_analytics_menu_keyboard(),
        parse_mode="HTML",
    )
    try:
        await callback.answer()
    except Exception as exc:
        logger.warning("Analytics menu callback ack failed: %s", exc)


@router.callback_query(F.data == "admindash:bot")
async def admin_dash_bot(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        await _build_bot_stats_detail(db),
        reply_markup=_admin_section_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.in_({"admindash:referrals", "admindash:withdraws"}))
async def admin_dash_referrals(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        await _build_referral_detail(db),
        reply_markup=_admin_section_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admindash:topref")
async def admin_dash_top_referrers(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        await _build_top_referrers_detail(db, limit=10),
        reply_markup=_admin_section_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admindash:health")
async def admin_dash_health(callback: CallbackQuery, db: Database, panel, payment_gateway):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        await _build_health_detail(db, panel, payment_gateway),
        reply_markup=_admin_section_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admindash:funnel")
async def admin_dash_funnel(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        await _build_funnel_offers_detail(db),
        reply_markup=_admin_section_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admindash:conversions")
async def admin_dash_conversions(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        await _build_funnel_conversion_detail(db),
        reply_markup=_admin_section_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admindash:whitelist")
async def admin_dash_whitelist(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        await _build_whitelist_slots_detail(),
        reply_markup=_admin_section_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admindash:incidents:"))
async def admin_dash_incidents(callback: CallbackQuery, db: Database, panel, payment_gateway):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    try:
        days_ago = max(0, int(callback.data.split(":")[-1]))
    except Exception:
        days_ago = 0
    await smart_edit_message(
        callback.message,
        await _build_incident_report_detail(db, panel, payment_gateway, days_ago=days_ago),
        reply_markup=_incident_report_keyboard(days_ago),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admindash:daily:"))
async def admin_dash_daily(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    try:
        days_ago = max(0, int(callback.data.split(":")[-1]))
    except Exception:
        days_ago = 0
    await smart_edit_message(
        callback.message,
        await _build_daily_report_detail(db, days_ago=days_ago),
        reply_markup=_daily_report_keyboard(days_ago),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admindash:period:"))
async def admin_dash_period(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    try:
        period = str(callback.data.split(":", 2)[-1] or "last_month").strip().lower()
    except Exception:
        period = "last_month"
    if period not in {"7", "30", "last_month", "all"}:
        period = "7"
    text = await _build_period_report_detail(db, period=period)
    markup = _period_report_keyboard(period)
    try:
        await smart_edit_message(
            callback.message,
            text,
            reply_markup=markup,
            parse_mode="HTML",
        )
    except Exception as exc:
        logger.warning("Period report render fallback: admin=%s period=%s error=%s", callback.from_user.id, period, exc)
        await callback.message.answer(text, reply_markup=markup, parse_mode="HTML")
    await callback.answer()


@router.message(Command("periodreport"))
async def admin_period_report_command(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        await _build_period_report_detail(db, period="7"),
        reply_markup=_period_report_keyboard("7"),
        parse_mode="HTML",
    )


@router.message(F.text == "📊 Периоды")
async def admin_period_report_message(message: Message, db: Database, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    await replace_message(
        message.from_user.id,
        await _build_period_report_detail(db, period="7"),
        reply_markup=_period_report_keyboard("7"),
        delete_user_msg=message,
        bot=bot,
    )


@router.message(Command("dailyreport"))
async def admin_daily_report_command(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        await _build_daily_report_detail(db, days_ago=0),
        reply_markup=_daily_report_keyboard(0),
        parse_mode="HTML",
    )


@router.message(F.text == "📈 Ежедневный отчёт")
async def admin_daily_report_message(message: Message, db: Database, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    await replace_message(
        message.from_user.id,
        await _build_daily_report_detail(db, days_ago=0),
        reply_markup=_daily_report_keyboard(0),
        delete_user_msg=message,
        bot=bot,
    )
