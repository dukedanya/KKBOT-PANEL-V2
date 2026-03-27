import logging
from html import escape
from typing import Any, Dict, List, Optional

from aiogram import F, Router, Bot
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config import Config
from db import Database
from handlers.admin_user_card_helpers import (
    _build_support_restrictions_list_text,
    _build_user_card_text,
    _build_user_more_menu_text,
    _build_user_partner_rates_prompt_text,
    _build_user_payments_menu_text,
    _build_user_referral_last_payment_text,
    _build_user_referral_menu_text,
    _build_user_referrals_history_text,
    _build_user_referrals_list_text,
    _build_user_subscription_menu_text,
    _build_user_timeline_text,
    _format_user_id_with_name,
    _notify_support_restriction_admins,
    _resolve_repairable_payment,
    _resolve_user_current_plan,
    _resolve_user_display_name,
    _support_restrictions_keyboard,
    _user_card_bonus_days_keyboard,
    _user_card_extend_tariff_keyboard,
    _user_card_grant_custom_days_keyboard,
    _user_card_grant_custom_plan_keyboard,
    _user_card_grant_tariff_keyboard,
    _user_card_keyboard,
    _user_card_more_menu_keyboard,
    _user_card_payments_menu_keyboard,
    _user_card_referral_menu_keyboard,
    _user_card_subscription_menu_keyboard,
    _user_card_support_keyboard,
    _user_delete_confirm_keyboard,
)
from handlers.payment_diagnostics_helpers import (
    PROVIDER_LABELS,
    SUPPORT_RESTRICTION_PRESETS,
    _admin_analytics_menu_keyboard,
    _admin_dashboard_keyboard,
    _admin_payments_menu_keyboard,
    _admin_user_id_html,
    _admin_users_menu_keyboard,
    _attention_keyboard,
    _build_admin_dashboard_text,
    _build_payment_diagnostics,
    _build_provider_summary,
    _diagnostics_keyboard,
    _format_access_mode_label,
    _format_bool_badge,
    _format_dt,
    _format_global_admin_actions,
    _format_user_timeline,
    _incident_report_keyboard,
    _pending_operations_keyboard,
    _provider_summary_keyboard,
    _render_attention_text,
    _render_pending_operations_text,
    _trim_text,
)
from keyboards import main_menu_keyboard
from kkbot.services.subscriptions import panel_base_email
from kkbot.services.subscriptions import create_subscription
from kkbot.services.subscriptions import get_subscription_status as get_runtime_subscription_status
from kkbot.services.payment_flow import process_successful_payment
from services.payment_flow import apply_referral_reward
from kkbot.services.subscriptions import revoke_subscription
from services.payment_attention_resolver import auto_resolve_payment_attention
from utils.helpers import replace_message, notify_admins, notify_user
from utils.support import format_support_restriction_reason, format_support_status
from utils.telegram_ui import smart_edit_message

logger = logging.getLogger(__name__)
router = Router()
class PaymentDiagnosticsFSM(StatesGroup):
    waiting_payment_id = State()
    waiting_user_id = State()
    waiting_support_blacklist = State()
    waiting_user_balance_adjustment = State()
    waiting_user_partner_rates = State()
    waiting_user_referrer_id = State()
    waiting_inbound_count = State()
    waiting_inbound_ids = State()
    waiting_user_grant_custom_days = State()
    waiting_admin_gift_user_id = State()
    waiting_admin_gift_custom_days = State()
    waiting_admin_gift_title = State()

def is_admin(user_id: int) -> bool:
    return user_id in Config.ADMIN_USER_IDS
@router.message(F.text == "💳 Диагностика платежей")
async def payment_provider_diagnostics(message: Message, db: Database, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    text = await _build_provider_summary(db)
    await replace_message(
        message.from_user.id,
        text,
reply_markup=_provider_summary_keyboard(),
        delete_user_msg=message,
        bot=bot,
    )


@router.message(F.text == "🧭 Админ дашборд")
async def admin_dashboard_message(message: Message, db: Database, panel, payment_gateway, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    text = await _build_admin_dashboard_text(db, panel, payment_gateway)
    await replace_message(
        message.from_user.id,
        text,
        reply_markup=_admin_dashboard_keyboard(),
        delete_user_msg=message,
        bot=bot,
    )


@router.message(Command("admindash"))
@router.message(Command("admin"))
async def admin_dashboard_command(message: Message, db: Database, panel, payment_gateway):
    if not is_admin(message.from_user.id):
        return
    text = await _build_admin_dashboard_text(db, panel, payment_gateway)
    await message.answer(text, reply_markup=_admin_dashboard_keyboard(), parse_mode="HTML")


@router.callback_query(F.data == "adminmenu:payments")
async def admin_payments_menu_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(callback.message, 
        "💳 <b>Платежи и диагностика</b>\n\nПроверка статусов, ручные операции и разбор проблемных платежей.",
        reply_markup=_admin_payments_menu_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "adminmenu:users")
async def admin_users_menu_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        "👥 <b>Пользователи и поддержка</b>\n\nКарточки пользователей, поддержка, ограничения и запросы на вывод.",
        reply_markup=_admin_users_menu_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.in_({"admin_dashboard", "admin:dashboard"}))
async def admin_dashboard_callback(callback: CallbackQuery, db: Database, panel, payment_gateway):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    text = await _build_admin_dashboard_text(db, panel, payment_gateway)
    await smart_edit_message(callback.message, text, reply_markup=_admin_dashboard_keyboard(), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data == "admin:exit")
async def admin_exit_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(callback.message,
        "🛠️ <b>Админ панель закрыта</b>\n\nИспользуйте /admin или кнопку «🛠️ Админ меню», чтобы открыть её снова.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🛠️ В админ панель", callback_data="admin_dashboard")],
                [InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")],
            ]
        ),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(F.text == "🔎 Платёж по ID")
async def payment_diagnostics_prompt(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(PaymentDiagnosticsFSM.waiting_payment_id)
    await replace_message(
        message.from_user.id,
        "🔎 <b>Диагностика платежа</b>\n\nОтправьте <code>PAYMENT_ID</code> одним сообщением.",
        reply_markup=main_menu_keyboard(True),
        delete_user_msg=message,
        bot=bot,
    )


@router.callback_query(F.data == "paydiag_prompt")
async def payment_diagnostics_prompt_callback(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await state.set_state(PaymentDiagnosticsFSM.waiting_payment_id)
    await smart_edit_message(callback.message, 
        "🔎 <b>Диагностика платежа</b>\n\nОтправьте <code>PAYMENT_ID</code> одним сообщением.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(PaymentDiagnosticsFSM.waiting_payment_id)
async def payment_diagnostics_lookup_by_id(message: Message, state: FSMContext, db: Database, payment_gateway):
    if not is_admin(message.from_user.id):
        return
    payment_id = (message.text or "").strip()
    if not payment_id or payment_id.startswith("/"):
        await message.answer("Отправьте корректный <code>PAYMENT_ID</code>.")
        return
    await state.clear()
    result = await _build_payment_diagnostics(payment_id, db, payment_gateway)
    if not result:
        await message.answer(f"❌ Платёж <code>{payment_id}</code> не найден", parse_mode="HTML")
        return
    await message.answer(result["text"], reply_markup=_diagnostics_keyboard(result["payment"], result["remote_payment"], result["checkout_url"]), parse_mode="HTML")


@router.message(F.text == "🧾 Последние действия")
async def payment_actions_text(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    actions = await db.get_recent_payment_admin_actions(limit=15)
    await message.answer(
        "🧾 <b>Последние admin actions по платежам</b>\n\n" + _format_global_admin_actions(actions),
        parse_mode="HTML",
    )


@router.message(F.text == "⏳ Pending операции")
async def payment_operations_text(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    text, items = await _render_pending_operations_text(db, provider="all", operation="all")
    await message.answer(text, reply_markup=_pending_operations_keyboard(provider="all", operation="all", items=items), parse_mode="HTML")


@router.message(F.text == "🚨 Требует внимания")
async def payment_attention_text(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    text, items = await _render_attention_text(db, provider="all", issue_type="all")
    await message.answer(text, reply_markup=_attention_keyboard(provider="all", issue_type="all", items=items), parse_mode="HTML")


@router.message(F.text == "🛠️ Авто-резолв attention")
async def payment_attention_resolve_text(message: Message, db: Database, payment_gateway, bot: Bot, panel):
    if not is_admin(message.from_user.id):
        return
    summary = await auto_resolve_payment_attention(
        db=db,
        panel=panel,
        payment_gateway=payment_gateway,
        bot=bot,
        provider="all",
        issue_type="all",
        limit=Config.PAYMENT_ATTENTION_RESOLVE_LIMIT,
    )
    await message.answer(
        (
            "🛠️ <b>Attention auto-resolver</b>\n\n"
            f"Resolved: <b>{summary.get('total_resolved', 0)}</b>\n"
            f"Skipped: <b>{summary.get('total_skipped', 0)}</b>\n"
            f"Processing: <b>{summary['processing']['resolved']}</b> / skip {summary['processing']['skipped']}\n"
            f"Operations: <b>{summary['operations']['resolved']}</b> / skip {summary['operations']['skipped']}\n"
            f"Mismatch: <b>{summary['mismatch']['resolved']}</b> / skip {summary['mismatch']['skipped']}"
        ),
        parse_mode="HTML",
    )


@router.message(Command("paydiag"))
async def payment_diagnostics_command(message: Message, db: Database, payment_gateway, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Используйте: <code>/paydiag PAYMENT_ID</code>")
        return
    payment_id = parts[1].strip()
    result = await _build_payment_diagnostics(payment_id, db, payment_gateway)
    if not result:
        await message.answer(f"❌ Платёж <code>{payment_id}</code> не найден")
        return
    await message.answer(result["text"], reply_markup=_diagnostics_keyboard(result["payment"], result["remote_payment"], result["checkout_url"]), parse_mode="HTML")


async def _render_payment_diagnostics(callback: CallbackQuery, payment_id: str, db: Database, payment_gateway) -> bool:
    result = await _build_payment_diagnostics(payment_id, db, payment_gateway)
    if not result:
        return False
    await smart_edit_message(callback.message, result["text"], reply_markup=_diagnostics_keyboard(result["payment"], result["remote_payment"], result["checkout_url"]), parse_mode="HTML")
    return True




@router.message(Command("payactions"))
async def payment_actions_command(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    actions = await db.get_recent_payment_admin_actions(limit=15)
    await message.answer(
        "🧾 <b>Последние admin actions по платежам</b>\n\n" + _format_global_admin_actions(actions),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "payactions_recent")
async def payment_actions_recent(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    actions = await db.get_recent_payment_admin_actions(limit=15)
    await smart_edit_message(callback.message, 
        "🧾 <b>Последние admin actions по платежам</b>\n\n" + _format_global_admin_actions(actions),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔎 Платёж по ID", callback_data="paydiag_prompt")], [InlineKeyboardButton(text="⏳ Pending операции", callback_data="payops:list:all:all")], [InlineKeyboardButton(text="🚨 Требует внимания", callback_data="payattention:list:all:all")], [InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(Command("payops"))
async def payment_operations_command(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    text, items = await _render_pending_operations_text(db, provider="all", operation="all")
    await message.answer(text, reply_markup=_pending_operations_keyboard(provider="all", operation="all", items=items), parse_mode="HTML")


@router.callback_query(F.data.startswith("payops:list:"))
async def payment_operations_list(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    _, _, provider, operation = callback.data.split(":", 3)
    text, items = await _render_pending_operations_text(db, provider=provider, operation=operation)
    await smart_edit_message(callback.message, text, reply_markup=_pending_operations_keyboard(provider=provider, operation=operation, items=items), parse_mode="HTML")
    await callback.answer()


@router.message(Command("payattention"))
async def payment_attention_command(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    text, items = await _render_attention_text(db, provider="all", issue_type="all")
    await message.answer(text, reply_markup=_attention_keyboard(provider="all", issue_type="all", items=items), parse_mode="HTML")


@router.callback_query(F.data.startswith("payattention:list:"))
async def payment_attention_list(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    _, _, provider, issue_type = callback.data.split(":", 3)
    text, items = await _render_attention_text(db, provider=provider, issue_type=issue_type)
    await smart_edit_message(callback.message, text, reply_markup=_attention_keyboard(provider=provider, issue_type=issue_type, items=items), parse_mode="HTML")
    await callback.answer()


@router.message(Command("payresolve"))
async def payment_attention_resolve_command(message: Message, db: Database, payment_gateway, bot: Bot, panel):
    if not is_admin(message.from_user.id):
        return
    summary = await auto_resolve_payment_attention(
        db=db,
        panel=panel,
        payment_gateway=payment_gateway,
        bot=bot,
        provider="all",
        issue_type="all",
        limit=Config.PAYMENT_ATTENTION_RESOLVE_LIMIT,
    )
    await message.answer(
        (
            "🛠️ <b>Attention auto-resolver</b>\n\n"
            f"Resolved: <b>{summary.get('total_resolved', 0)}</b>\n"
            f"Skipped: <b>{summary.get('total_skipped', 0)}</b>\n"
            f"Processing: <b>{summary['processing']['resolved']}</b> / skip {summary['processing']['skipped']}\n"
            f"Operations: <b>{summary['operations']['resolved']}</b> / skip {summary['operations']['skipped']}\n"
            f"Mismatch: <b>{summary['mismatch']['resolved']}</b> / skip {summary['mismatch']['skipped']}"
        ),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("payattention:resolve:"))
async def payment_attention_resolve_callback(callback: CallbackQuery, db: Database, payment_gateway, bot: Bot, panel):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    _, _, provider, issue_type = callback.data.split(":", 3)
    summary = await auto_resolve_payment_attention(
        db=db,
        panel=panel,
        payment_gateway=payment_gateway,
        bot=bot,
        provider=provider,
        issue_type=issue_type,
        limit=Config.PAYMENT_ATTENTION_RESOLVE_LIMIT,
    )
    text, items = await _render_attention_text(db, provider=provider, issue_type=issue_type)
    text += (
        "\n\n🛠️ <b>Auto-resolver</b>\n"
        f"Resolved: <b>{summary.get('total_resolved', 0)}</b>\n"
        f"Skipped: <b>{summary.get('total_skipped', 0)}</b>"
    )
    await smart_edit_message(callback.message, text, reply_markup=_attention_keyboard(provider=provider, issue_type=issue_type, items=items), parse_mode="HTML")
    await callback.answer(f"Resolved {summary.get('total_resolved', 0)}")


@router.callback_query(F.data == "paydiag_provider_summary")
async def payment_provider_summary_callback(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    text = await _build_provider_summary(db)
    await smart_edit_message(callback.message, 
        text,
reply_markup=_provider_summary_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("paydiag_refresh:"))
async def payment_diagnostics_refresh(callback: CallbackQuery, db: Database, payment_gateway):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    payment_id = callback.data.split(":", 1)[1]
    if not await _render_payment_diagnostics(callback, payment_id, db, payment_gateway):
        await callback.answer("Платёж не найден", show_alert=True)
        return
    await callback.answer("Обновлено")


@router.callback_query(F.data.startswith("paydiag_refund:"))
async def payment_diagnostics_refund_yookassa(callback: CallbackQuery, db: Database, payment_gateway, bot: Bot, panel):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    payment_id = callback.data.split(":", 1)[1]
    payment = await db.get_pending_payment(payment_id)
    if not payment or (payment.get("provider") or "") != "yookassa":
        await callback.answer("Это не платёж ЮKassa", show_alert=True)
        return
    provider_payment_id = get_provider_payment_id(payment)
    if not provider_payment_id:
        await callback.answer("Нет внешнего ID платежа", show_alert=True)
        return
    refund = await payment_gateway.create_refund(
        payment_id=provider_payment_id,
        amount=float(payment.get("amount") or 0),
        reason=f"admin refund for {payment_id}",
    )
    if not refund:
        await db.add_payment_admin_action(payment_id, callback.from_user.id, "yookassa_refund", provider="yookassa", result="failed", details="provider refund call failed")
        await callback.answer("Не удалось создать refund в ЮKassa", show_alert=True)
        return

    await db.record_payment_status_transition(
        payment_id,
        from_status=payment.get("status"),
        to_status="refund_requested",
        source="payment_admin/yookassa_refund",
        reason=f"admin={callback.from_user.id}",
        metadata=f"provider_refund_id={refund.get('id', '')};status={refund.get('status', '')};awaiting_confirmation=1",
    )
    await db.add_payment_admin_action(
        payment_id,
        callback.from_user.id,
        "yookassa_refund",
        provider="yookassa",
        result="ok",
        details=f"refund_id={refund.get('id', '')};status={refund.get('status', '')};awaiting_confirmation=1",
    )
    await callback.answer("Refund в ЮKassa создан")
    await notify_admins(
        (
            f"↩️ <b>Создан refund ЮKassa</b>\n"
            f"💳 <code>{payment_id}</code>\n"
            f"🧷 <code>{provider_payment_id}</code>\n"
            f"🆔 Refund: <code>{refund.get('id', '-')}</code>"
        ),
        bot=bot,
    )
    await _render_payment_diagnostics(callback, payment_id, db, payment_gateway)


@router.callback_query(F.data.startswith("paydiag_cancel:"))
async def payment_diagnostics_cancel_yookassa(callback: CallbackQuery, db: Database, payment_gateway, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    payment_id = callback.data.split(":", 1)[1]
    payment = await db.get_pending_payment(payment_id)
    if not payment or (payment.get("provider") or "") != "yookassa":
        await callback.answer("Это не платёж ЮKassa", show_alert=True)
        return
    provider_payment_id = get_provider_payment_id(payment)
    if not provider_payment_id:
        await callback.answer("Нет внешнего ID платежа", show_alert=True)
        return
    cancelled = await payment_gateway.cancel_payment(provider_payment_id)
    if not cancelled:
        await db.add_payment_admin_action(payment_id, callback.from_user.id, "yookassa_cancel", provider="yookassa", result="failed", details="provider cancel call failed")
        await callback.answer("Не удалось отменить платёж в ЮKassa", show_alert=True)
        return
    local_cancelled = await db.update_payment_status(
        payment_id,
        "cancelled",
        allowed_current_statuses=["pending", "processing"],
        source="payment_admin/yookassa_cancel",
        reason=f"admin={callback.from_user.id}",
        metadata=f"provider_payment_id={provider_payment_id};status={cancelled.get('status', '')}",
    )
    await db.record_payment_status_transition(
        payment_id,
        from_status=payment.get("status"),
        to_status="cancel_requested",
        source="payment_admin/yookassa_cancel",
        reason=f"admin={callback.from_user.id}",
        metadata=f"provider_payment_id={provider_payment_id};status={cancelled.get('status', '')};local_cancelled={int(local_cancelled)}",
    )
    await db.add_payment_admin_action(payment_id, callback.from_user.id, "yookassa_cancel", provider="yookassa", result="ok", details=f"local_cancelled={int(local_cancelled)}")
    await callback.answer("Платёж отменён в ЮKassa")
    await notify_admins(
        (
            f"🛑 <b>Отменён платёж ЮKassa</b>\n"
            f"💳 <code>{payment_id}</code>\n"
            f"🧷 <code>{provider_payment_id}</code>"
        ),
        bot=bot,
    )
    await _render_payment_diagnostics(callback, payment_id, db, payment_gateway)


@router.callback_query(F.data.startswith("paydiag_refund_stars:"))
async def payment_diagnostics_refund_stars(callback: CallbackQuery, db: Database, payment_gateway, bot: Bot, panel):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    payment_id = callback.data.split(":", 1)[1]
    payment = await db.get_pending_payment(payment_id)
    if not payment or (payment.get("provider") or "") != "telegram_stars":
        await callback.answer("Это не Stars-платёж", show_alert=True)
        return
    charge_id = payment.get("provider_payment_id")
    if not charge_id:
        await callback.answer("Нет Telegram charge id", show_alert=True)
        return
    ok = await payment_gateway.refund_payment(bot=bot, user_id=int(payment["user_id"]), telegram_payment_charge_id=charge_id)
    if not ok:
        await db.add_payment_admin_action(payment_id, callback.from_user.id, "stars_refund", provider="telegram_stars", result="failed", details="refund_star_payment returned false")
        await callback.answer("Не удалось выполнить refund Stars", show_alert=True)
        return
    local_refunded = False
    subscription_revoked = False
    if payment.get("status") == "accepted":
        local_refunded = await db.update_payment_status(
            payment_id,
            "refunded",
            allowed_current_statuses=["accepted"],
            source="payment_admin/stars_refund",
            reason=f"admin={callback.from_user.id}",
            metadata=f"telegram_payment_charge_id={charge_id}",
        )
        subscription_revoked = await revoke_subscription(
            int(payment["user_id"]), db=db, panel=panel, reason="Возврат Telegram Stars"
        )
    await db.record_payment_status_transition(
        payment_id,
        from_status=payment.get("status"),
        to_status="stars_refunded",
        source="payment_admin/stars_refund",
        reason=f"admin={callback.from_user.id}",
        metadata=f"telegram_payment_charge_id={charge_id};local_refunded={int(local_refunded)};subscription_revoked={int(subscription_revoked)}",
    )
    await db.add_payment_admin_action(payment_id, callback.from_user.id, "stars_refund", provider="telegram_stars", result="ok", details=f"local_refunded={int(local_refunded)};subscription_revoked={int(subscription_revoked)}")
    await notify_user(
        int(payment["user_id"]),
        "↩️ Ваш платёж в Telegram Stars был возвращён администратором.",
        bot=bot,
    )
    await callback.answer("Refund Stars выполнен")
    await _render_payment_diagnostics(callback, payment_id, db, payment_gateway)
