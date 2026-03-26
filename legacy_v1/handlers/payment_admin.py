import logging
from datetime import datetime

from aiogram import F, Router
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config import Config
from db import Database
from services.payment_flow import process_successful_payment, reject_pending_payment
from tariffs import get_by_id
from aiogram import Bot
from services.panel import PanelAPI
from utils.helpers import replace_message
from utils.telegram_ui import smart_edit_message

logger = logging.getLogger(__name__)
router = Router()


@router.message(F.text == "💰 Ожидающие платежи")
async def admin_pending_payments(message: Message, db: Database, bot: Bot):
    user_id = message.from_user.id
    if user_id not in Config.ADMIN_USER_IDS:
        return

    pending = await db.get_all_pending_payments()
    if not pending:
        await replace_message(
            user_id,
            "🕒 Нет ожидающих платежей.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")]]),
            delete_user_msg=message,
        )
        return

    await replace_message(
        user_id,
        "🕒 Список ожидающих платежей:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")]]),
        delete_user_msg=message,
    )
    for payment in pending:
        payment_id = payment.get("payment_id", "")
        p_user_id = payment.get("user_id", 0)
        plan_id = payment.get("plan_id", "")
        amount = payment.get("amount", 0)
        timestamp = payment.get("created_at", "")
        plan = get_by_id(plan_id)
        plan_name = plan.get("name", plan_id) if plan else plan_id
        try:
            dt = datetime.fromisoformat(timestamp)
            time_str = dt.strftime("%d.%m.%Y %H:%M")
        except (TypeError, ValueError) as exc:
            logger.debug("Некорректный created_at для платежа %s: %s", payment_id, exc)
            time_str = str(timestamp)

        text = (
            f"📋 <b>Платеж ID:</b> <code>{payment_id}</code>\n"
            f"👤 <b>Пользователь:</b> <code>{p_user_id}</code>\n"
            f"📦 <b>Тариф:</b> {plan_name}\n"
            f"💰 <b>Сумма:</b> {amount} ₽\n"
            f"🕐 <b>Время:</b> {time_str}"
        )
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"pay_await_accept:{payment_id}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"pay_await_reject:{payment_id}"),
                ],
                [InlineKeyboardButton(text="🩺 Диагностика", callback_data=f"paydiag_refresh:{payment_id}")],
            ]
        )
        await bot.send_message(user_id, text, reply_markup=keyboard)


@router.callback_query(F.data.startswith("pay_await_accept:"))
async def pay_await_accept(callback: CallbackQuery, db: Database, bot: Bot, panel: PanelAPI):
    if callback.from_user.id not in Config.ADMIN_USER_IDS:
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return

    payment_id = callback.data.split(":", 1)[1]
    payment = await db.get_pending_payment(payment_id)
    if not payment:
        await callback.answer("❌ Платеж не найден", show_alert=True)
        return
    current_status = payment.get("status")
    if current_status == "accepted":
        await callback.answer("ℹ️ Платеж уже подтверждён", show_alert=True)
        return
    if current_status == "rejected":
        await callback.answer("ℹ️ Платеж уже отклонён", show_alert=True)
        return
    if current_status == "processing":
        await callback.answer("⏳ Платеж уже обрабатывается", show_alert=True)
        return

    if not get_by_id(payment.get("plan_id")):
        await callback.answer("❌ Тариф не найден", show_alert=True)
        return

    result = await process_successful_payment(
        payment=payment,
        db=db,
        panel=panel,
        bot=bot,
        admin_context=f"Ручное подтверждение админом {callback.from_user.id}",
    )
    if result.get("ok"):
        await smart_edit_message(callback.message, 
            callback.message.text + "\n\n✅ <b>ПОДТВЕРЖДЕНО</b>",
            parse_mode="HTML",
        )
        await callback.answer(f"✅ Платеж {payment_id} подтвержден!")
        return

    reason = result.get("reason", "unknown")
    await callback.answer(f"❌ Ошибка активации для платежа {payment_id}: {reason}", show_alert=True)


@router.callback_query(F.data.startswith("pay_await_reject:"))
async def pay_await_reject(callback: CallbackQuery, db: Database, bot: Bot):
    if callback.from_user.id not in Config.ADMIN_USER_IDS:
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return

    payment_id = callback.data.split(":", 1)[1]
    payment = await db.get_pending_payment(payment_id)
    if not payment:
        await callback.answer("❌ Платеж не найден", show_alert=True)
        return
    current_status = payment.get("status")
    if current_status == "accepted":
        await callback.answer("ℹ️ Платеж уже подтверждён", show_alert=True)
        return
    if current_status == "rejected":
        await callback.answer("ℹ️ Платеж уже отклонён", show_alert=True)
        return
    if current_status == "processing":
        await callback.answer("⏳ Платеж уже обрабатывается", show_alert=True)
        return

    result = await reject_pending_payment(
        payment=payment,
        db=db,
        bot=bot,
        admin_context=f"Ручное отклонение админом {callback.from_user.id}",
    )
    if not result.get("ok"):
        await callback.answer("❌ Ошибка отклонения платежа", show_alert=True)
        return

    await smart_edit_message(callback.message, 
        callback.message.text + "\n\n❌ <b>ОТКЛОНЕНО</b>",
        parse_mode="HTML",
    )
    await callback.answer(f"❌ Платеж {payment_id} отклонен!")


@router.callback_query(F.data == "admin:pending_payments")
async def admin_pending_payments_callback(callback: CallbackQuery, db: Database, bot: Bot):
    if callback.from_user.id not in Config.ADMIN_USER_IDS:
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    pending = await db.get_all_pending_payments()
    if not pending:
        await smart_edit_message(callback.message, 
            "🕒 Нет ожидающих платежей.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")]]),
        )
        await callback.answer()
        return
    await smart_edit_message(callback.message, 
        "🕒 <b>Список ожидающих платежей</b>\n\nНиже отправлены карточки с действиями по каждому платежу.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")]]),
        parse_mode="HTML",
    )
    for payment in pending:
        payment_id = payment.get("payment_id", "")
        p_user_id = payment.get("user_id", 0)
        plan_id = payment.get("plan_id", "")
        amount = payment.get("amount", 0)
        timestamp = payment.get("created_at", "")
        plan = get_by_id(plan_id)
        plan_name = plan.get("name", plan_id) if plan else plan_id
        try:
            dt = datetime.fromisoformat(timestamp)
            time_str = dt.strftime("%d.%m.%Y %H:%M")
        except (TypeError, ValueError) as exc:
            logger.debug("Некорректный created_at для платежа %s: %s", payment_id, exc)
            time_str = str(timestamp)
        text = (
            f"📋 <b>Платеж ID:</b> <code>{payment_id}</code>\n"
            f"👤 <b>Пользователь:</b> <code>{p_user_id}</code>\n"
            f"📦 <b>Тариф:</b> {plan_name}\n"
            f"💰 <b>Сумма:</b> {amount} ₽\n"
            f"🕐 <b>Время:</b> {time_str}"
        )
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"pay_await_accept:{payment_id}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"pay_await_reject:{payment_id}"),
                ],
                [InlineKeyboardButton(text="🩺 Диагностика", callback_data=f"paydiag_refresh:{payment_id}")],
            ]
        )
        await bot.send_message(callback.from_user.id, text, reply_markup=keyboard)
    await callback.answer()
