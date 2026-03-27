from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from db import Database
from handlers.admin import is_admin
from handlers.admin_overview_helpers import (
    admin_panel_text,
    bot_stats_text,
    dashboard_back_keyboard,
    withdraw_request_keyboard,
    withdraw_request_text,
)
from keyboards import admin_menu_keyboard, main_menu_keyboard
from utils.helpers import notify_user, replace_message
from utils.telegram_ui import smart_edit_message

router = Router()


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
        admin_panel_text(),
        reply_markup=admin_menu_keyboard(),
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
            admin_panel_text(),
            reply_markup=admin_menu_keyboard(),
            bot=bot,
        )
    await callback.answer()


@router.message(F.text == "📊 Статистика")
async def admin_stats(message: Message, db: Database, bot: Bot):
    user_id = message.from_user.id
    if not is_admin(user_id):
        return
    total_users = await db.get_total_users()
    subscribed = len(await db.get_subscribed_user_ids())
    banned = await db.get_banned_users_count()
    await replace_message(
        user_id,
        bot_stats_text(total_users=total_users, subscribed=subscribed, banned=banned),
        reply_markup=admin_menu_keyboard(),
        delete_user_msg=message,
        bot=bot,
    )


@router.callback_query(F.data == "admin:stats")
async def admin_stats_callback(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    total_users = await db.get_total_users()
    subscribed = len(await db.get_subscribed_user_ids())
    banned = await db.get_banned_users_count()
    await smart_edit_message(
        callback.message,
        bot_stats_text(total_users=total_users, subscribed=subscribed, banned=banned),
        reply_markup=dashboard_back_keyboard(),
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
        await bot.send_message(user_id, withdraw_request_text(request), reply_markup=withdraw_request_keyboard(request["id"]))


@router.callback_query(F.data == "admin:withdraw_requests")
async def admin_withdraw_requests_callback(callback: CallbackQuery, db: Database, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    requests = await db.get_pending_withdraw_requests()
    if not requests:
        await smart_edit_message(
            callback.message,
            "💸 Нет активных запросов на вывод.",
            reply_markup=dashboard_back_keyboard(),
        )
        await callback.answer()
        return
    summary = "💸 <b>Запросы на вывод</b>\n\n" + "\n".join(
        f"• <code>#{r['id']}</code> — user <code>{r['user_id']}</code> — <b>{r['amount']} ₽</b>" for r in requests[:20]
    )
    await smart_edit_message(callback.message, summary, reply_markup=dashboard_back_keyboard(), parse_mode="HTML")
    for request in requests:
        await bot.send_message(callback.from_user.id, withdraw_request_text(request), reply_markup=withdraw_request_keyboard(request["id"]))
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
