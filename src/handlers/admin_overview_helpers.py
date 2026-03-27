from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def admin_panel_text() -> str:
    return (
        "🛠️ <b>Админ панель</b>\n\n"
        "Разделы собраны по задачам:\n"
        "• 🧭 Сводка\n"
        "• 👥 Пользователи\n"
        "• 💳 Платежи\n"
        "• 📈 Аналитика\n"
        "• 📝 Контент и продажи\n"
        "• ⚙️ Система и панель"
    )


def dashboard_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")]])


def bot_stats_text(*, total_users: int, subscribed: int, banned: int) -> str:
    return (
        "📊 <b>Статистика бота</b>\n\n"
        f"👥 Всего пользователей: {total_users}\n"
        f"✅ Активных VPN: {subscribed}\n"
        f"⛔ Заблокировано: {banned}"
    )


def withdraw_request_text(request: dict) -> str:
    return (
        f"📋 <b>Запрос #{request['id']}</b>\n"
        f"👤 Пользователь: <code>{request['user_id']}</code>\n"
        f"💰 Сумма: {request['amount']} ₽\n"
        f"🕐 Создан: {request['created_at']}"
    )


def withdraw_request_keyboard(request_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"withdraw_accept:{request_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"withdraw_reject:{request_id}"),
        ]]
    )
