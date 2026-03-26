from typing import Dict, List
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from config import Config


def kb(rows: List[List[Dict[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(**button) for button in row] for row in rows
        ]
    )


def user_dashboard_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="user_menu:profile")],
        [
            InlineKeyboardButton(text="📦 Подписки", callback_data="user_menu:subscriptions"),
            InlineKeyboardButton(text="🆘 Поддержка", callback_data="user_menu:support"),
        ],
        [
            InlineKeyboardButton(text="🎁 Подарок", callback_data="buy:gift_prompt"),
            InlineKeyboardButton(text="🏷 Промокод", callback_data="buy:promo_prompt"),
        ],
        [InlineKeyboardButton(text="📖 Инструкция", callback_data="user_menu:instruction")],
        [InlineKeyboardButton(text="📢 Наш канал", url=Config.TG_CHANNEL)],
    ]
    if is_admin:
        rows.append([InlineKeyboardButton(text="🛠️ Админ меню", callback_data="admin_dashboard")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def main_menu_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    return user_dashboard_keyboard(is_admin)


def back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")]]
    )


def profile_inline_keyboard(has_subscription: bool = False, is_frozen: bool = False, is_admin: bool = False) -> InlineKeyboardMarkup:
    button_text = "💰 Продлить подписку" if has_subscription else "💰 Оформить подписку"
    rows = [
        [InlineKeyboardButton(text=button_text, callback_data="open_buy_menu")],
        [InlineKeyboardButton(text="📦 Подписки", callback_data="user_menu:subscriptions")],
        [InlineKeyboardButton(text="🧾 История покупок", callback_data="profile:history")],
        [InlineKeyboardButton(text="🤝 Реферальная система", callback_data="user_menu:referral")],
    ]
    if has_subscription:
        rows.append([InlineKeyboardButton(text="▶️ Разморозить подписку" if is_frozen else "⏸ Заморозить подписку", callback_data="profile:unfreeze" if is_frozen else "profile:freeze")])
    footer = [InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")]
    if is_admin:
        footer.append(InlineKeyboardButton(text="🛠️ Админ", callback_data="admin_dashboard"))
    rows.append(footer)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def subscriptions_inline_keyboard(has_subscription: bool = False, is_admin: bool = False) -> InlineKeyboardMarkup:
    button_text = "💰 Продлить подписку" if has_subscription else "💰 Оформить подписку"
    rows = [
        [InlineKeyboardButton(text=button_text, callback_data="open_buy_menu")],
        [
            InlineKeyboardButton(text="🎁 Подарить", callback_data="buy:gift_prompt"),
            InlineKeyboardButton(text="🏷 Промокод", callback_data="buy:promo_prompt"),
        ],
        [InlineKeyboardButton(text="📱 Подключение", callback_data="onboarding:start")],
    ]
    footer = [InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")]
    rows.append(footer)
    if is_admin:
        rows.append([InlineKeyboardButton(text="🛠️ Админ меню", callback_data="admin_dashboard")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🧭 Сводка", callback_data="admin_dashboard"),
                InlineKeyboardButton(text="👥 Пользователи", callback_data="adminmenu:users"),
            ],
            [
                InlineKeyboardButton(text="💳 Платежи", callback_data="adminmenu:payments"),
                InlineKeyboardButton(text="📈 Аналитика", callback_data="adminmenu:analytics"),
            ],
            [
                InlineKeyboardButton(text="📝 Контент и продажи", callback_data="adminmenu:content"),
                InlineKeyboardButton(text="⚙️ Система и панель", callback_data="adminmenu:service"),
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")],
        ]
    )



def support_keyboard_reply() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")]]
    )


def support_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✉️ Написать в поддержку", callback_data="support:start")],
            [InlineKeyboardButton(text="📜 История обращений", callback_data="support:history")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")],
        ]
    )


def instruction_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📱 Подключение", callback_data="onboarding:start")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")],
        ]
    )


def referral_inline_keyboard(*, balance: float, min_withdraw: float, is_admin: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🔗 Получить ссылку", callback_data="referral:get_link")],
        [InlineKeyboardButton(text="👥 Мои рефералы", callback_data="referral:list")],
        [InlineKeyboardButton(text="📊 История начислений", callback_data="referral:history")],
    ]
    if balance >= min_withdraw:
        rows.append([InlineKeyboardButton(text="💸 Вывести средства", callback_data="referral:withdraw")])
    rows.append([InlineKeyboardButton(text="🤝 Партнёрский кабинет", callback_data="referral:cabinet")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")])
    if is_admin:
        rows.append([InlineKeyboardButton(text="🛠️ Админ меню", callback_data="admin_dashboard")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def simple_back_to_referral_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ К реферальной системе", callback_data="user_menu:referral")]]
    )
