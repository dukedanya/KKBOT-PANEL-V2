from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


IOS_URL = "https://apps.apple.com/ru/app/happ-proxy-utility-plus/id6746188973"
ANDROID_URL = "https://play.google.com/store/apps/details?id=com.happproxy"
WINDOWS_URL = "https://github.com/Happ-proxy/happ-desktop/releases/latest/download/setup-Happ.x64.exe"


def onboarding_text() -> str:
    return (
        "🚀 <b>Быстрый старт</b>\n\n"
        "1. Выберите своё устройство.\n"
        "2. Установите клиент Happ.\n"
        "3. Откройте вашу ссылку подключения.\n"
        "4. Если что-то не работает, нажмите «Автопомощь».\n"
    )


def onboarding_keyboard(*, include_main_menu: bool = True) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="🍎 iPhone / Mac", url=IOS_URL),
            InlineKeyboardButton(text="🤖 Android", url=ANDROID_URL),
        ],
        [InlineKeyboardButton(text="🪟 Windows", url=WINDOWS_URL)],
        [InlineKeyboardButton(text="🛠 Автопомощь", callback_data="help:auto:start")],
    ]
    if include_main_menu:
        rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
