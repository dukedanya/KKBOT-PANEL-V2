from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


IOS_URL = "https://apps.apple.com/ru/app/happ-proxy-utility-plus/id6746188973"
ANDROID_URL = "https://play.google.com/store/apps/details?id=com.happproxy"
WINDOWS_URL = "https://github.com/Happ-proxy/happ-desktop/releases/latest/download/setup-Happ.x64.exe"

PLATFORM_LABELS = {
    "ios": "iPhone / Mac",
    "android": "Android",
    "windows": "Windows",
}


def help_text() -> str:
    return (
        "🆘 <b>Поддержка</b>\n\n"
        "Здесь собраны самые частые вопросы и ответы.\n\n"
        "Если хотите быстро подключить VPN, откройте раздел ниже."
    )


def help_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❓ Как подключиться?", callback_data="onboarding:start")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")],
        ]
    )


def onboarding_text() -> str:
    return (
        "❓ <b>Как подключиться?</b>\n\n"
        "Выберите своё устройство, и я помогу установить клиент и добавить подписку.\n\n"
        "Мы рекомендуем <b>Happ</b> — через него подключение проходит проще и быстрее всего."
    )


def onboarding_keyboard(*, include_main_menu: bool = True) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="🍎 iPhone / Mac", callback_data="onboarding:platform:ios"),
            InlineKeyboardButton(text="🤖 Android", callback_data="onboarding:platform:android"),
        ],
        [InlineKeyboardButton(text="🪟 Windows", callback_data="onboarding:platform:windows")],
    ]
    if include_main_menu:
        rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def install_url_for_platform(platform: str) -> str:
    platform_key = str(platform or "").strip().lower()
    if platform_key == "ios":
        return IOS_URL
    if platform_key == "android":
        return ANDROID_URL
    if platform_key == "windows":
        return WINDOWS_URL
    return ""


def happ_add_url(subscription_url: str) -> str:
    clean_url = (subscription_url or "").strip()
    if not clean_url:
        return ""
    parsed = urlparse(clean_url)
    query_items = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True) if key != "happ"]
    if not any(key == "format" for key, _ in query_items):
        query_items.append(("format", "plain"))
    query = urlencode(query_items)
    query_items = [(key, value) for key, value in parse_qsl(query, keep_blank_values=True) if key != "happ"]
    query_items.append(("happ", "1"))
    return urlunparse(parsed._replace(query=urlencode(query_items)))


def onboarding_platform_text(*, platform: str, subscription_url: str = "") -> str:
    platform_name = PLATFORM_LABELS.get(str(platform or "").strip().lower(), "устройство")
    lines = [
        f"❓ <b>Как подключиться | {platform_name}</b>",
        "",
        "Мы рекомендуем <b>Happ</b>.",
        "Через него удобнее всего добавить подписку, быстро подключиться и при необходимости переподключить профиль.",
    ]
    if subscription_url:
        lines.extend(
            [
                "",
                "Что делать дальше:",
                "1. Установите клиент по кнопке ниже.",
                "2. Попробуйте добавить подписку автоматически.",
                "3. Если это не сработает, откройте <b>HAPP</b>, нажмите <b>+</b> и выберите <b>Добавить из буфера обмена</b>.",
                "",
                "Вот ваша ссылка для подключения:",
                f"<blockquote><code>{subscription_url}</code></blockquote>",
            ]
        )
    else:
        lines.extend(
            [
                "",
                "Ссылка для подключения появится здесь сразу после оформления подписки.",
            ]
        )
    return "\n".join(lines)


def onboarding_platform_keyboard(*, platform: str, subscription_url: str = "") -> InlineKeyboardMarkup:
    install_url = install_url_for_platform(platform)
    connect_url = happ_add_url(subscription_url) if subscription_url else ""
    rows = []
    if install_url:
        rows.append([InlineKeyboardButton(text="⬇️ Установить клиент", url=install_url)])
    if connect_url:
        rows.append([InlineKeyboardButton(text="⚡ Подключиться к подписке", url=connect_url)])
    else:
        rows.append([InlineKeyboardButton(text="💰 Оформить подписку", callback_data="open_buy_menu")])
    rows.append([InlineKeyboardButton(text="⬅️ Выбрать систему", callback_data="onboarding:start")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
