import secrets
from html import escape
from typing import Dict, List

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import Config
from db import Database
from kkbot.tools.self_check import build_self_check_report, report_to_html_text

ADMIN_GIFT_REFERRER_ID = 794419497
ADMIN_GIFT_BASE_PLAN_ID = "basic"


def _write_env_variable(key: str, value: str) -> None:
    import os

    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    lines = []
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as file:
            lines = file.read().splitlines()
    replaced = False
    for idx, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[idx] = f"{key}={value}"
            replaced = True
            break
    if not replaced:
        lines.append(f"{key}={value}")
    with open(env_path, "w", encoding="utf-8") as file:
        file.write("\n".join(lines) + "\n")


def _parse_panel_inbound_ids(raw: str) -> List[int]:
    result: List[int] = []
    for item in str(raw or "").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            value = int(item)
        except ValueError:
            continue
        if value not in result:
            result.append(value)
    return result


def _effective_panel_inbound_ids() -> List[int]:
    ids = _parse_panel_inbound_ids(Config.PANEL_TARGET_INBOUND_IDS)
    count = max(0, int(getattr(Config, "PANEL_TARGET_INBOUND_COUNT", 0) or 0))
    if count > 0:
        return ids[:count]
    return ids


def _panel_inbounds_settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔢 Изменить количество", callback_data="admin:panel_inbounds:count")],
        [InlineKeyboardButton(text="🆔 Изменить ID инбаундов", callback_data="admin:panel_inbounds:ids")],
        [InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")],
    ])


def _panel_inbounds_settings_text() -> str:
    configured = _parse_panel_inbound_ids(Config.PANEL_TARGET_INBOUND_IDS)
    effective = _effective_panel_inbound_ids()
    count = max(0, int(getattr(Config, "PANEL_TARGET_INBOUND_COUNT", 0) or 0))
    return (
        "🛰 <b>Регистрация в инбаундах</b>\n\n"
        f"Список ID: <code>{', '.join(str(item) for item in configured) or 'не задан'}</code>\n"
        f"Активное количество: <b>{count if count > 0 else len(configured)}</b>"
        + (" <i>(режим: все из списка)</i>" if count == 0 else "")
        + "\n"
        f"Сейчас используются: <code>{', '.join(str(item) for item in effective) or 'не заданы'}</code>\n\n"
        "Бот может работать с любым количеством инбаундов.\n"
        "Если указать количество <code>0</code>, будут использованы все ID из списка."
    )


async def _build_safe_mode_text(db: Database) -> str:
    enabled = str(await db.get_setting("system:safe_mode", "0") or "0") == "1"
    reason = str(await db.get_setting("system:safe_mode_reason", "") or "")
    manual = str(await db.get_setting("system:safe_mode_manual_override", "") or "")
    manual_label = {
        "1": "принудительно включён",
        "0": "принудительно выключен",
    }.get(manual, "авто-режим")
    return (
        "🧯 <b>Safe mode</b>\n\n"
        f"Сейчас: <b>{'включён' if enabled else 'выключен'}</b>\n"
        f"Режим работы: <b>{manual_label}</b>\n"
        f"Причина: <code>{reason or '-'}</code>\n\n"
        "При включённом safe mode новые покупки временно блокируются."
    )


def _trim_text(value: str, limit: int = 80) -> str:
    text = " ".join((value or "").split())
    if len(text) <= limit:
        return text or "—"
    return text[: limit - 1].rstrip() + "…"


async def _build_support_blacklist_text(db: Database) -> str:
    raw = await db.get_setting("support:blacklist_phrases", "") if hasattr(db, "get_setting") else ""
    phrases = [line.strip() for line in raw.splitlines() if line.strip()]
    recent_hits = await db.get_recent_support_blacklist_hits(limit=5) if hasattr(db, "get_recent_support_blacklist_hits") else []
    hits_text = "\n".join(
        f"• <code>{item.get('created_at') or '-'}</code> — user <code>{item.get('user_id')}</code> — {_trim_text(str(item.get('details') or '-'), 60)}"
        for item in recent_hits
    ) or "—"
    phrases_text = "\n".join(f"• <code>{escape(item)}</code>" for item in phrases[:20]) or "• список пуст"
    return (
        "🛡 <b>Blacklist поддержки</b>\n\n"
        "Фразы проверяются только в сообщениях, которые пользователь отправляет в поддержку.\n\n"
        f"<b>Текущий список</b>\n{phrases_text}\n\n"
        f"<b>Последние срабатывания</b>\n{hits_text}"
    )


def _admin_content_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 Тарифы", callback_data="admin:tariffs"),
            InlineKeyboardButton(text="🏷 Промокоды", callback_data="admin:promo_menu"),
        ],
        [
            InlineKeyboardButton(text="📝 Шаблоны", callback_data="admin:templates"),
            InlineKeyboardButton(text="📣 Рассылки", callback_data="adminmenu:bulk"),
        ],
        [
            InlineKeyboardButton(text="📨 Главное сообщение", callback_data="admin:main_message"),
            InlineKeyboardButton(text="🎀 Подарочная ссылка", callback_data="admin:gift_link_prompt"),
        ],
        [
            InlineKeyboardButton(text="✨ Inline ссылки", callback_data="admin:inline_links"),
            InlineKeyboardButton(text="🎁 История подарков", callback_data="admin:gift_history"),
        ],
        [InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")],
    ])


def _admin_bulk_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📣 Рассылка всем", callback_data="bulk:prompt:all"),
            InlineKeyboardButton(text="📣 Рассылка активным", callback_data="bulk:prompt:active"),
        ],
        [
            InlineKeyboardButton(text="💤 Без подписки", callback_data="bulk:prompt:broadcast:inactive"),
            InlineKeyboardButton(text="💰 С балансом", callback_data="bulk:prompt:broadcast:with_balance"),
        ],
        [
            InlineKeyboardButton(text="🤝 Из рефералки", callback_data="bulk:prompt:broadcast:referred"),
            InlineKeyboardButton(text="⌛ Истекла подписка", callback_data="bulk:prompt:broadcast:expired"),
        ],
        [InlineKeyboardButton(text="🎁 Пробный период без покупки", callback_data="bulk:prompt:broadcast:trial_only")],
        [InlineKeyboardButton(text="⏱ Продлить всем активным", callback_data="bulk:prompt:extend")],
        [InlineKeyboardButton(text="🧵 Очередь фоновых задач", callback_data="bulk:jobs")],
        [InlineKeyboardButton(text="⬅️ К контенту", callback_data="adminmenu:content")],
    ])


def _admin_service_menu_keyboard(*, safe_mode_enabled: bool = False) -> InlineKeyboardMarkup:
    safe_mode_label = "🧯 Safe mode: выключить" if safe_mode_enabled else "🧯 Safe mode: включить"
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⭐ Stars", callback_data="admin:stars_settings"),
            InlineKeyboardButton(text="🤝 Реф. настройки", callback_data="admin:ref_settings"),
        ],
        [InlineKeyboardButton(text=safe_mode_label, callback_data="admin:safe_mode:toggle")],
        [
            InlineKeyboardButton(text="🧩 Inbound панели", callback_data="admin:panel_inbounds"),
            InlineKeyboardButton(text="🗓 Инциденты за день", callback_data="admindash:incidents:0"),
        ],
        [InlineKeyboardButton(text="🩺 Self-check", callback_data="admin:self_check")],
        [InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")],
    ])


async def _build_self_check_text(db: Database, panel, payment_gateway) -> str:
    report = await build_self_check_report(db, panel, payment_gateway)
    report["target_inbounds"] = ", ".join(str(x) for x in _effective_panel_inbound_ids()) or "-"
    return report_to_html_text(report)


def _admin_gift_link_days_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="7 дней", callback_data="admin:gift_link_confirm:7"),
            InlineKeyboardButton(text="14 дней", callback_data="admin:gift_link_confirm:14"),
        ],
        [
            InlineKeyboardButton(text="30 дней", callback_data="admin:gift_link_confirm:30"),
            InlineKeyboardButton(text="60 дней", callback_data="admin:gift_link_confirm:60"),
        ],
        [
            InlineKeyboardButton(text="90 дней", callback_data="admin:gift_link_confirm:90"),
            InlineKeyboardButton(text="180 дней", callback_data="admin:gift_link_confirm:180"),
        ],
        [InlineKeyboardButton(text="✍️ Ввести вручную", callback_data="admin:gift_link_input")],
        [InlineKeyboardButton(text="⬅️ К контенту", callback_data="adminmenu:content")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_admin_gift_token() -> str:
    return secrets.token_urlsafe(8).replace("-", "").replace("_", "")[:12]


def _build_admin_gift_link(*, token: str, bot: Bot | None) -> str:
    username = (getattr(Config, "BOT_PUBLIC_USERNAME", "") or "").strip()
    if not username and bot:
        username = getattr(bot, "username", "") or ""
    if username:
        return f"https://t.me/{username}?start=gift_{token}"
    return f"https://t.me/?start=gift_{token}"


def _build_admin_gift_deep_link(*, token: str, bot: Bot | None) -> str:
    username = (getattr(Config, "BOT_PUBLIC_USERNAME", "") or "").strip()
    if not username and bot:
        username = getattr(bot, "username", "") or ""
    if username:
        return f"tg://resolve?domain={username}&start=gift_{token}"
    return ""


def _build_admin_gift_start_command(token: str) -> str:
    return f"/start gift_{token}"


def _admin_gift_link_result_keyboard(*, gift_link: str, deep_link: str) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton(text="🎁 Открыть подарок", url=gift_link)])
    if deep_link:
        rows.append([InlineKeyboardButton(text="📲 Открыть через приложение", url=deep_link)])
    return InlineKeyboardMarkup(
        inline_keyboard=rows + [
            [InlineKeyboardButton(text="📨 Поделиться inline", switch_inline_query=gift_link)],
            [InlineKeyboardButton(text="🎀 Создать ещё одну", callback_data="admin:gift_link_prompt")],
            [InlineKeyboardButton(text="⬅️ К контенту", callback_data="adminmenu:content")],
        ]
    )


def _format_gift_status(gift: Dict[str, object]) -> str:
    claimed_by = int(gift.get("claimed_by_user_id") or 0)
    if claimed_by > 0:
        return f"✅ Активирован ({claimed_by})"
    return "⏳ Ожидает активации"


def _support_blacklist_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Изменить список", callback_data="admin:support_blacklist:edit")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin:support_blacklist")],
            [InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")],
        ]
    )
