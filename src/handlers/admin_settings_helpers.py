import os

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import Config
from db import Database
from utils.telegram_ui import smart_edit_message


def _trim_text(value: str, limit: int = 80) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _stars_settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить коэффициент", callback_data="admin:stars_multiplier")],
        [InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")],
    ])


def _write_env_variable(key: str, value: str) -> None:
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


def _ref_settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🎁 Бонус днями: {Config.REF_BONUS_DAYS}", callback_data="admin:refedit:REF_BONUS_DAYS")],
        [InlineKeyboardButton(text=f"1️⃣ Уровень 1: {Config.REF_PERCENT_LEVEL1}%", callback_data="admin:refedit:REF_PERCENT_LEVEL1")],
        [InlineKeyboardButton(text=f"2️⃣ Уровень 2: {Config.REF_PERCENT_LEVEL2}%", callback_data="admin:refedit:REF_PERCENT_LEVEL2")],
        [InlineKeyboardButton(text=f"3️⃣ Уровень 3: {Config.REF_PERCENT_LEVEL3}%", callback_data="admin:refedit:REF_PERCENT_LEVEL3")],
        [InlineKeyboardButton(text=f"💸 Мин. вывод: {Config.MIN_WITHDRAW} ₽", callback_data="admin:refedit:MIN_WITHDRAW")],
        [InlineKeyboardButton(text="🎯 Индивидуальные условия", callback_data="admin:partner_rates_prompt")],
        [InlineKeyboardButton(text="💰 Корректировка баланса", callback_data="admin:partner_balance_prompt")],
        [InlineKeyboardButton(text="📋 Реферальный аудит", callback_data="admin:ref_audit")],
        [InlineKeyboardButton(text="🚨 Suspicious referrals", callback_data="admin:ref_suspicious")],
        [InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")],
    ])


def _set_config_value(key: str, value):
    if key == "REF_BONUS_DAYS":
        Config.REF_BONUS_DAYS = int(value)
    elif key == "REF_PERCENT_LEVEL1":
        Config.REF_PERCENT_LEVEL1 = float(value)
    elif key == "REF_PERCENT_LEVEL2":
        Config.REF_PERCENT_LEVEL2 = float(value)
    elif key == "REF_PERCENT_LEVEL3":
        Config.REF_PERCENT_LEVEL3 = float(value)
    elif key == "MIN_WITHDRAW":
        Config.MIN_WITHDRAW = float(value)


async def _render_ref_settings(message_obj):
    text = (
        "🤝 <b>Настройки реферальной системы</b>\n\n"
        f"🎁 Бонус днями: <b>{Config.REF_BONUS_DAYS}</b>\n"
        f"1️⃣ Уровень 1: <b>{Config.REF_PERCENT_LEVEL1}%</b>\n"
        f"2️⃣ Уровень 2: <b>{Config.REF_PERCENT_LEVEL2}%</b>\n"
        f"3️⃣ Уровень 3: <b>{Config.REF_PERCENT_LEVEL3}%</b>\n"
        f"💸 Минимальный вывод: <b>{Config.MIN_WITHDRAW} ₽</b>"
    )
    await smart_edit_message(message_obj, text, reply_markup=_ref_settings_keyboard(), parse_mode="HTML")


async def _build_ref_audit_text(db: Database) -> str:
    users = await db.get_all_users() if hasattr(db, "get_all_users") else []
    user_map = {int(row.get("user_id") or 0): row for row in users}
    referred = []
    for row in users:
        ref_by = int(row.get("ref_by") or 0)
        if ref_by > 0:
            referred.append(row)
    referred.sort(key=lambda item: str(item.get("join_date") or ""), reverse=True)
    gifts = await db.list_recent_claimed_gift_links(limit=10) if hasattr(db, "list_recent_claimed_gift_links") else []
    suspicious = await db.get_suspicious_referrals(limit=20) if hasattr(db, "get_suspicious_referrals") else []
    paid_referred = [row for row in referred if int(row.get("ref_rewarded") or 0) == 1]
    gift_referrals = []
    for row in gifts:
        buyer = int(row.get("buyer_user_id") or 0)
        claimed_by = int(row.get("claimed_by_user_id") or 0)
        claimed_user = user_map.get(claimed_by) or {}
        linked = int(claimed_user.get("ref_by") or 0) == buyer and buyer > 0 and claimed_by > 0
        gift_referrals.append((row, linked))
    lines = [
        "📋 <b>Реферальный аудит</b>",
        "",
        f"Привязанных пользователей: <b>{len(referred)}</b>",
        f"Оплативших рефералов: <b>{len(paid_referred)}</b>",
        f"Подозрительных кейсов: <b>{len(suspicious)}</b>",
        f"Подарков, которые стали рефералкой: <b>{sum(1 for _, linked in gift_referrals if linked)}</b>",
        "",
        "<b>Последние обычные / стартовые привязки</b>",
    ]
    if not referred:
        lines.append("• пока нет данных")
    else:
        for row in referred[:12]:
            paid_label = "оплатил" if int(row.get("ref_rewarded") or 0) == 1 else "ещё не оплатил"
            lines.append(
                f"• user <code>{row.get('user_id')}</code> ← ref <code>{int(row.get('ref_by') or 0)}</code> "
                f"• <code>{row.get('join_date') or '-'}</code> • {paid_label}"
            )
    lines.extend(["", "<b>Последние активации подарков</b>"])
    if not gift_referrals:
        lines.append("• пока нет данных")
    else:
        for row, linked in gift_referrals[:10]:
            buyer = int(row.get("buyer_user_id") or 0)
            claimed_by = int(row.get("claimed_by_user_id") or 0)
            note = str(row.get("note") or "").strip()
            note_suffix = f" — {_trim_text(note, 40)}" if note else ""
            linked_suffix = " • стал рефералом" if linked else " • без ref-привязки"
            lines.append(
                f"• buyer <code>{buyer}</code> → user <code>{claimed_by}</code> "
                f"• <code>{row.get('claimed_at') or row.get('created_at') or '-'}</code>{linked_suffix}{note_suffix}"
            )
    lines.extend(["", "<b>Последние подозрительные кейсы</b>"])
    if not suspicious:
        lines.append("• нет активных кейсов")
    else:
        for row in suspicious[:8]:
            lines.append(
                f"• user <code>{row.get('user_id')}</code> ← ref <code>{row.get('ref_by') or 0}</code>"
                f" — {_trim_text(str(row.get('partner_note') or 'без заметки'), 60)}"
            )
    return "\n".join(lines)


def _build_ref_audit_keyboard(referred: list[dict], gifts: list[dict]) -> InlineKeyboardMarkup:
    seen: set[int] = set()
    rows: list[list[InlineKeyboardButton]] = []

    def add_user_button(user_id: int) -> None:
        if user_id <= 0 or user_id in seen:
            return
        seen.add(user_id)
        rows.append([
            InlineKeyboardButton(
                text=f"👤 user {user_id}",
                callback_data=f"admin:usercard:{user_id}",
            )
        ])

    for row in referred[:8]:
        add_user_button(int(row.get("user_id") or 0))
        add_user_button(int(row.get("ref_by") or 0))

    for row in gifts[:8]:
        add_user_button(int(row.get("buyer_user_id") or 0))
        add_user_button(int(row.get("claimed_by_user_id") or 0))

    rows.append([InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
