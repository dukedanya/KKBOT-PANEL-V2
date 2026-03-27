import json
import os

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import Config


def _optional_int_or_none(raw: str):
    value = (raw or "").strip().lower()
    if value in {"", "-", "none", "null", "auto"}:
        return None
    return int(value)


TARIFF_FIELDS = {
    "name": ("Название", str),
    "price_rub": ("Цена (руб)", int),
    "old_price_rub": ("Старая цена (руб)", _optional_int_or_none),
    "duration_days": ("Дней", int),
    "ip_limit": ("Устройств", int),
    "sort": ("Порядок", int),
    "description": ("Описание", str),
    "price_stars": ("Цена в Stars", _optional_int_or_none),
}


def tariffs_list_keyboard(plans):
    rows = []
    for plan in plans:
        status = "✅" if plan.get("active", True) else "❌"
        rows.append([
            InlineKeyboardButton(text=f"{status} {plan.get('name', plan['id'])}", callback_data=f"tedit:{plan['id']}"),
            InlineKeyboardButton(text="🔀", callback_data=f"ttoggle:{plan['id']}"),
        ])
    rows.append([InlineKeyboardButton(text="➕ Добавить тариф", callback_data="tadd")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_admin")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def tariff_fields_keyboard(plan_id: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"✏️ {label}", callback_data=f"tfield:{plan_id}:{field}")]
        for field, (label, _) in TARIFF_FIELDS.items()
    ]
    rows.append([InlineKeyboardButton(text="🗑 Удалить тариф", callback_data=f"tdelete:{plan_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="tlist")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _format_tariff_field_value(plan: dict, field: str):
    value = plan.get(field, "—")
    if field == "price_stars" and value in (None, "", 0):
        return f"авто × {Config.TELEGRAM_STARS_PRICE_MULTIPLIER}"
    return value


def tariff_editor_text(plan: dict, plan_id: str, *, success_prefix: str = "") -> str:
    lines = []
    if success_prefix:
        lines.extend([success_prefix, ""])
    lines.extend([f"✏️ <b>Тариф: {plan.get('name', plan_id)}</b>", ""])
    for key, (label, _) in TARIFF_FIELDS.items():
        lines.append(f"{label}: <b>{_format_tariff_field_value(plan, key)}</b>")
    return "\n".join(lines)


def tariffs_list_text() -> str:
    return "📋 <b>Редактор тарифов</b>\n\nВыберите тариф для редактирования:"


def save_tariffs(plans) -> None:
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data",
        "tarifs.json",
    )
    with open(path, "w", encoding="utf-8") as file:
        json.dump({"plans": plans}, file, ensure_ascii=False, indent=2)
    from tariffs.loader import load_tariffs
    load_tariffs()
