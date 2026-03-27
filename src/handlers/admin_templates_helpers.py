from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from db import Database
from utils.templates import (
    TEMPLATES,
    get_template_content,
    template_allow_photo,
    template_title,
    template_variables,
    template_variables_map,
)
from utils.telegram_ui import smart_edit_message


def _templates_menu_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=template_title(key), callback_data=f"admin:template:{key}")] for key in TEMPLATES.keys()]
    rows.append([InlineKeyboardButton(text="ℹ️ Переменные шаблонов", callback_data="admin:template_vars")])
    rows.append([InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _template_preview_keyboard(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить", callback_data=f"admin:template_edit:{key}")],
        [InlineKeyboardButton(text="🗑 Убрать фото", callback_data=f"admin:template_clear_photo:{key}")],
        [InlineKeyboardButton(text="↩️ Сбросить в стандарт", callback_data=f"admin:template_reset:{key}")],
        [InlineKeyboardButton(text="⬅️ К шаблонам", callback_data="admin:templates")],
    ])


def _template_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="admin:template_confirm")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin:template_cancel")],
    ])


async def _render_template_item(message_obj, db: Database, key: str):
    text, photo = await get_template_content(db, key)
    preview = text if len(text) <= 1000 else text[:1000] + "..."
    vars_for_key = template_variables(key)
    vars_text = ", ".join(f"<code>{{{name}}}</code>" for name in vars_for_key) if vars_for_key else "нет"
    msg = (
        f"📝 <b>{template_title(key)}</b>\n\n"
        f"Ключ: <code>{key}</code>\n"
        f"Фото: <b>{'да' if photo else 'нет'}</b>\n"
        f"Можно прикреплять фото: <b>{'да' if template_allow_photo(key) else 'нет'}</b>\n\n"
        f"<b>Переменные:</b> {vars_text}\n\n"
        f"<b>Текущий текст:</b>\n{preview}"
    )
    await smart_edit_message(message_obj, msg, reply_markup=_template_preview_keyboard(key), parse_mode="HTML")


def build_template_variables_text() -> str:
    rows = template_variables_map()
    lines = [
        "ℹ️ <b>Переменные в шаблонах</b>",
        "",
        "Используйте только переменные из списка. Если для шаблона указано «нет», подстановок там нет.",
    ]
    for key in TEMPLATES.keys():
        variables = rows.get(key) or []
        title = template_title(key)
        if not variables:
            lines.append(f"\n• <b>{title}</b> (<code>{key}</code>): нет")
            continue
        vars_text = ", ".join(f"<code>{{{name}}}</code>" for name in variables)
        lines.append(f"\n• <b>{title}</b> (<code>{key}</code>): {vars_text}")
    return "\n".join(lines)
