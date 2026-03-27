from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from db import Database
from handlers.admin import MainMessageFSM, TemplateEditFSM, is_admin
from handlers.admin_templates_helpers import (
    _render_template_item,
    _template_confirm_keyboard,
    _templates_menu_keyboard,
    build_template_variables_text,
)
from utils.templates import get_template_content, template_allow_photo, template_default_text, template_title
from utils.telegram_ui import smart_edit_message

router = Router()


@router.callback_query(F.data == "admin:main_message")
async def admin_main_message_prompt(callback: CallbackQuery, state: FSMContext, db: Database):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(MainMessageFSM.content)
    text = await db.get_setting("main_message_text", "") if hasattr(db, "get_setting") else ""
    has_photo = bool(await db.get_setting("main_message_photo", "")) if hasattr(db, "get_setting") else False
    msg = (
        "🖼 <b>Главное сообщение</b>\n\n"
        "Отправьте новый текст или фото с подписью следующим сообщением.\n"
        f"Изображение сейчас: <b>{'есть' if has_photo else 'нет'}</b>\n\n"
        f"Текущий текст:\n{text[:500] if text else 'стандартный'}"
    )
    await smart_edit_message(callback.message, msg, parse_mode="HTML")
    await callback.answer()


@router.message(MainMessageFSM.content)
async def admin_main_message_save_real(message: Message, state: FSMContext, db: Database):
    if not is_admin(message.from_user.id):
        return
    text = message.caption or message.text or ""
    photo = message.photo[-1].file_id if message.photo else ""
    if hasattr(db, "set_setting"):
        await db.set_setting("main_message_text", text)
        if photo:
            await db.set_setting("main_message_photo", photo)
    await state.clear()
    await message.answer("✅ Главное сообщение обновлено.")


@router.callback_query(F.data == "admin:templates")
async def admin_templates_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await smart_edit_message(
        callback.message,
        "📝 <b>Шаблоны сообщений</b>\n\nВыберите сообщение для редактирования.",
        reply_markup=_templates_menu_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:template_vars")
async def admin_template_variables_menu(callback: CallbackQuery):
    await smart_edit_message(
        callback.message,
        build_template_variables_text(),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ К шаблонам", callback_data="admin:templates")],
            ]
        ),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin:template:"))
async def admin_template_item(callback: CallbackQuery, db: Database, state: FSMContext):
    await state.clear()
    key = callback.data.split(":", 2)[2]
    await _render_template_item(callback.message, db, key)
    await callback.answer()


@router.callback_query(F.data.startswith("admin:template_edit:"))
async def admin_template_edit_prompt(callback: CallbackQuery, state: FSMContext):
    key = callback.data.split(":", 2)[2]
    await state.set_state(TemplateEditFSM.content)
    await state.update_data(template_key=key)
    await smart_edit_message(
        callback.message,
        (
            f"✏️ <b>{template_title(key)}</b>\n\n"
            "Отправьте новый текст или фото с подписью.\n"
            "HTML-форматирование поддерживается.\n"
            "Напишите <code>/clearphoto</code>, чтобы убрать картинку и оставить только текст.\n\n"
            "После этого бот попросит подтверждение изменения."
        ),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:template_cancel")
async def admin_template_cancel(callback: CallbackQuery, state: FSMContext, db: Database):
    data = await state.get_data()
    key = data.get("template_key")
    await state.clear()
    if key:
        await _render_template_item(callback.message, db, key)
    else:
        await smart_edit_message(
            callback.message,
            "📝 <b>Шаблоны сообщений</b>\n\nВыберите сообщение для редактирования.",
            reply_markup=_templates_menu_keyboard(),
            parse_mode="HTML",
        )
    await callback.answer("Редактирование отменено")


@router.message(TemplateEditFSM.content)
async def admin_template_edit_receive(message: Message, state: FSMContext, db: Database):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    key = data.get("template_key")
    if not key:
        await state.clear()
        return
    current_text, current_photo = await get_template_content(db, key)
    pending_text = (message.text or message.caption or "").strip()
    pending_photo = current_photo
    if (message.text or "").strip() == "/clearphoto":
        pending_text = current_text
        pending_photo = ""
    elif message.photo:
        if not template_allow_photo(key):
            await message.answer("Для этого шаблона картинка не поддерживается.")
            return
        pending_photo = message.photo[-1].file_id
        if not pending_text:
            pending_text = current_text
    elif pending_text:
        pass
    else:
        await message.answer("Отправьте текст или фото с подписью.")
        return

    await state.set_state(TemplateEditFSM.confirm)
    await state.update_data(template_key=key, pending_text=pending_text, pending_photo=pending_photo)
    preview = f"✅ <b>Подтвердите изменение шаблона</b>\n\n<b>{template_title(key)}</b>\n\n{pending_text}"
    if pending_photo and template_allow_photo(key):
        await message.answer_photo(pending_photo, caption=preview, parse_mode="HTML", reply_markup=_template_confirm_keyboard())
    else:
        await message.answer(preview, parse_mode="HTML", reply_markup=_template_confirm_keyboard())


@router.callback_query(F.data == "admin:template_confirm")
async def admin_template_confirm(callback: CallbackQuery, state: FSMContext, db: Database):
    data = await state.get_data()
    key = data.get("template_key")
    if not key:
        await callback.answer("Нет данных шаблона", show_alert=True)
        return
    await db.set_setting(f"template:{key}:text", data.get("pending_text", template_default_text(key)))
    await db.set_setting(f"template:{key}:photo", data.get("pending_photo", ""))
    await state.clear()
    await _render_template_item(callback.message, db, key)
    await callback.answer("Шаблон обновлён")


@router.callback_query(F.data.startswith("admin:template_clear_photo:"))
async def admin_template_clear_photo(callback: CallbackQuery, db: Database):
    key = callback.data.split(":", 3)[3]
    await db.set_setting(f"template:{key}:photo", "")
    await _render_template_item(callback.message, db, key)
    await callback.answer("Фото убрано")


@router.callback_query(F.data.startswith("admin:template_reset:"))
async def admin_template_reset(callback: CallbackQuery, db: Database):
    key = callback.data.split(":", 3)[3]
    await db.set_setting(f"template:{key}:text", template_default_text(key))
    await db.set_setting(f"template:{key}:photo", "")
    await _render_template_item(callback.message, db, key)
    await callback.answer("Сброшено")
