import logging
import time

from aiogram import Bot, F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from handlers.admin import TariffEditFSM, is_admin
from handlers.admin_tariffs_helpers import (
    TARIFF_FIELDS,
    save_tariffs,
    tariff_editor_text,
    tariff_fields_keyboard,
    tariffs_list_keyboard,
    tariffs_list_text,
)
from tariffs import get_by_id
from utils.helpers import replace_message
from utils.telegram_ui import smart_edit_message

logger = logging.getLogger(__name__)
router = Router()


@router.message(F.text == "📋 Тарифы")
async def admin_tariffs_list(message: Message, bot: Bot):
    user_id = message.from_user.id
    if not is_admin(user_id):
        return
    from tariffs.loader import TARIFFS_ALL
    await replace_message(
        user_id,
        tariffs_list_text(),
        reply_markup=tariffs_list_keyboard(list(TARIFFS_ALL)),
        delete_user_msg=message,
        bot=bot,
    )


@router.callback_query(F.data == "admin:tariffs")
async def admin_tariffs_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    from tariffs.loader import TARIFFS_ALL
    await smart_edit_message(
        callback.message,
        tariffs_list_text(),
        reply_markup=tariffs_list_keyboard(list(TARIFFS_ALL)),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "tlist")
async def tariffs_list_cb(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return
    from tariffs.loader import TARIFFS_ALL
    await smart_edit_message(
        callback.message,
        tariffs_list_text(),
        reply_markup=tariffs_list_keyboard(list(TARIFFS_ALL)),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tedit:"))
async def tariff_edit_menu(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return
    plan_id = callback.data.split(":", 1)[1]
    plan = get_by_id(plan_id)
    if not plan:
        await callback.answer("Тариф не найден", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        tariff_editor_text(plan, plan_id),
        reply_markup=tariff_fields_keyboard(plan_id),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ttoggle:"))
async def tariff_toggle(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return
    from tariffs.loader import TARIFFS_ALL
    plan_id = callback.data.split(":", 1)[1]
    plans = list(TARIFFS_ALL)
    for plan in plans:
        if plan.get("id") == plan_id:
            plan["active"] = not plan.get("active", True)
            await callback.answer("Тариф включён" if plan["active"] else "Тариф выключен")
            break
    save_tariffs(plans)
    from tariffs.loader import TARIFFS_ALL as reloaded_plans
    await smart_edit_message(
        callback.message,
        tariffs_list_text(),
        reply_markup=tariffs_list_keyboard(list(reloaded_plans)),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("tfield:"))
async def tariff_field_select(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return
    _, plan_id, field = callback.data.split(":", 2)
    label = TARIFF_FIELDS.get(field, (field,))[0]
    await state.set_state(TariffEditFSM.value)
    await state.update_data(plan_id=plan_id, field=field, msg_id=callback.message.message_id)
    await smart_edit_message(
        callback.message,
        f"✏️ Введите новое значение для поля <b>{label}</b>:\n(отправьте /cancel для отмены)",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data=f"tedit:{plan_id}")]]
        ),
    )
    await callback.answer()


@router.message(TariffEditFSM.value)
async def tariff_field_value(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    if message.text == "/cancel":
        await state.clear()
        await message.delete()
        return

    data = await state.get_data()
    plan_id = data["plan_id"]
    field = data["field"]
    _, cast = TARIFF_FIELDS[field]
    try:
        value = cast(message.text.strip())
    except (TypeError, ValueError):
        await message.answer("❌ Неверный формат. Попробуйте ещё раз.")
        return

    from tariffs.loader import TARIFFS_ALL
    plans = list(TARIFFS_ALL)
    for plan in plans:
        if plan.get("id") == plan_id:
            plan[field] = value
            break
    save_tariffs(plans)
    await state.clear()
    try:
        await message.delete()
    except Exception as exc:
        logger.debug("Не удалось удалить сообщение редактирования тарифа: %s", exc)

    plan = get_by_id(plan_id)
    await bot.send_message(
        user_id,
        tariff_editor_text(plan, plan_id, success_prefix="✅ Сохранено!"),
        reply_markup=tariff_fields_keyboard(plan_id),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("tdelete:"))
async def tariff_delete(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return
    from tariffs.loader import TARIFFS_ALL
    plan_id = callback.data.split(":", 1)[1]
    save_tariffs([plan for plan in TARIFFS_ALL if plan.get("id") != plan_id])
    from tariffs.loader import TARIFFS_ALL as reloaded_plans
    await smart_edit_message(
        callback.message,
        "🗑 Тариф удалён.\n\n📋 <b>Редактор тарифов</b>:",
        reply_markup=tariffs_list_keyboard(list(reloaded_plans)),
        parse_mode="HTML",
    )
    await callback.answer("Удалено")


@router.callback_query(F.data == "tadd")
async def tariff_add(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return
    new_id = f"plan_{int(time.time())}"
    from tariffs.loader import TARIFFS_ALL
    plans = list(TARIFFS_ALL)
    plans.append(
        {
            "id": new_id,
            "name": "Новый тариф",
            "active": False,
            "price_rub": 0,
            "old_price_rub": None,
            "duration_days": 30,
            "ip_limit": 1,
            "sort": 999,
            "description": "",
            "price_stars": None,
        }
    )
    save_tariffs(plans)
    await smart_edit_message(
        callback.message,
        "➕ Тариф создан (выключен). Отредактируйте его:",
        reply_markup=tariff_fields_keyboard(new_id),
        parse_mode="HTML",
    )
    await callback.answer()
