import logging
from html import escape

from aiogram import Bot, F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config import Config
from db import Database
from handlers.admin_operations_helpers import (
    ADMIN_GIFT_BASE_PLAN_ID,
    ADMIN_GIFT_REFERRER_ID,
    _admin_bulk_menu_keyboard,
    _admin_content_menu_keyboard,
    _admin_gift_link_days_keyboard,
    _admin_gift_link_result_keyboard,
    _admin_service_menu_keyboard,
    _build_admin_gift_deep_link,
    _build_admin_gift_link,
    _build_admin_gift_start_command,
    _build_admin_gift_token,
    _build_safe_mode_text,
    _build_self_check_text,
    _build_support_blacklist_text,
    _format_gift_status,
    _panel_inbounds_settings_keyboard,
    _panel_inbounds_settings_text,
    _parse_panel_inbound_ids,
    _support_blacklist_keyboard,
    _write_env_variable,
)
from handlers.payment_diagnostics import PaymentDiagnosticsFSM, is_admin
from tariffs import get_by_id
from utils.helpers import notify_admins
from utils.telegram_ui import smart_edit_message

logger = logging.getLogger(__name__)
router = Router()


@router.callback_query(F.data == "adminmenu:content")
async def admin_content_menu_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        "📝 <b>Контент и продажи</b>\n\nТарифы, промокоды, шаблоны, рассылки и главное сообщение.",
        reply_markup=_admin_content_menu_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "adminmenu:bulk")
async def admin_bulk_menu_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        "📣 <b>Рассылки и массовые действия</b>\n\nСообщения для сегментов и массовое продление активных подписок.",
        reply_markup=_admin_bulk_menu_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:gift_link_prompt")
async def admin_gift_link_prompt(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    base_plan = get_by_id(ADMIN_GIFT_BASE_PLAN_ID)
    if not base_plan:
        await callback.answer("Базовый тариф не найден", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        (
            "🎀 <b>Подарочная ссылка</b>\n\n"
            f"База: <b>{escape(str(base_plan.get('name') or ADMIN_GIFT_BASE_PLAN_ID))}</b>\n"
            "Количество устройств будет как у обычной подписки.\n\n"
            "Выберите срок или укажите своё количество дней.\n"
            "Ссылка будет свободной: вы сами сможете отправить её кому угодно."
        ),
        reply_markup=_admin_gift_link_days_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:inline_links")
async def admin_inline_links(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        (
            "✨ <b>Inline ссылки</b>\n\n"
            "Через inline можно быстро отправлять:\n"
            "• реферальную ссылку\n"
            "• короткое приглашение\n"
            "• инструкцию по установке\n"
            "• подарочную ссылку\n\n"
            "Нажмите кнопку ниже, выберите чат и отправьте нужный вариант."
        ),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✨ Открыть inline", switch_inline_query="")],
                [InlineKeyboardButton(text="🎀 Gift inline", switch_inline_query="gift")],
                [InlineKeyboardButton(text="🔗 Реф inline", switch_inline_query="ref")],
                [InlineKeyboardButton(text="⬇️ Установка inline", switch_inline_query="install")],
                [InlineKeyboardButton(text="⬅️ К контенту", callback_data="adminmenu:content")],
            ]
        ),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:gift_history")
async def admin_gift_history(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    stats = await db.get_gift_links_stats() if hasattr(db, "get_gift_links_stats") else {"total": 0, "claimed": 0, "unclaimed": 0}
    gifts = await db.list_recent_gift_links(limit=20) if hasattr(db, "list_recent_gift_links") else []
    lines = [
        "🎁 <b>История подарков</b>",
        "",
        f"Создано: <b>{int(stats.get('total') or 0)}</b>",
        f"Активировано: <b>{int(stats.get('claimed') or 0)}</b>",
        f"Ожидают: <b>{int(stats.get('unclaimed') or 0)}</b>",
        "",
    ]
    if not gifts:
        lines.append("История подарков пока пуста.")
    else:
        for gift in gifts[:20]:
            token = str(gift.get("token") or "")
            title = str(gift.get("note") or "Без названия").strip()
            days = int(gift.get("custom_duration_days") or 0)
            status_label = _format_gift_status(gift)
            created_at = str(gift.get("created_at") or "")[:16].replace("T", " ")
            lines.append(
                f"• <b>{escape(title)}</b>\n"
                f"  {status_label}\n"
                + (f"  Срок: <b>{days}</b> дней\n" if days > 0 else "")
                + (f"  Токен: <code>{escape(token)}</code>\n" if token else "")
                + (f"  Создан: <i>{escape(created_at)}</i>" if created_at else "")
            )
    await smart_edit_message(
        callback.message,
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ К контенту", callback_data="adminmenu:content")]]
        ),
        parse_mode="HTML",
    )
    await callback.answer()


async def _prompt_admin_gift_title(*, message_obj: Message, days: int) -> None:
    await smart_edit_message(
        message_obj,
        (
            "🎀 <b>Название подарка</b>\n\n"
            f"Срок: <b>{int(days)}</b> дней\n\n"
            "Отправьте название следующим сообщением.\n"
            "Например: <code>Подарок на день рождения</code>"
        ),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ К выбору срока", callback_data="admin:gift_link_prompt")]]
        ),
        parse_mode="HTML",
    )


async def _finalize_admin_gift_link_result(
    *,
    message_obj: Message,
    actor_user_id: int,
    db: Database,
    bot: Bot,
    plan_id: str,
    days: int,
    title: str,
) -> None:
    plan = get_by_id(plan_id)
    if not plan:
        await message_obj.answer("❌ Тариф не найден.", parse_mode="HTML")
        return
    gift_token = _build_admin_gift_token()
    created = await db.create_gift_link(
        token=gift_token,
        buyer_user_id=ADMIN_GIFT_REFERRER_ID,
        recipient_user_id=None,
        plan_id=plan_id,
        note=title,
        custom_duration_days=int(days),
    ) if hasattr(db, "create_gift_link") else False
    if not created:
        await message_obj.answer("❌ Не удалось создать подарочную ссылку.", parse_mode="HTML")
        return
    gift_link = _build_admin_gift_link(token=gift_token, bot=bot)
    deep_link = _build_admin_gift_deep_link(token=gift_token, bot=bot)
    start_command = _build_admin_gift_start_command(gift_token)
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(
            actor_user_id,
            actor_user_id,
            "create_gift_link",
            f"plan_id={plan_id} days={int(days)} token={gift_token} title={title}",
        )
    await smart_edit_message(
        message_obj,
        (
            "✅ <b>Подарочная ссылка создана</b>\n\n"
            f"Название: <b>{escape(title)}</b>\n"
            f"Тариф: <b>{escape(str(plan.get('name') or plan_id))}</b>\n"
            f"Срок: <b>{int(days)}</b> дней\n\n"
            "Отправляйте подарок через кнопку ниже или через inline, чтобы он открывался сразу в Telegram.\n\n"
            "Если у получателя снова откроется web-страница Telegram, пусть просто отправит боту команду:\n"
            f"<blockquote>{escape(start_command)}</blockquote>"
        ),
        reply_markup=_admin_gift_link_result_keyboard(gift_link=gift_link, deep_link=deep_link),
        parse_mode="HTML",
    )


@router.callback_query(F.data.regexp(r"^admin:gift_link_confirm:\d+$"))
async def admin_gift_link_confirm(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    days = int(callback.data.split(":")[-1])
    await state.set_state(PaymentDiagnosticsFSM.waiting_admin_gift_title)
    await state.update_data(gift_plan_id=ADMIN_GIFT_BASE_PLAN_ID, gift_days=days)
    await _prompt_admin_gift_title(message_obj=callback.message, days=days)
    await callback.answer()


@router.callback_query(F.data == "admin:gift_link_input")
async def admin_gift_link_input_prompt(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    plan_id = ADMIN_GIFT_BASE_PLAN_ID
    plan = get_by_id(plan_id)
    if not plan:
        await callback.answer("Тариф не найден", show_alert=True)
        return
    await state.set_state(PaymentDiagnosticsFSM.waiting_admin_gift_custom_days)
    await state.update_data(gift_plan_id=plan_id)
    await smart_edit_message(
        callback.message,
        (
            "🎀 <b>Подарочная ссылка</b>\n\n"
            f"Тариф: <b>{escape(str(plan.get('name') or plan_id))}</b>\n\n"
            "Отправьте количество дней следующим сообщением.\n"
            "Пример: <code>45</code>"
        ),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ К выбору срока", callback_data="admin:gift_link_prompt")]]
        ),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "adminmenu:service")
async def admin_service_menu_callback(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    safe_mode_enabled = str(await db.get_setting("system:safe_mode", "0") or "0") == "1"
    await smart_edit_message(
        callback.message,
        "⚙️ <b>Система и панель</b>\n\nПанель, safe mode, Stars, реферальные настройки и служебные действия.",
        reply_markup=_admin_service_menu_keyboard(safe_mode_enabled=safe_mode_enabled),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:self_check")
async def admin_self_check(callback: CallbackQuery, db: Database, panel, payment_gateway):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    safe_mode_enabled = str(await db.get_setting("system:safe_mode", "0") or "0") == "1"
    await smart_edit_message(
        callback.message,
        await _build_self_check_text(db, panel, payment_gateway),
        reply_markup=_admin_service_menu_keyboard(safe_mode_enabled=safe_mode_enabled),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:panel_inbounds")
async def admin_panel_inbounds(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        _panel_inbounds_settings_text(),
        reply_markup=_panel_inbounds_settings_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:panel_inbounds:count")
async def admin_panel_inbounds_count_prompt(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    configured = _parse_panel_inbound_ids(Config.PANEL_TARGET_INBOUND_IDS)
    await state.set_state(PaymentDiagnosticsFSM.waiting_inbound_count)
    await smart_edit_message(
        callback.message,
        (
            "🔢 <b>Количество активных инбаундов</b>\n\n"
            f"Сейчас в списке ID: <code>{', '.join(str(item) for item in configured) or 'не заданы'}</code>\n"
            f"Отправьте число от <b>0</b> до <b>{len(configured)}</b>.\n"
            "<code>0</code> = использовать все ID из списка.\n"
            "Любое другое число = использовать первые N ID из списка."
        ),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:panel_inbounds")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:panel_inbounds:ids")
async def admin_panel_inbounds_ids_prompt(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await state.set_state(PaymentDiagnosticsFSM.waiting_inbound_ids)
    await smart_edit_message(
        callback.message,
        (
            "🆔 <b>ID инбаундов для регистрации</b>\n\n"
            "Отправьте список ID через запятую.\n"
            "Можно указывать сколько угодно ID.\n"
            "Пример: <code>2,3,4,7,8,11</code>"
        ),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:panel_inbounds")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:safe_mode:toggle")
async def admin_safe_mode_toggle(callback: CallbackQuery, db: Database, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    enabled = str(await db.get_setting("system:safe_mode", "0") or "0") == "1"
    next_enabled = not enabled
    await db.set_setting("system:safe_mode", "1" if next_enabled else "0")
    await db.set_setting("system:safe_mode_reason", "manual_admin_toggle")
    await db.set_setting("system:safe_mode_manual_override", "1" if next_enabled else "0")
    await notify_admins(
        (
            f"⚠️ <b>Safe mode {'включён' if next_enabled else 'выключен'} вручную</b>\n\n"
            f"Админ: <code>{callback.from_user.id}</code>\n"
            "Причина: <code>manual_admin_toggle</code>"
        ),
        bot=bot,
    )
    text = await _build_safe_mode_text(db)
    await smart_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔁 Переключить ещё раз", callback_data="admin:safe_mode:toggle")],
                [InlineKeyboardButton(text="♻️ Вернуть авто-режим", callback_data="admin:safe_mode:auto")],
                [InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")],
            ]
        ),
    )
    await callback.answer("Режим обновлён")


@router.callback_query(F.data == "admin:safe_mode:auto")
async def admin_safe_mode_auto(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await db.set_setting("system:safe_mode_manual_override", "")
    text = await _build_safe_mode_text(db)
    await smart_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔁 Переключить вручную", callback_data="admin:safe_mode:toggle")],
                [InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")],
            ]
        ),
    )
    await callback.answer("Авто-режим восстановлен")


@router.callback_query(F.data == "admin:support_blacklist")
async def admin_support_blacklist(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    text = await _build_support_blacklist_text(db)
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=_support_blacklist_keyboard())
    await callback.answer()


@router.callback_query(F.data == "admin:support_blacklist:edit")
async def admin_support_blacklist_edit(callback: CallbackQuery, state: FSMContext, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await state.set_state(PaymentDiagnosticsFSM.waiting_support_blacklist)
    raw = await db.get_setting("support:blacklist_phrases", "") if hasattr(db, "get_setting") else ""
    preview = raw.strip() or "список пуст"
    await smart_edit_message(
        callback.message,
        "🛡 <b>Редактор blacklist поддержки</b>\n\n"
        "Отправьте список фраз, по одной на строке.\n"
        "Пустое сообщение не подходит. Чтобы очистить список, отправьте <code>clear</code>.\n\n"
        f"Текущий список:\n<code>{escape(preview[:1500])}</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:support_blacklist")]]),
    )
    await callback.answer()


@router.message(PaymentDiagnosticsFSM.waiting_support_blacklist)
async def admin_support_blacklist_save(message: Message, state: FSMContext, db: Database):
    if not is_admin(message.from_user.id):
        return
    raw = (message.text or "").strip()
    value = "" if raw.lower() == "clear" else raw
    await db.set_setting("support:blacklist_phrases", value)
    await state.clear()
    text = await _build_support_blacklist_text(db)
    await message.answer(text, parse_mode="HTML", reply_markup=_support_blacklist_keyboard())


@router.message(PaymentDiagnosticsFSM.waiting_admin_gift_custom_days)
async def admin_gift_link_custom_days_save(message: Message, state: FSMContext, db: Database, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    plan_id = str(data.get("gift_plan_id") or "").strip()
    if not plan_id:
        await state.clear()
        await message.answer("❌ Не удалось определить тариф.")
        return
    try:
        days = int((message.text or "").strip())
    except ValueError:
        await message.answer("❌ Укажите срок числом, например <code>45</code>.", parse_mode="HTML")
        return
    if days <= 0 or days > 3650:
        await message.answer("❌ Введите срок от 1 до 3650 дней.", parse_mode="HTML")
        return
    await state.set_state(PaymentDiagnosticsFSM.waiting_admin_gift_title)
    await state.update_data(gift_plan_id=plan_id, gift_days=days)
    sent = await message.answer("Открываю ввод названия...", parse_mode="HTML")
    await _prompt_admin_gift_title(message_obj=sent, days=days)


@router.message(PaymentDiagnosticsFSM.waiting_admin_gift_title)
async def admin_gift_link_title_save(message: Message, state: FSMContext, db: Database, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    plan_id = str(data.get("gift_plan_id") or ADMIN_GIFT_BASE_PLAN_ID).strip()
    days = int(data.get("gift_days") or 0)
    title = (message.text or "").strip()
    if days <= 0 or not plan_id:
        await state.clear()
        await message.answer("❌ Не удалось определить срок подарка.")
        return
    if not title:
        await message.answer("❌ Укажите название подарка.", parse_mode="HTML")
        return
    if len(title) > 120:
        await message.answer("❌ Название слишком длинное. До 120 символов.", parse_mode="HTML")
        return
    await state.clear()
    sent = await message.answer("Создаю подарочную ссылку...", parse_mode="HTML")
    await _finalize_admin_gift_link_result(
        message_obj=sent,
        actor_user_id=message.from_user.id,
        db=db,
        bot=bot,
        plan_id=plan_id,
        days=days,
        title=title,
    )


@router.message(PaymentDiagnosticsFSM.waiting_inbound_count)
async def admin_panel_inbounds_count_save(message: Message, state: FSMContext, bot: Bot, db: Database):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    configured = _parse_panel_inbound_ids(Config.PANEL_TARGET_INBOUND_IDS)
    if not configured:
        await message.answer("❌ Сначала задайте список ID инбаундов.")
        return
    try:
        value = int((message.text or "").strip())
    except ValueError:
        await message.answer("❌ Введите целое число.")
        return
    if value < 0 or value > len(configured):
        await message.answer(f"❌ Введите число от 0 до {len(configured)}.")
        return
    Config.set_panel_target_inbound_count(value)
    await db.set_setting("system:panel_target_inbound_count", str(value))
    _write_env_variable("PANEL_TARGET_INBOUND_COUNT", str(value))
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    await bot.send_message(
        message.from_user.id,
        _panel_inbounds_settings_text(),
        reply_markup=_panel_inbounds_settings_keyboard(),
        parse_mode="HTML",
    )


@router.message(PaymentDiagnosticsFSM.waiting_inbound_ids)
async def admin_panel_inbounds_ids_save(message: Message, state: FSMContext, bot: Bot, db: Database):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    ids = _parse_panel_inbound_ids(message.text or "")
    if not ids:
        await message.answer("❌ Отправьте хотя бы один корректный числовой ID.")
        return
    Config.set_panel_target_inbound_ids(",".join(str(item) for item in ids))
    await db.set_setting("system:panel_target_inbound_ids", ",".join(str(item) for item in ids))
    if Config.PANEL_TARGET_INBOUND_COUNT > len(ids):
        Config.set_panel_target_inbound_count(len(ids))
        await db.set_setting("system:panel_target_inbound_count", str(len(ids)))
        _write_env_variable("PANEL_TARGET_INBOUND_COUNT", str(len(ids)))
    _write_env_variable("PANEL_TARGET_INBOUND_IDS", ",".join(str(item) for item in ids))
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    await bot.send_message(
        message.from_user.id,
        _panel_inbounds_settings_text(),
        reply_markup=_panel_inbounds_settings_keyboard(),
        parse_mode="HTML",
    )
