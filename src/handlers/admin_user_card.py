from html import escape

from aiogram import Bot, F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from db import Database
from handlers.admin_user_card_actions_helpers import (
    build_delete_prompt_text,
    build_support_menu_text,
    build_support_restrictions_screen,
    disable_support_restriction,
    edit_user_card_message,
    enable_support_restriction,
    perform_delete_user,
    revoke_user_subscription,
    send_user_card_message,
    user_delete_confirm_markup,
    user_support_menu_markup,
)
from handlers.admin_user_card_helpers import (
    _build_user_card_text,
    _build_user_more_menu_text,
    _build_user_subscription_menu_text,
    _build_user_timeline_text,
    _format_user_id_with_name,
    _resolve_user_display_name,
    _user_card_keyboard,
    _user_card_more_menu_keyboard,
    _user_card_subscription_menu_keyboard,
)
from handlers.admin_user_card_payment_helpers import (
    add_user_bonus_days,
    change_user_tariff,
    extend_user_tariff,
    grant_custom_tariff_days,
    grant_user_tariff,
    rebuild_user_subscription,
    repair_user_payment,
    reset_user_trial,
    save_user_balance_adjustment,
    save_user_partner_rates,
    save_user_referrer,
)
from handlers.admin_user_card_view_helpers import (
    build_balance_prompt_text,
    build_partner_rates_prompt_screen,
    build_referral_last_payment_screen,
    build_referral_menu_screen,
    build_rebind_referrer_prompt_text,
    build_referrals_history_screen,
    build_referrals_list_screen,
    build_user_payments_menu_screen,
    build_user_payments_text,
    user_back_to_card_markup,
    user_balance_prompt_markup,
    user_rebind_referrer_markup,
)
from handlers.admin_user_card_subscription_view_helpers import (
    build_bonus_days_prompt,
    build_change_tariff_prompt,
    build_extend_tariff_prompt,
    build_grant_custom_input_prompt,
    build_grant_custom_plan_prompt,
    build_grant_custom_prompt,
    build_grant_tariff_prompt,
)
from handlers.payment_diagnostics import PaymentDiagnosticsFSM, is_admin
from utils.support import format_support_status
from utils.telegram_ui import smart_edit_message

router = Router()


async def _send_user_card(message: Message, db: Database, user_id: int, *, panel=None, state: FSMContext | None = None) -> None:
    if state is not None:
        await state.clear()
    await send_user_card_message(message, db, user_id, panel=panel, bot=message.bot)


@router.callback_query(F.data == "admin:user_lookup")
async def admin_user_lookup_prompt(callback: CallbackQuery, state: FSMContext, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await state.set_state(PaymentDiagnosticsFSM.waiting_user_id)
    recent = await db.get_recent_user_ids(limit=50) if hasattr(db, "get_recent_user_ids") else []
    recent_labels = [await _format_user_id_with_name(callback.bot, db, int(item)) for item in recent]
    recent_text = "\n".join(f"• {label}" for label in recent_labels) if recent_labels else "нет данных"
    await smart_edit_message(
        callback.message,
        "👤 <b>Поиск пользователя</b>\n\n"
        "Отправьте <code>user_id</code> или <code>@username</code> одним сообщением.\n\n"
        "<b>Последние пользователи</b>\n"
        f"{recent_text}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К пользователям", callback_data="adminmenu:users")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(PaymentDiagnosticsFSM.waiting_user_id)
async def admin_user_lookup_receive(message: Message, state: FSMContext, db: Database, panel):
    if not is_admin(message.from_user.id):
        return
    raw = (message.text or "").strip()
    if raw.startswith("@"):
        user = await db.get_user_by_username(raw) if hasattr(db, "get_user_by_username") else None
        if not user:
            await state.clear()
            await message.answer(f"Пользователь <code>{escape(raw)}</code> не найден.", parse_mode="HTML")
            return
        await _send_user_card(message, db, int(user.get("user_id") or 0), panel=panel, state=state)
        return
    try:
        user_id = int(raw)
    except ValueError:
        await message.answer("❌ Отправьте user_id или @username.")
        return
    user = await db.get_user(user_id)
    if not user:
        await state.clear()
        await message.answer(f"Пользователь <code>{user_id}</code> не найден.", parse_mode="HTML")
        return
    await _send_user_card(message, db, user_id, panel=panel, state=state)


@router.message(F.text.regexp(r"^\d{5,}$"))
async def admin_user_lookup_quick(message: Message, state: FSMContext, db: Database, panel):
    if not is_admin(message.from_user.id):
        return
    current_state = await state.get_state()
    if current_state:
        return
    raw = (message.text or "").strip()
    try:
        user_id = int(raw)
    except ValueError:
        return
    user = await db.get_user(user_id)
    if not user:
        await message.answer(f"Пользователь <code>{user_id}</code> не найден.", parse_mode="HTML")
        return
    await _send_user_card(message, db, user_id, panel=panel)


@router.message(F.text.regexp(r"^@[A-Za-z0-9_]{3,}$"))
async def admin_user_lookup_quick_username(message: Message, state: FSMContext, db: Database, panel):
    if not is_admin(message.from_user.id):
        return
    current_state = await state.get_state()
    if current_state:
        return
    raw = (message.text or "").strip()
    user = await db.get_user_by_username(raw) if hasattr(db, "get_user_by_username") else None
    if not user:
        await message.answer(f"Пользователь <code>{escape(raw)}</code> не найден.", parse_mode="HTML")
        return
    await _send_user_card(message, db, int(user.get("user_id") or 0), panel=panel)


@router.callback_query(F.data.regexp(r"^admin:usercard:\d+$"))
async def admin_user_card_callback(callback: CallbackQuery, db: Database, bot: Bot, panel):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[2])
    user = await db.get_user(user_id)
    display_name = await _resolve_user_display_name(bot, user_id, user)
    text = await _build_user_card_text(db, user_id, panel=panel, display_name_override=display_name)
    if not user:
        await smart_edit_message(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К пользователям", callback_data="adminmenu:users")]]),
        )
        await callback.answer("Пользователь уже удалён")
        return
    restriction = await db.get_support_restriction(user_id) if hasattr(db, "get_support_restriction") else {}
    await smart_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=_user_card_keyboard(
            user_id,
            banned=bool((user or {}).get("banned")),
            support_blocked=bool(restriction.get("active")),
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:timeline:"))
async def admin_user_card_timeline(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    await smart_edit_message(
        callback.message,
        await _build_user_timeline_text(db, user_id),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")]]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:tickets:"))
async def admin_user_card_tickets(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    tickets = await db.list_user_support_tickets(user_id, limit=10)
    lines = ["📜 <b>Тикеты пользователя</b>", "", f"Пользователь: <code>{user_id}</code>"]
    if not tickets:
        lines.append("\nТикетов пока нет.")
    else:
        for ticket in tickets:
            lines.append(
                f"\n• <code>#{ticket.get('id')}</code> — <b>{format_support_status(str(ticket.get('status') or ''), lowercase=True)}</b> — <code>{ticket.get('updated_at') or '-'}</code>"
            )
    await smart_edit_message(
        callback.message,
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")]]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:payments:"))
async def admin_user_card_payments(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    await smart_edit_message(callback.message, await build_user_payments_text(db, user_id), parse_mode="HTML", reply_markup=user_back_to_card_markup(user_id))
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:subscription_menu:"))
async def admin_user_card_subscription_menu(callback: CallbackQuery, db: Database, panel):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    await smart_edit_message(
        callback.message,
        await _build_user_subscription_menu_text(db, user_id, panel=panel),
        parse_mode="HTML",
        reply_markup=_user_card_subscription_menu_keyboard(user_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:payments_menu:"))
async def admin_user_card_payments_menu(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    text, markup = await build_user_payments_menu_screen(db, user_id)
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:more_menu:"))
async def admin_user_card_more_menu(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    await smart_edit_message(
        callback.message,
        await _build_user_more_menu_text(db, user_id),
        parse_mode="HTML",
        reply_markup=_user_card_more_menu_keyboard(user_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:balance_prompt:"))
async def admin_user_card_balance_prompt(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    await state.set_state(PaymentDiagnosticsFSM.waiting_user_balance_adjustment)
    await state.update_data(target_user_id=user_id)
    await smart_edit_message(callback.message, await build_balance_prompt_text(user_id), parse_mode="HTML", reply_markup=user_balance_prompt_markup(user_id))
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:rebuild_subscription:"))
async def admin_user_card_rebuild_subscription(callback: CallbackQuery, db: Database, panel, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    ok, prefix = await rebuild_user_subscription(
        actor_user_id=callback.from_user.id,
        target_user_id=user_id,
        db=db,
        panel=panel,
        bot=bot,
    )
    if not ok:
        await callback.answer(prefix, show_alert=True)
        return
    await edit_user_card_message(callback, db, user_id, panel=panel, prefix=prefix)
    await callback.answer("Подписка пересобрана")


@router.callback_query(F.data.startswith("admin:usercard:repair_payment:"))
async def admin_user_card_repair_payment(callback: CallbackQuery, db: Database, panel, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    ok, prefix = await repair_user_payment(
        actor_user_id=callback.from_user.id,
        target_user_id=user_id,
        db=db,
        panel=panel,
        bot=bot,
    )
    if not ok:
        await callback.answer(prefix, show_alert=True)
        return
    await edit_user_card_message(callback, db, user_id, panel=panel, prefix=prefix)
    await callback.answer("Repair выполнен")


@router.callback_query(F.data.startswith("admin:usercard:change_tariff:"))
async def admin_user_card_change_tariff_prompt(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    ok, text, markup = await build_change_tariff_prompt(db, user_id)
    if not ok:
        await callback.answer(text, show_alert=True)
        return
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:grant_tariff:"))
async def admin_user_card_grant_tariff_prompt(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    ok, text, markup = await build_grant_tariff_prompt(db, user_id)
    if not ok:
        await callback.answer(text, show_alert=True)
        return
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:grant_custom:"))
async def admin_user_card_grant_custom_prompt(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    ok, text, markup = await build_grant_custom_prompt(db, user_id)
    if not ok:
        await callback.answer(text, show_alert=True)
        return
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.regexp(r"^admin:usercard:grant_custom_plan:\d+:[A-Za-z0-9_.-]+$"))
async def admin_user_card_grant_custom_plan_prompt(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    parts = callback.data.split(":")
    user_id = int(parts[-2])
    plan_id = parts[-1]
    ok, text, markup = await build_grant_custom_plan_prompt(db, user_id, plan_id)
    if not ok:
        await callback.answer(text, show_alert=True)
        return
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:extend_tariff:"))
async def admin_user_card_extend_tariff_prompt(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    ok, text, markup = await build_extend_tariff_prompt(db, user_id)
    if not ok:
        await callback.answer(text, show_alert=True)
        return
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.regexp(r"^admin:usercard:extend_tariff_confirm:\d+:[A-Za-z0-9_.-]+$"))
async def admin_user_card_extend_tariff_confirm(callback: CallbackQuery, db: Database, panel, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    parts = callback.data.split(":")
    user_id = int(parts[-2])
    plan_id = parts[-1]
    ok, prefix = await extend_user_tariff(
        actor_user_id=callback.from_user.id,
        target_user_id=user_id,
        plan_id=plan_id,
        db=db,
        panel=panel,
        bot=bot,
    )
    if not ok:
        await callback.answer(prefix, show_alert=True)
        return
    await edit_user_card_message(callback, db, user_id, panel=panel, prefix=prefix)
    await callback.answer("Тариф продлён")


@router.callback_query(F.data.regexp(r"^admin:usercard:change_tariff_confirm:\d+:[A-Za-z0-9_.-]+$"))
async def admin_user_card_change_tariff_confirm(callback: CallbackQuery, db: Database, panel, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    parts = callback.data.split(":")
    user_id = int(parts[-2])
    plan_id = parts[-1]
    ok, prefix = await change_user_tariff(
        actor_user_id=callback.from_user.id,
        target_user_id=user_id,
        plan_id=plan_id,
        db=db,
        panel=panel,
        bot=bot,
    )
    if not ok:
        await callback.answer(prefix, show_alert=True)
        return
    await edit_user_card_message(callback, db, user_id, panel=panel, prefix=prefix)
    await callback.answer("Тариф изменён")


@router.callback_query(F.data.startswith("admin:usercard:bonus_days:"))
async def admin_user_card_bonus_days_prompt(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    ok, text, markup = await build_bonus_days_prompt(db, user_id)
    if not ok:
        await callback.answer(text, show_alert=True)
        return
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.regexp(r"^admin:usercard:bonus_days_confirm:\d+:\d+$"))
async def admin_user_card_bonus_days_confirm(callback: CallbackQuery, db: Database, panel):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    parts = callback.data.split(":")
    user_id = int(parts[-2])
    bonus_days = int(parts[-1])
    ok, prefix = await add_user_bonus_days(
        actor_user_id=callback.from_user.id,
        target_user_id=user_id,
        bonus_days=bonus_days,
        db=db,
        panel=panel,
    )
    if not ok:
        await callback.answer(prefix, show_alert=True)
        return
    await edit_user_card_message(callback, db, user_id, panel=panel, prefix=prefix)
    await callback.answer("Бонусные дни добавлены")


@router.callback_query(F.data.regexp(r"^admin:usercard:grant_custom_confirm:\d+:[A-Za-z0-9_.-]+:\d+$"))
async def admin_user_card_grant_custom_confirm(callback: CallbackQuery, db: Database, panel, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    parts = callback.data.split(":")
    user_id = int(parts[-3])
    plan_id = parts[-2]
    days = int(parts[-1])
    user = await db.get_user(user_id)
    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return
    ok, prefix = await grant_custom_tariff_days(
        actor_user_id=callback.from_user.id,
        target_user_id=user_id,
        plan_id=plan_id,
        days=days,
        db=db,
        panel=panel,
        bot=bot,
    )
    if not ok:
        await callback.answer(prefix, show_alert=True)
        return
    await edit_user_card_message(callback, db, user_id, panel=panel, prefix=prefix)
    await callback.answer("Тариф выдан")


@router.callback_query(F.data.regexp(r"^admin:usercard:grant_custom_input:\d+:[A-Za-z0-9_.-]+$"))
async def admin_user_card_grant_custom_input_prompt(callback: CallbackQuery, state: FSMContext, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    parts = callback.data.split(":")
    user_id = int(parts[-2])
    plan_id = parts[-1]
    ok, text, markup = await build_grant_custom_input_prompt(db, user_id, plan_id)
    if not ok:
        await callback.answer(text, show_alert=True)
        return
    await state.set_state(PaymentDiagnosticsFSM.waiting_user_grant_custom_days)
    await state.update_data(target_user_id=user_id, grant_plan_id=plan_id)
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.regexp(r"^admin:usercard:grant_tariff_confirm:\d+:[A-Za-z0-9_.-]+$"))
async def admin_user_card_grant_tariff_confirm(callback: CallbackQuery, db: Database, panel, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    parts = callback.data.split(":")
    user_id = int(parts[-2])
    plan_id = parts[-1]
    ok, prefix = await grant_user_tariff(
        actor_user_id=callback.from_user.id,
        target_user_id=user_id,
        plan_id=plan_id,
        db=db,
        panel=panel,
        bot=bot,
    )
    if not ok:
        await callback.answer(prefix, show_alert=True)
        return
    await edit_user_card_message(callback, db, user_id, panel=panel, prefix=prefix)
    await callback.answer("Тариф выдан")


@router.callback_query(F.data.startswith("admin:usercard:reset_trial:"))
async def admin_user_card_reset_trial(callback: CallbackQuery, db: Database, panel):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    user = await db.get_user(user_id)
    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return
    ok, prefix = await reset_user_trial(
        actor_user_id=callback.from_user.id,
        target_user_id=user_id,
        db=db,
    )
    if not ok:
        await callback.answer(prefix, show_alert=True)
        return
    await edit_user_card_message(callback, db, user_id, panel=panel, prefix=prefix)
    await callback.answer("Пробный период сброшен")


@router.callback_query(F.data.startswith("admin:usercard:referral_menu:"))
async def admin_user_card_referral_menu(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    text, markup = await build_referral_menu_screen(db, user_id)
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:rebind_referrer:"))
async def admin_user_card_rebind_referrer_prompt(callback: CallbackQuery, state: FSMContext, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    await state.set_state(PaymentDiagnosticsFSM.waiting_user_referrer_id)
    await state.update_data(target_user_id=user_id)
    await smart_edit_message(callback.message, await build_rebind_referrer_prompt_text(db, user_id), parse_mode="HTML", reply_markup=user_rebind_referrer_markup(user_id))
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:referrals_list:"))
async def admin_user_card_referrals_list(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    text, markup = await build_referrals_list_screen(db, user_id)
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:referrals_history:"))
async def admin_user_card_referrals_history(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    text, markup = await build_referrals_history_screen(db, user_id)
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:referral_last_payment:"))
async def admin_user_card_referral_last_payment(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    text, markup = await build_referral_last_payment_screen(db, user_id)
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:partner_rates:"))
async def admin_user_card_partner_rates_prompt(callback: CallbackQuery, state: FSMContext, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    await state.set_state(PaymentDiagnosticsFSM.waiting_user_partner_rates)
    await state.update_data(target_user_id=user_id)
    text, markup = await build_partner_rates_prompt_screen(db, user_id)
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await callback.answer()


@router.message(PaymentDiagnosticsFSM.waiting_user_balance_adjustment)
async def admin_user_card_balance_save(message: Message, state: FSMContext, db: Database, panel):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    user_id = int(data.get("target_user_id") or 0)
    parts = (message.text or "").split(maxsplit=1)
    if user_id <= 0 or not parts:
        await message.answer("❌ Не удалось определить пользователя.")
        return
    try:
        amount = float(parts[0].replace(",", "."))
    except ValueError:
        await message.answer("❌ Укажите сумму числом, например <code>+150</code>.", parse_mode="HTML")
        return
    reason = parts[1].strip() if len(parts) > 1 else "Быстрая корректировка из карточки"
    prefix = await save_user_balance_adjustment(
        actor_user_id=message.from_user.id,
        target_user_id=user_id,
        amount=amount,
        reason=reason,
        db=db,
    )
    await state.clear()
    await send_user_card_message(message, db, user_id, panel=panel, prefix=prefix)


@router.message(PaymentDiagnosticsFSM.waiting_user_partner_rates)
async def admin_user_card_partner_rates_save(message: Message, state: FSMContext, db: Database):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    user_id = int(data.get("target_user_id") or 0)
    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=4)
    if user_id <= 0 or len(parts) < 4:
        await message.answer("❌ Отправьте строку в формате:\n<code>level1 level2 level3 status note</code>", parse_mode="HTML")
        return
    try:
        def _optional_percent(value: str):
            value = (value or "").strip()
            if value in {"", "-", "none", "null"}:
                return None
            return float(value.replace(",", "."))

        l1 = _optional_percent(parts[0])
        l2 = _optional_percent(parts[1])
        l3 = _optional_percent(parts[2])
    except Exception:
        await message.answer("❌ Проценты должны быть числами или <code>-</code>.", parse_mode="HTML")
        return
    status = parts[3].strip().lower()
    note = parts[4].strip() if len(parts) > 4 else ""
    if status not in {"standard", "partner", "vip", "ambassador"}:
        await message.answer("❌ Статус: <code>standard / partner / vip / ambassador</code>.", parse_mode="HTML")
        return
    prefix = await save_user_partner_rates(
        actor_user_id=message.from_user.id,
        target_user_id=user_id,
        l1=l1,
        l2=l2,
        l3=l3,
        status=status,
        note=note,
        db=db,
    )
    await state.clear()
    text, markup = await build_referral_menu_screen(db, user_id)
    await message.answer(prefix + text, parse_mode="HTML", reply_markup=markup)


@router.message(PaymentDiagnosticsFSM.waiting_user_referrer_id)
async def admin_user_card_referrer_save(message: Message, state: FSMContext, db: Database):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    user_id = int(data.get("target_user_id") or 0)
    raw = (message.text or "").strip()
    try:
        referrer_id = int(raw)
    except ValueError:
        await message.answer("❌ Отправьте числовой ID реферера или <code>0</code>.", parse_mode="HTML")
        return
    if user_id <= 0:
        await state.clear()
        await message.answer("❌ Не удалось определить пользователя.")
        return
    if referrer_id == user_id:
        await message.answer("❌ Пользователь не может быть рефералом сам себе.", parse_mode="HTML")
        return
    if referrer_id > 0:
        referrer = await db.get_user(referrer_id)
        if not referrer:
            await message.answer("❌ Реферер не найден.", parse_mode="HTML")
            return
    prefix = await save_user_referrer(
        actor_user_id=message.from_user.id,
        target_user_id=user_id,
        referrer_id=referrer_id,
        db=db,
    )
    await state.clear()
    text, markup = await build_referral_menu_screen(db, user_id)
    await message.answer(prefix + text, parse_mode="HTML", reply_markup=markup)


@router.message(PaymentDiagnosticsFSM.waiting_user_grant_custom_days)
async def admin_user_card_grant_custom_days_save(message: Message, state: FSMContext, db: Database, panel, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    user_id = int(data.get("target_user_id") or 0)
    plan_id = str(data.get("grant_plan_id") or "").strip()
    if user_id <= 0 or not plan_id:
        await state.clear()
        await message.answer("❌ Не удалось определить пользователя или тариф.")
        return
    try:
        days = int((message.text or "").strip())
    except ValueError:
        await message.answer("❌ Укажите срок числом, например <code>45</code>.", parse_mode="HTML")
        return
    if days <= 0 or days > 3650:
        await message.answer("❌ Введите срок от 1 до 3650 дней.", parse_mode="HTML")
        return
    await state.clear()
    ok, prefix = await grant_custom_tariff_days(actor_user_id=message.from_user.id, target_user_id=user_id, plan_id=plan_id, days=days, db=db, panel=panel, bot=bot)
    if not ok:
        await message.answer(prefix, parse_mode="HTML")
        return
    await send_user_card_message(message, db, user_id, panel=panel, prefix=prefix)


@router.callback_query(F.data.startswith("admin:usercard:ban_toggle:"))
async def admin_user_card_ban_toggle(callback: CallbackQuery, db: Database, panel):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    user = await db.get_user(user_id)
    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return
    if bool(user.get("banned")):
        await db.unban_user(user_id)
        if hasattr(db, "add_admin_user_action"):
            await db.add_admin_user_action(user_id, callback.from_user.id, "unban", "")
        await callback.answer("Бан снят")
    else:
        await db.ban_user(user_id, reason=f"admin_quick_action:{callback.from_user.id}")
        if hasattr(db, "add_admin_user_action"):
            await db.add_admin_user_action(user_id, callback.from_user.id, "ban", "quick action")
        await callback.answer("Пользователь забанен")
    await edit_user_card_message(callback, db, user_id, panel=panel)


@router.callback_query(F.data.startswith("admin:usercard:reset_notify:"))
async def admin_user_card_reset_notifications(callback: CallbackQuery, db: Database, panel):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    await db.reset_expiry_notifications(user_id)
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(user_id, callback.from_user.id, "reset_expiry_notifications", "")
    await callback.answer("Уведомления сброшены")
    await edit_user_card_message(callback, db, user_id, panel=panel)


@router.callback_query(F.data.startswith("admin:usercard:revoke_subscription:"))
async def admin_user_card_revoke_subscription(callback: CallbackQuery, db: Database, panel, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    ok, prefix = await revoke_user_subscription(db, panel, bot, user_id, callback.from_user.id)
    if not ok and prefix == "Пользователь не найден":
        await callback.answer(prefix, show_alert=True)
        return
    await edit_user_card_message(callback, db, user_id, panel=panel, bot=bot, prefix=prefix, preserve_display_name=True)
    await callback.answer("Подписка отключена" if ok else "Не удалось отключить")


@router.callback_query(F.data.startswith("admin:usercard:delete_prompt:"))
async def admin_user_card_delete_prompt(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    ok, text = await build_delete_prompt_text(db, user_id)
    if not ok:
        await callback.answer(text, show_alert=True)
        return
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=user_delete_confirm_markup(user_id))
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:delete_confirm:"))
async def admin_user_card_delete_confirm(callback: CallbackQuery, db: Database, panel):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    text = await perform_delete_user(db, panel, user_id)
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")]]))
    await callback.answer("Пользователь удалён")


@router.callback_query(F.data.startswith("admin:usercard:support_menu:"))
async def admin_user_card_support_menu(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    restriction = await db.get_support_restriction(user_id) if hasattr(db, "get_support_restriction") else {}
    text = await build_support_menu_text(db, user_id)
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=user_support_menu_markup(user_id, support_blocked=bool(restriction.get("active"))))
    await callback.answer()


@router.callback_query(F.data == "admin:support_restrictions:list")
async def admin_support_restrictions_list(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    text, markup = await build_support_restrictions_screen(db)
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data == "admin:support_restrictions:toggle_notify")
async def admin_support_restrictions_toggle_notify(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    current = await db.support_restriction_notifications_enabled() if hasattr(db, "support_restriction_notifications_enabled") else True
    await db.set_support_restriction_notifications_enabled(not current)
    text, markup = await build_support_restrictions_screen(db)
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await callback.answer("Настройка обновлена")


@router.callback_query(F.data.startswith("admin:usercard:support_block:"))
async def admin_user_card_support_block(callback: CallbackQuery, db: Database, bot: Bot, panel):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    _, _, _, user_id_raw, preset_key = callback.data.split(":")
    user_id = int(user_id_raw)
    ok, result_text = await enable_support_restriction(db, bot, user_id, callback.from_user.id, preset_key)
    if not ok:
        await callback.answer(result_text, show_alert=True)
        return
    await callback.answer(result_text)
    await edit_user_card_message(callback, db, user_id, panel=panel)


@router.callback_query(F.data.startswith("admin:usercard:support_unblock:"))
async def admin_user_card_support_unblock(callback: CallbackQuery, db: Database, bot: Bot, panel):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    await disable_support_restriction(db, bot, user_id, callback.from_user.id)
    await callback.answer("Ограничение снято")
    await edit_user_card_message(callback, db, user_id, panel=panel)
