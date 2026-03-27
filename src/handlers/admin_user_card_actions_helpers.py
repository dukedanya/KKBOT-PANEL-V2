import logging
from html import escape

from aiogram import Bot
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from db import Database
from handlers.admin_user_card_helpers import (
    _build_support_restrictions_list_text,
    _build_user_card_text,
    _notify_support_restriction_admins,
    _support_restrictions_keyboard,
    _user_card_keyboard,
    _user_card_support_keyboard,
    _user_delete_confirm_keyboard,
    _resolve_user_display_name,
)
from handlers.payment_diagnostics_helpers import SUPPORT_RESTRICTION_PRESETS
from kkbot.services.subscriptions import panel_base_email, revoke_subscription
from utils.support import format_support_restriction_reason
from utils.telegram_ui import smart_edit_message

logger = logging.getLogger(__name__)


async def _resolve_user_card_state(db: Database, user_id: int) -> tuple[dict, dict]:
    user = await db.get_user(user_id) or {}
    restriction = await db.get_support_restriction(user_id) if hasattr(db, "get_support_restriction") else {}
    return user, restriction


def _user_card_markup(user_id: int, user: dict, restriction: dict):
    return _user_card_keyboard(
        user_id,
        banned=bool(user.get("banned")),
        support_blocked=bool(restriction.get("active")),
    )


async def send_user_card_message(
    message: Message,
    db: Database,
    user_id: int,
    *,
    panel=None,
    bot: Bot | None = None,
    prefix: str = "",
) -> None:
    user, restriction = await _resolve_user_card_state(db, user_id)
    text = await _build_user_card_text(db, user_id, panel=panel)
    await message.answer(
        prefix + text,
        parse_mode="HTML",
        reply_markup=_user_card_markup(user_id, user, restriction),
    )


async def edit_user_card_message(
    callback: CallbackQuery,
    db: Database,
    user_id: int,
    *,
    panel=None,
    bot: Bot | None = None,
    prefix: str = "",
    preserve_display_name: bool = False,
) -> None:
    user, restriction = await _resolve_user_card_state(db, user_id)
    display_name = None
    if preserve_display_name and bot:
        display_name = await _resolve_user_display_name(bot, user_id, user)
    text = await _build_user_card_text(db, user_id, panel=panel, display_name_override=display_name)
    await smart_edit_message(
        callback.message,
        prefix + text,
        parse_mode="HTML",
        reply_markup=_user_card_markup(user_id, user, restriction),
    )


async def build_delete_prompt_text(db: Database, user_id: int) -> tuple[bool, str]:
    user = await db.get_user(user_id)
    if not user:
        return False, "Пользователь не найден"
    base_email = await panel_base_email(user_id, db)
    text = (
        "🗑 <b>Удаление пользователя</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n"
        f"Panel email: <code>{escape(base_email or '-')}</code>\n\n"
        "Будет удалён из бота, PostgreSQL/SQLite и из панели 3x-ui.\n"
        "Действие необратимо."
    )
    return True, text


async def perform_delete_user(db: Database, panel, user_id: int) -> str:
    base_email = await panel_base_email(user_id, db)
    panel_result = {"found": 0, "deleted": 0, "already_missing": 0, "errors": []}
    panel_error = ""
    if base_email:
        try:
            if hasattr(panel, "delete_client_detailed"):
                panel_result = await panel.delete_client_detailed(base_email)
            else:
                panel_deleted = await panel.delete_client(base_email)
                panel_result = {
                    "found": 1 if panel_deleted else 0,
                    "deleted": 1 if panel_deleted else 0,
                    "already_missing": 0,
                    "errors": [],
                }
        except Exception as exc:
            panel_error = str(exc)
            logger.error("User delete: panel cleanup failed user=%s error=%s", user_id, exc)

    stats = await db.delete_user_everywhere(user_id)
    panel_status = "не требовалось"
    if base_email:
        if panel_error:
            panel_status = "ошибка"
        elif int(panel_result.get("deleted", 0)) > 0:
            panel_status = f"удалён: {int(panel_result.get('deleted', 0))}"
            if int(panel_result.get("already_missing", 0)) > 0:
                panel_status += f", уже отсутствовал: {int(panel_result.get('already_missing', 0))}"
        elif int(panel_result.get("already_missing", 0)) > 0:
            panel_status = f"уже отсутствовал: {int(panel_result.get('already_missing', 0))}"
        elif int(panel_result.get("found", 0)) == 0:
            panel_status = "клиенты не найдены"
        elif panel_result.get("errors"):
            panel_status = "частично с ошибками"

    text = (
        "✅ <b>Пользователь удалён</b>\n\n"
        f"User ID: <code>{user_id}</code>\n"
        f"Панель 3x-ui: <b>{escape(panel_status)}</b>\n"
        f"Удалено записей в БД: <b>{int(stats.get('deleted', 0))}</b>"
    )
    if panel_error:
        text += f"\nОшибка панели: <code>{escape(panel_error[:300])}</code>"
    elif panel_result.get("errors"):
        text += f"\nДетали панели: <code>{escape(str(panel_result['errors'][0])[:300])}</code>"
    return text


async def build_support_menu_text(db: Database, user_id: int) -> str:
    restriction = await db.get_support_restriction(user_id) if hasattr(db, "get_support_restriction") else {}
    return (
        "🆘 <b>Ограничение поддержки</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n"
        f"Сейчас ограничено: <b>{'да' if restriction.get('active') else 'нет'}</b>\n"
        f"До: <code>{restriction.get('expires_at') or '-'}</code>\n"
        f"Причина: <code>{escape(format_support_restriction_reason(str(restriction.get('reason') or '-')))}</code>"
    )


async def build_support_restrictions_screen(db: Database):
    rows = await db.list_support_restricted_users(limit=20) if hasattr(db, "list_support_restricted_users") else []
    notify_enabled = await db.support_restriction_notifications_enabled() if hasattr(db, "support_restriction_notifications_enabled") else True
    text = await _build_support_restrictions_list_text(db)
    markup = _support_restrictions_keyboard(rows, notify_enabled=notify_enabled)
    return text, markup


async def enable_support_restriction(db: Database, bot: Bot, user_id: int, admin_user_id: int, preset_key: str) -> tuple[bool, str]:
    preset = SUPPORT_RESTRICTION_PRESETS.get(preset_key)
    if not preset:
        return False, "Неизвестная причина"
    await db.set_support_restriction(user_id, hours=int(preset["hours"]), reason=f"{preset['reason']} by admin {admin_user_id}")
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(user_id, admin_user_id, "support_restriction_set", f"{preset['reason']} {preset['hours']}h")
    await _notify_support_restriction_admins(
        db,
        bot,
        "🆘 <b>Ограничение поддержки включено</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n"
        f"Причина: <code>{preset['reason']}</code>\n"
        f"Срок: <b>{preset['hours']} ч</b>\n"
        f"Админ: <code>{admin_user_id}</code>",
    )
    return True, "Ограничение поддержки включено"


async def disable_support_restriction(db: Database, bot: Bot, user_id: int, admin_user_id: int) -> None:
    await db.clear_support_restriction(user_id)
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(user_id, admin_user_id, "support_restriction_cleared", "")
    await _notify_support_restriction_admins(
        db,
        bot,
        "✅ <b>Ограничение поддержки снято</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n"
        f"Админ: <code>{admin_user_id}</code>",
    )


async def revoke_user_subscription(db: Database, panel, bot: Bot, user_id: int, admin_user_id: int) -> tuple[bool, str]:
    user = await db.get_user(user_id)
    if not user:
        return False, "Пользователь не найден"
    ok = await revoke_subscription(user_id, db=db, panel=panel, reason="Отключено администратором")
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(user_id, admin_user_id, "revoke_subscription", f"ok={int(bool(ok))}")
    if ok:
        await bot.send_message(
            user_id,
            "⛔ <b>Подписка отключена администратором</b>\n\nЕсли это ошибка, напишите в поддержку.",
            parse_mode="HTML",
        )
    return ok, ("⛔ Подписка отключена.\n\n" if ok else "⚠️ Не удалось отключить подписку полностью.\n\n")


def user_delete_confirm_markup(user_id: int):
    return _user_delete_confirm_keyboard(user_id)


def user_support_menu_markup(user_id: int, support_blocked: bool):
    return _user_card_support_keyboard(user_id, support_blocked=support_blocked)

