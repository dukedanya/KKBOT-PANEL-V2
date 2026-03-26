from html import escape
import time
from datetime import datetime
from typing import Dict, Tuple

from aiogram import Bot, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config import Config
from db import Database
from keyboards import back_keyboard, support_menu_keyboard
from utils.helpers import notify_user, register_transient_message
from utils.support import format_support_restriction_reason, format_support_status
from utils.telegram_ui import smart_edit_message
from utils.templates import render_template

router = Router()

_LAST_SUPPORT_MESSAGE: Dict[Tuple[int, str], Tuple[float, str]] = {}


class SupportFSM(StatesGroup):
    waiting_user_message = State()
    waiting_admin_reply = State()


SUPPORT_REPLY_TEMPLATES = {
    "check_payment": "Пожалуйста, отправьте чек или точное время оплаты. Мы проверим платёж вручную.",
    "reinstall_profile": "Попробуйте заново установить профиль по актуальной инструкции. Если не поможет, пришлите скриншот ошибки.",
    "access_activated": "Проверили доступ: подписка активна. Если приложение ещё не подключается, перезапустите клиент и обновите профиль.",
    "send_instruction": "Отправляем инструкцию по подключению. Если какой-то шаг не получается, напишите, на каком этапе возникла ошибка.",
}

SUPPORT_AUTOHELP_RULES = {
    "connect": {
        "keywords": ("не подключ", "не работает", "ошибка подключения", "не соединяется", "vpn не работает"),
        "text": (
            "🛠 <b>Похоже, проблема с подключением</b>\n\n"
            "Попробуйте сразу:\n"
            "1. Ещё раз открыть ссылку подключения.\n"
            "2. Проверить, что профиль импортирован в Happ.\n"
            "3. Перезапустить приложение.\n"
            "4. Если не помогло, напишите следующим сообщением, что уже проверили."
        ),
    },
    "slow": {
        "keywords": ("медленно", "тормозит", "низкая скорость", "долго грузит"),
        "text": (
            "🛠 <b>Похоже, проблема со скоростью</b>\n\n"
            "Попробуйте:\n"
            "1. Переключить Wi‑Fi / мобильный интернет.\n"
            "2. Полностью перезапустить VPN-клиент.\n"
            "3. Подождать 1–2 минуты и проверить снова.\n"
            "4. Если не помогло, напишите страну и оператора."
        ),
    },
    "sites": {
        "keywords": ("не открыва", "сайты не груз", "ничего не открывает", "не грузит сайты"),
        "text": (
            "🛠 <b>Похоже, не открываются сайты</b>\n\n"
            "Попробуйте:\n"
            "1. Выключить и снова включить VPN.\n"
            "2. Проверить, открываются ли сайты без VPN.\n"
            "3. Если проблема только у отдельных сайтов, пришлите их списком следующим сообщением."
        ),
    },
    "payment": {
        "keywords": ("оплатил", "списались деньги", "нет доступа", "не активировалось", "после оплаты"),
        "text": (
            "🛠 <b>Похоже, вопрос по оплате</b>\n\n"
            "Сначала попробуйте:\n"
            "1. Нажать проверку оплаты в боте.\n"
            "2. Открыть личный кабинет и проверить ссылку подключения.\n"
            "3. Если доступа всё ещё нет, следующим сообщением пришлите время оплаты."
        ),
    },
}


def _admin_ticket_keyboard(ticket_id: int, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="💬 Ответить", callback_data=f"support:reply:{ticket_id}:{user_id}"),
                InlineKeyboardButton(text="⚡ Шаблоны", callback_data=f"support:templates:{ticket_id}:{user_id}"),
                InlineKeyboardButton(text="🗑 Удалить", callback_data=f"support:close:{ticket_id}:{user_id}"),
            ],
            [
                InlineKeyboardButton(text="👤 Карточка", callback_data=f"admin:usercard:{user_id}"),
            ],
        ]
    )


def _user_ticket_keyboard(ticket_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="💬 Ответить", callback_data=f"support:user_reply:{ticket_id}"),
            InlineKeyboardButton(text="✅ Закрыть вопрос", callback_data=f"support:user_close:{ticket_id}"),
        ]]
    )


def _main_menu_only_keyboard() -> InlineKeyboardMarkup:
    return back_keyboard()


def _extract_support_payload(message: Message):
    text = (message.text or message.caption or "").strip()
    media_type = ""
    media_file_id = ""
    if message.photo:
        media_type = "photo"
        media_file_id = message.photo[-1].file_id
    return text, media_type, media_file_id


def _quote_block(text: str) -> str:
    return f"<blockquote>{escape(text)}</blockquote>\n\n" if text else ""


def _support_spam_guard(user_id: int, role: str, payload: str) -> bool:
    now = time.time()
    key = (user_id, role)
    last = _LAST_SUPPORT_MESSAGE.get(key)
    if last and now - last[0] < 3 and last[1] == payload:
        return False
    _LAST_SUPPORT_MESSAGE[key] = (now, payload)
    return True


def _support_blacklist_phrases(raw: str) -> list[str]:
    phrases = []
    for line in (raw or "").splitlines():
        normalized = " ".join(line.strip().lower().split())
        if normalized:
            phrases.append(normalized)
    return phrases


def _match_support_blacklist(text: str, phrases: list[str]) -> str:
    normalized = " ".join((text or "").strip().lower().split())
    if not normalized:
        return ""
    for phrase in phrases:
        if phrase and phrase in normalized:
            return phrase
    return ""


def _match_support_autohelp(text: str) -> tuple[str, str]:
    normalized = " ".join((text or "").strip().lower().split())
    if not normalized:
        return "", ""
    for key, rule in SUPPORT_AUTOHELP_RULES.items():
        for keyword in rule["keywords"]:
            if keyword in normalized:
                return key, rule["text"]
    return "", ""


def _format_support_dt(value: str) -> str:
    if not value:
        return "неизвестно"
    try:
        return datetime.fromisoformat(value).strftime("%d.%m.%Y %H:%M")
    except ValueError:
        return value


def _trim_text(value: str, limit: int = 40) -> str:
    text = " ".join((value or "").split())
    if not text:
        return "Без текста"
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _safe_support_text(value: str) -> str:
    return escape((value or "").strip())


async def _register_support_notice(db: Database, *, chat_id: int, message_id: int, category: str, ttl_hours: int) -> None:
    await register_transient_message(
        db=db,
        chat_id=chat_id,
        message_id=message_id,
        category=category,
        ttl_hours=ttl_hours,
    )


async def _notify_admins_with_optional_ttl(
    bot: Bot,
    db: Database,
    text: str,
    *,
    reply_markup: InlineKeyboardMarkup | None = None,
    category: str | None = None,
    ttl_hours: int = 24,
) -> None:
    for admin_id in Config.ADMIN_USER_IDS:
        try:
            sent = await bot.send_message(admin_id, text, parse_mode="HTML", reply_markup=reply_markup)
            if category:
                await _register_support_notice(
                    db,
                    chat_id=admin_id,
                    message_id=sent.message_id,
                    category=category,
                    ttl_hours=ttl_hours,
                )
        except TelegramBadRequest:
            continue


async def _send_admin_ticket_message(
    bot: Bot,
    admin_id: int,
    *,
    media_type: str,
    media_file_id: str,
    text: str,
    reply_markup: InlineKeyboardMarkup,
) -> None:
    if media_type == "photo" and media_file_id:
        await bot.send_photo(admin_id, photo=media_file_id, caption=text, parse_mode="HTML", reply_markup=reply_markup)
        return
    await bot.send_message(admin_id, text, parse_mode="HTML", reply_markup=reply_markup)


async def _send_support_reply_to_user(
    bot: Bot,
    *,
    user_id: int,
    ticket_id: int,
    text: str,
    media_type: str = "",
    media_file_id: str = "",
) -> None:
    reply_markup = _user_ticket_keyboard(ticket_id)
    if media_type == "photo" and media_file_id:
        try:
            await bot.send_photo(user_id, photo=media_file_id, caption=text, parse_mode="HTML", reply_markup=reply_markup)
            return
        except TelegramBadRequest:
            pass
    await notify_user(user_id, text, reply_markup=reply_markup, bot=bot)


def _support_history_keyboard(tickets: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for ticket in tickets:
        status = format_support_status(ticket.get("status"))
        rows.append([
            InlineKeyboardButton(
                text=f"#{ticket['id']} • {status}",
                callback_data=f"support:view:{ticket['id']}",
            )
        ])
    rows.append([InlineKeyboardButton(text="⬅️ К поддержке", callback_data="user_menu:support")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _admin_reply_templates_keyboard(ticket_id: int, user_id: int) -> InlineKeyboardMarkup:
    rows = []
    labels = {
        "check_payment": "💳 Запросить чек",
        "reinstall_profile": "🔁 Переустановить профиль",
        "access_activated": "✅ Доступ активирован",
        "send_instruction": "📖 Отправить инструкцию",
    }
    for key, label in labels.items():
        rows.append([InlineKeyboardButton(text=label, callback_data=f"support:template_send:{ticket_id}:{user_id}:{key}")])
    rows.append([InlineKeyboardButton(text="⬅️ К тикету", callback_data=f"support:reply:{ticket_id}:{user_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _support_ticket_detail_keyboard(ticket_id: int, status: str) -> InlineKeyboardMarkup:
    rows = []
    if status != "closed":
        rows.append([InlineKeyboardButton(text="💬 Ответить", callback_data=f"support:user_reply:{ticket_id}")])
        rows.append([InlineKeyboardButton(text="✅ Закрыть вопрос", callback_data=f"support:user_close:{ticket_id}")])
    rows.append([InlineKeyboardButton(text="📜 К истории обращений", callback_data="support:history")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _ensure_support_available(target, db: Database) -> bool:
    user_id = None
    if isinstance(target, Message):
        user_id = target.from_user.id if target.from_user else None
    elif isinstance(target, CallbackQuery):
        user_id = target.from_user.id if target.from_user else None
    if not user_id:
        return False
    restriction = await db.get_support_restriction(int(user_id)) if hasattr(db, "get_support_restriction") else {}
    if not restriction or not restriction.get("active"):
        return True
    text = (
        "⛔ <b>Доступ к поддержке временно ограничен.</b>\n\n"
        f"До: <code>{restriction.get('expires_at') or '-'}</code>\n"
        f"Причина: <code>{escape(format_support_restriction_reason(str(restriction.get('reason') or '-')))}</code>"
    )
    if isinstance(target, Message):
        await target.answer(text, parse_mode="HTML", reply_markup=_main_menu_only_keyboard())
    else:
        try:
            await target.answer("Доступ к поддержке временно ограничен", show_alert=True)
        except Exception:
            pass
        if target.message:
            await smart_edit_message(target.message, text, parse_mode="HTML", reply_markup=_main_menu_only_keyboard())
    return False


async def _render_support_ticket_view(db: Database, ticket: dict, *, limit: int = 10) -> str:
    messages = await db.get_support_messages(int(ticket["id"]), limit=limit)
    lines = [
        "🆘 <b>Обращение в поддержку</b>",
        "",
        f"Тикет: <code>#{ticket['id']}</code>",
        f"Статус: <b>{format_support_status(ticket.get('status', ''))}</b>",
        f"Обновлён: <b>{_format_support_dt(ticket.get('updated_at', ''))}</b>",
        "",
    ]
    if not messages:
        lines.append("<i>Сообщений пока нет.</i>")
        return "\n".join(lines)

    lines.append("<b>Последние сообщения:</b>")
    for item in messages[-limit:]:
        role = "Вы" if item.get("sender_role") == "user" else "Поддержка"
        created_at = _format_support_dt(item.get("created_at", ""))
        body = _safe_support_text(item.get("text", "")) or ("📷 Фото без подписи" if item.get("media_type") == "photo" else "Вложение без текста")
        lines.extend([
            "",
            f"<b>{role}</b> • <code>{created_at}</code>",
            body,
        ])
    return "\n".join(lines)


@router.callback_query(F.data == "support:history")
async def support_history(callback: CallbackQuery, db: Database):
    tickets = await db.list_user_support_tickets(callback.from_user.id, limit=10)
    if not tickets:
        await smart_edit_message(
            callback.message,
            "📜 <b>История обращений</b>\n\nУ вас пока нет обращений в поддержку.",
            parse_mode="HTML",
            reply_markup=support_menu_keyboard(),
        )
        await callback.answer()
        return

    text_lines = ["📜 <b>История обращений</b>", "", "Выберите заявку из списка ниже:"]
    for ticket in tickets:
        preview = await db.get_last_support_message(int(ticket["id"]))
        preview_text = preview.get("text") if preview else ""
        preview_body = _safe_support_text(
            _trim_text(preview_text or ('Фото без подписи' if preview and preview.get('media_type') == 'photo' else 'Без сообщений'))
        )
        text_lines.extend([
            "",
            f"• <b>#{ticket['id']}</b> — {format_support_status(ticket.get('status', ''))}",
            f"  {preview_body}",
        ])
    await smart_edit_message(
        callback.message,
        "\n".join(text_lines),
        parse_mode="HTML",
        reply_markup=_support_history_keyboard(tickets),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("support:view:"))
async def support_view_ticket(callback: CallbackQuery, db: Database):
    ticket_id = int(callback.data.split(":")[2])
    ticket = await db.get_support_ticket(ticket_id)
    if not ticket or int(ticket.get("user_id", 0)) != callback.from_user.id:
        await callback.answer("Обращение не найдено", show_alert=True)
        return

    text = await _render_support_ticket_view(db, ticket)
    await smart_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=_support_ticket_detail_keyboard(ticket_id, ticket.get("status", "")),
    )
    await callback.answer()


@router.callback_query(F.data == "support:start")
async def support_start(callback: CallbackQuery, state: FSMContext, db: Database):
    if not await _ensure_support_available(callback, db):
        return
    await state.set_state(SupportFSM.waiting_user_message)
    await state.update_data(ticket_id=None)
    await smart_edit_message(
        callback.message,
        "✉️ <b>Напишите сообщение для тех. поддержки</b>\n\nМожно отправить текст или фото с подписью следующим сообщением. Если проблема типовая, бот сначала подскажет решение автоматически.",
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("support:user_reply:"))
async def support_user_reply(callback: CallbackQuery, state: FSMContext, db: Database):
    if not await _ensure_support_available(callback, db):
        return
    ticket_id = int(callback.data.split(":")[2])
    await state.set_state(SupportFSM.waiting_user_message)
    await state.update_data(ticket_id=ticket_id)
    await smart_edit_message(callback.message, "💬 <b>Введите ответ для тех. поддержки</b>\n\nМожно отправить текст или фото с подписью.", parse_mode="HTML")
    await callback.answer()


@router.message(SupportFSM.waiting_user_message)
async def support_user_message(message: Message, state: FSMContext, db: Database, bot: Bot):
    if not await _ensure_support_available(message, db):
        await state.clear()
        return
    data = await state.get_data()
    ticket_id = data.get("ticket_id") or await db.get_or_create_support_ticket(message.from_user.id)
    autohelp_shown = str(data.get("autohelp_shown") or "")
    last_admin = await db.get_last_support_message(ticket_id, "admin")
    text, media_type, media_file_id = _extract_support_payload(message)
    if not text and not media_file_id:
        await message.answer("Отправьте текст или фото с подписью для поддержки.")
        return
    blacklist_raw = await db.get_setting("support:blacklist_phrases", "") if hasattr(db, "get_setting") else ""
    blocked_phrase = _match_support_blacklist(text, _support_blacklist_phrases(blacklist_raw))
    if blocked_phrase:
        if hasattr(db, "add_antifraud_event"):
            await db.add_antifraud_event(
                message.from_user.id,
                "support_blacklist",
                details=f"phrase={blocked_phrase}",
                severity="warning",
            )
        if Config.SUPPORT_BLACKLIST_NOTIFY_ADMINS:
            admin_notice = (
                "🛡 <b>Support blacklist сработал</b>\n\n"
                f"Пользователь: <code>{message.from_user.id}</code>\n"
                f"Фраза: <code>{escape(blocked_phrase)}</code>\n"
                f"Текст: <code>{escape((text or '')[:500])}</code>"
            )
            await _notify_admins_with_optional_ttl(
                bot,
                db,
                admin_notice,
                category="support_blacklist_notice",
                ttl_hours=48,
            )
        await message.answer(
            "⚠️ Сообщение не отправлено в поддержку. Уточните запрос без запрещённых фраз или вернитесь в главное меню.",
            reply_markup=_main_menu_only_keyboard(),
        )
        await state.clear()
        return
    payload_key = text or f"media:{media_file_id}"
    if not _support_spam_guard(message.from_user.id, "user", payload_key):
        await message.answer("⏳ Подождите пару секунд перед повторной отправкой.")
        return

    autohelp_key, autohelp_text = _match_support_autohelp(text)
    if text and not autohelp_shown and not last_admin and autohelp_text:
        await state.update_data(ticket_id=ticket_id, autohelp_shown=autohelp_key)
        await message.answer(
            f"{autohelp_text}\n\nЕсли это не решило проблему, просто напишите следующим сообщением подробнее, и я передам вопрос в поддержку.",
            parse_mode="HTML",
            reply_markup=_main_menu_only_keyboard(),
        )
        return

    await db.add_support_message(ticket_id, "user", message.from_user.id, text, media_type=media_type, media_file_id=media_file_id)
    if hasattr(db, 'set_support_ticket_status'):
        await db.set_support_ticket_status(ticket_id, "open")
    quote = _quote_block((last_admin or {}).get("text", ""))
    rendered_text = _safe_support_text(text)
    admin_text = (
        "🆘 <b>Новое сообщение в поддержку</b>\n\n"
        f"Тикет: <code>#{ticket_id}</code>\n"
        f"Пользователь: <code>{message.from_user.id}</code>\n\n"
        f"{quote}{rendered_text or '<i>Фото без подписи</i>'}"
    )
    for admin_id in Config.ADMIN_USER_IDS:
        try:
            await _send_admin_ticket_message(
                bot,
                admin_id,
                media_type=media_type,
                media_file_id=media_file_id,
                text=admin_text,
                reply_markup=_admin_ticket_keyboard(ticket_id, message.from_user.id),
            )
        except TelegramBadRequest:
            continue

    sent_text, _ = await render_template(db, "support_sent_user")
    sent = await message.answer(sent_text, parse_mode="HTML", reply_markup=_main_menu_only_keyboard())
    await _register_support_notice(db, chat_id=message.from_user.id, message_id=sent.message_id, category="support_sent_user", ttl_hours=12)
    await state.clear()


@router.callback_query(F.data.startswith("support:reply:"))
async def support_admin_reply(callback: CallbackQuery, state: FSMContext, db: Database):
    _, _, ticket_id, user_id = callback.data.split(":")
    await state.set_state(SupportFSM.waiting_admin_reply)
    await state.update_data(ticket_id=int(ticket_id), user_id=int(user_id))
    if hasattr(db, 'set_support_ticket_status'):
        await db.set_support_ticket_status(int(ticket_id), "in_progress", callback.from_user.id)
    await callback.message.answer(f"💬 Ответьте пользователю <code>{user_id}</code> следующим сообщением.\nМожно отправить текст или фото с подписью.", parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data.startswith("support:templates:"))
async def support_admin_templates(callback: CallbackQuery, db: Database):
    _, _, ticket_id, user_id = callback.data.split(":")
    ticket = await db.get_support_ticket(int(ticket_id))
    if not ticket:
        await callback.answer("Тикет не найден", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        "⚡ <b>Шаблоны ответов</b>\n\nВыберите готовый ответ для пользователя.",
        parse_mode="HTML",
        reply_markup=_admin_reply_templates_keyboard(int(ticket_id), int(user_id)),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("support:template_send:"))
async def support_admin_send_template(callback: CallbackQuery, db: Database, bot: Bot):
    _, _, ticket_id_raw, user_id_raw, template_key = callback.data.split(":")
    ticket_id = int(ticket_id_raw)
    user_id = int(user_id_raw)
    template_text = SUPPORT_REPLY_TEMPLATES.get(template_key, "").strip()
    if not template_text:
        await callback.answer("Шаблон не найден", show_alert=True)
        return
    await db.add_support_message(ticket_id, "admin", callback.from_user.id, template_text)
    if hasattr(db, "set_support_ticket_status"):
        await db.set_support_ticket_status(ticket_id, "in_progress", callback.from_user.id)
    title, _ = await render_template(db, "support_reply_title")
    user_text = f"{title}\n\n{escape(template_text)}"
    await _send_support_reply_to_user(bot, user_id=user_id, ticket_id=ticket_id, text=user_text)
    await smart_edit_message(
        callback.message,
        (
            "✅ <b>Шаблон отправлен</b>\n\n"
            f"Тикет: <code>#{ticket_id}</code>\n"
            f"Пользователь: <code>{user_id}</code>\n\n"
            f"{escape(template_text)}"
        ),
        parse_mode="HTML",
        reply_markup=_admin_ticket_keyboard(ticket_id, user_id),
    )
    await callback.answer("Шаблон отправлен")


@router.message(SupportFSM.waiting_admin_reply)
async def support_admin_send(message: Message, state: FSMContext, db: Database, bot: Bot):
    if message.from_user.id not in Config.ADMIN_USER_IDS:
        return
    data = await state.get_data()
    ticket_id = int(data["ticket_id"])
    user_id = int(data["user_id"])
    last_user = await db.get_last_support_message(ticket_id, "user")
    text, media_type, media_file_id = _extract_support_payload(message)
    if not text and not media_file_id:
        await message.answer("Отправьте текст или фото с подписью.")
        return
    payload_key = text or f"media:{media_file_id}"
    if not _support_spam_guard(message.from_user.id, "admin", payload_key):
        await message.answer("⏳ Подождите пару секунд перед повторной отправкой.")
        return

    await db.add_support_message(ticket_id, "admin", message.from_user.id, text, media_type=media_type, media_file_id=media_file_id)
    if hasattr(db, 'set_support_ticket_status'):
        await db.set_support_ticket_status(ticket_id, "in_progress", message.from_user.id)
    title, _ = await render_template(db, "support_reply_title")
    quote = _quote_block((last_user or {}).get("text", ""))
    rendered_text = _safe_support_text(text)
    user_text = f"{title}\n\n{quote}{rendered_text or '<i>Фото без подписи</i>'}"
    await _send_support_reply_to_user(
        bot,
        user_id=user_id,
        ticket_id=ticket_id,
        text=user_text,
        media_type=media_type,
        media_file_id=media_file_id,
    )
    sent_text, _ = await render_template(db, "support_sent_admin")
    sent = await message.answer(sent_text, parse_mode="HTML", reply_markup=_main_menu_only_keyboard())
    await _register_support_notice(db, chat_id=message.from_user.id, message_id=sent.message_id, category="support_sent_admin", ttl_hours=12)
    await state.clear()


@router.callback_query(F.data.startswith("support:close:"))
async def support_admin_close(callback: CallbackQuery, db: Database, bot: Bot):
    _, _, ticket_id, user_id = callback.data.split(":")
    await db.close_support_ticket(int(ticket_id))
    await callback.message.edit_reply_markup(reply_markup=None)
    dismiss_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🧹 Убрать уведомление", callback_data="dismiss_notice")]]
    )
    close_text, _ = await render_template(db, "support_closed_by_admin_user")
    sent = await bot.send_message(
        int(user_id),
        close_text,
        parse_mode="HTML",
        reply_markup=dismiss_keyboard,
    )
    await _register_support_notice(
        db,
        chat_id=int(user_id),
        message_id=sent.message_id,
        category="support_closed_notice",
        ttl_hours=24,
    )
    await callback.answer("Закрыто")


@router.callback_query(F.data.startswith("support:user_close:"))
async def support_user_close(callback: CallbackQuery, db: Database, bot: Bot):
    ticket_id = int(callback.data.split(":")[2])
    await db.close_support_ticket(ticket_id)
    try:
        await callback.message.delete()
    except TelegramBadRequest:
        await callback.message.edit_reply_markup(reply_markup=None)
    user_id = callback.from_user.id
    notice = (
        "✅ <b>Пользователь закрыл вопрос</b>\n\n"
        f"Тикет: <code>#{ticket_id}</code>\n"
        f"Пользователь: <code>{user_id}</code>"
    )
    await _notify_admins_with_optional_ttl(
        bot,
        db,
        notice,
        reply_markup=_main_menu_only_keyboard(),
        category="support_user_closed_notice",
        ttl_hours=24,
    )
    await callback.answer("Вопрос закрыт")
