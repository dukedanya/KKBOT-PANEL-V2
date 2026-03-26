import html
import logging
import secrets
import string
from typing import Any, Dict, List, Optional

from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message

from config import Config
from db import Database
from tariffs import get_all_active, is_trial_plan

logger = logging.getLogger(__name__)

# Legacy singleton bot. Оставлен только для совместимости со старыми вызовами helper-ов.
_bot: Optional[Bot] = None
user_last_msg: Dict[int, int] = {}
BOT_USERNAME: str = ""


def set_bot(bot: Bot, username: str = "") -> None:
    global _bot, BOT_USERNAME
    _bot = bot
    BOT_USERNAME = username or BOT_USERNAME


def get_bot() -> Bot:
    if _bot is None:
        raise RuntimeError("Bot not initialized | Передайте bot явно или вызовите set_bot() в main.py")
    return _bot


async def replace_message(
    user_id: int,
    text: str,
    reply_markup=None,
    parse_mode: Optional[str] = ParseMode.HTML,
    delete_user_msg: Optional[Message] = None,
    bot: Optional[Bot] = None,
    **kwargs,
) -> Optional[Message]:
    bot = bot or get_bot()
    msg = await bot.send_message(
        user_id, text, reply_markup=reply_markup, parse_mode=parse_mode, **kwargs
    )
    previous_msg_id = user_last_msg.get(user_id)
    if previous_msg_id:
        try:
            await bot.delete_message(user_id, previous_msg_id)
        except Exception as exc:
            logger.debug("Previous message cleanup skipped: msg=%s user=%s error=%s", previous_msg_id, user_id, exc)
    if delete_user_msg:
        try:
            await delete_user_msg.delete()
        except Exception as exc:
            logger.debug("User message delete skipped: user=%s error=%s", user_id, exc)
    user_last_msg[user_id] = msg.message_id
    return msg


async def safe_send_message(
    user_id: int,
    message: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    bot: Optional[Bot] = None,
) -> None:
    bot = bot or get_bot()
    try:
        await bot.send_message(
            user_id, message, parse_mode=ParseMode.HTML, reply_markup=reply_markup
        )
    except TelegramBadRequest as exc:
        logger.warning("HTML parse error: user=%s error=%s", user_id, exc)
        try:
            await bot.send_message(
                user_id,
                html.escape(message),
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
            )
        except Exception as escaped_exc:
            logger.warning("Escaped HTML send failed: user=%s error=%s", user_id, escaped_exc)
            try:
                await bot.send_message(user_id, message, reply_markup=reply_markup)
            except Exception as plain_exc:
                logger.error("Message send failed | Не удалось отправить сообщение: user=%s error=%s", user_id, plain_exc)
    except Exception as exc:
        logger.error("Message send failed | Не удалось отправить сообщение: user=%s error=%s", user_id, exc)


async def notify_admins(
    message: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    bot: Optional[Bot] = None,
) -> None:
    for admin_id in Config.ADMIN_USER_IDS:
        await safe_send_message(admin_id, message, reply_markup=reply_markup, bot=bot)


async def notify_user(
    user_id: int,
    message: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    bot: Optional[Bot] = None,
) -> None:
    await safe_send_message(user_id, message, reply_markup=reply_markup, bot=bot)


async def register_transient_message(
    *,
    db: Optional[Database],
    chat_id: int,
    message_id: int,
    category: str = "",
    ttl_hours: Optional[int] = None,
) -> None:
    if not db or not hasattr(db, "register_transient_message"):
        return
    try:
        await db.register_transient_message(
            chat_id=chat_id,
            message_id=message_id,
            category=category,
            ttl_hours=int(ttl_hours or Config.TRANSIENT_MESSAGE_DEFAULT_TTL_HOURS),
        )
    except Exception as exc:
        logger.debug("Transient message register skipped: chat=%s msg=%s error=%s", chat_id, message_id, exc)


async def smart_answer(event, text, reply_markup=None, delete_origin=False) -> None:
    try:
        if isinstance(event, Message):
            await event.answer(text, reply_markup=reply_markup)
            return
        if isinstance(event, CallbackQuery):
            if event.message:
                await event.message.answer(text, reply_markup=reply_markup)
                if delete_origin:
                    try:
                        await event.message.delete()
                    except Exception as exc:
                        logger.debug("Origin message delete skipped: %s", exc)
            try:
                await event.answer()
            except Exception as exc:
                logger.debug("Callback answer skipped: %s", exc)
    except Exception as exc:
        logger.error("smart_answer failed: %s", exc)


async def get_visible_plans(
    user_id: int, *, for_admin: bool, db: Database
) -> List[Dict[str, Any]]:
    plans = get_all_active()
    if for_admin:
        return [p for p in plans if not is_trial_plan(p)]
    user = await db.get_user(user_id)
    trial_used = bool(user.get("trial_used")) if user else False
    visible: List[Dict[str, Any]] = []
    for plan in plans:
        if is_trial_plan(plan):
            continue
        visible.append(plan)
    return visible


def generate_ref_code() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(8))


def get_ref_link(ref_code: str, system_type: int = 2, bot_username: Optional[str] = None) -> str:
    prefix = "ref2"
    username = bot_username or BOT_USERNAME
    if username:
        return f"https://t.me/{username}?start={prefix}_{ref_code}"
    return f"https://t.me/?start={prefix}_{ref_code}"
