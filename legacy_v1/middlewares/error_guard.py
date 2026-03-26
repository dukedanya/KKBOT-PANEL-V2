import logging
import time
import traceback
from typing import Dict

from aiogram.types import CallbackQuery, Message

from config import Config

logger = logging.getLogger(__name__)

_last_error_alert: Dict[str, float] = {}


def _event_user_id(event) -> int:
    user = getattr(event, "from_user", None)
    return int(getattr(user, "id", 0) or 0)


def _event_fingerprint(event) -> str:
    event_type = type(event).__name__
    if isinstance(event, CallbackQuery):
        return f"{event_type}:{_event_user_id(event)}:{(event.data or '')[:80]}"
    if isinstance(event, Message):
        return f"{event_type}:{_event_user_id(event)}:{(event.text or event.caption or '')[:80]}"
    return f"{event_type}:{_event_user_id(event)}"


async def _notify_admins_about_error(*, event, data, exc: Exception) -> None:
    bot = data.get("bot") or getattr(getattr(event, "message", None), "bot", None) or getattr(event, "bot", None)
    if bot is None:
        return

    fingerprint = _event_fingerprint(event) + ":" + type(exc).__name__
    now = time.monotonic()
    cooldown = max(0, int(getattr(Config, "ERROR_ALERT_COOLDOWN_SEC", 120) or 0))
    last = _last_error_alert.get(fingerprint)
    if last is not None and now - last < cooldown:
        return
    _last_error_alert[fingerprint] = now

    tb_short = "".join(traceback.format_exception_only(type(exc), exc)).strip()
    user_id = _event_user_id(event)
    event_data = ""
    if isinstance(event, CallbackQuery):
        event_data = f"\ncallback: <code>{(event.data or '')[:120]}</code>"
    elif isinstance(event, Message):
        event_data = f"\nmessage: <code>{(event.text or event.caption or '')[:120]}</code>"

    text = (
        "🚨 <b>Unhandled exception</b>\n"
        f"user: <code>{user_id or '-'}</code>\n"
        f"type: <code>{type(exc).__name__}</code>\n"
        f"error: <code>{tb_short[:700]}</code>"
        f"{event_data}"
    )
    for admin_id in Config.ADMIN_USER_IDS:
        try:
            await bot.send_message(admin_id, text, parse_mode="HTML")
        except Exception:
            pass


async def error_guard_middleware(handler, event, data):
    try:
        return await handler(event, data)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unhandled event exception: %s", exc)
        await _notify_admins_about_error(event=event, data=data, exc=exc)
        if isinstance(event, CallbackQuery):
            try:
                await event.answer("⚠️ Временная ошибка. Попробуйте снова через пару секунд.", show_alert=True)
            except Exception:
                pass
            return None
        if isinstance(event, Message):
            try:
                await event.answer("⚠️ Внутренняя ошибка. Мы уже получили сигнал и разбираемся.")
            except Exception:
                pass
            return None
        return None
