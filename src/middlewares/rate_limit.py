import time
from typing import Dict, Tuple

from aiogram.types import CallbackQuery, Message

from config import Config

_last_events: Dict[Tuple[str, int], float] = {}


def _is_command_message(event: Message) -> bool:
    text = (getattr(event, "text", "") or "").strip()
    return text.startswith("/")


async def rate_limit_middleware(handler, event, data):
    now = time.monotonic()

    if isinstance(event, Message):
        user = getattr(event, "from_user", None)
        if not user or not _is_command_message(event):
            return await handler(event, data)
        user_id = int(user.id)
        key = ("cmd", user_id)
        window = max(0.0, float(getattr(Config, "COMMAND_RATE_LIMIT_SEC", 0.8) or 0.0))
        last = _last_events.get(key)
        if last is not None and now - last < window:
            return None
        _last_events[key] = now
        return await handler(event, data)

    if isinstance(event, CallbackQuery):
        user = getattr(event, "from_user", None)
        if not user:
            return await handler(event, data)
        user_id = int(user.id)
        key = ("cb", user_id)
        window = max(0.0, float(getattr(Config, "CALLBACK_RATE_LIMIT_SEC", 0.35) or 0.0))
        last = _last_events.get(key)
        if last is not None and now - last < window:
            try:
                await event.answer("Слишком часто, подождите секунду.", show_alert=False)
            except Exception:
                pass
            return None
        _last_events[key] = now
        return await handler(event, data)

    return await handler(event, data)
