import time
from typing import Dict, Tuple

from aiogram.types import CallbackQuery

from config import Config

_last_callbacks: Dict[Tuple[int, str], float] = {}


async def callback_dedup_middleware(handler, event, data):
    if not isinstance(event, CallbackQuery):
        return await handler(event, data)

    user = getattr(event, "from_user", None)
    if not user:
        return await handler(event, data)

    callback_data = (getattr(event, "data", "") or "").strip()
    if not callback_data:
        return await handler(event, data)

    key = (int(user.id), callback_data)
    now = time.monotonic()
    window = max(0.0, float(getattr(Config, "CALLBACK_DEDUP_WINDOW_SEC", 1.5) or 0.0))
    last_seen = _last_callbacks.get(key)
    _last_callbacks[key] = now
    if last_seen is not None and now - last_seen < window:
        try:
            await event.answer("Запрос уже обрабатывается.", show_alert=False)
        except Exception:
            pass
        return None
    return await handler(event, data)
