import time
from typing import Dict

from aiogram.types import Message

from config import Config

_last_start_by_user: Dict[int, float] = {}


def _is_start_command(message: Message) -> bool:
    text = (getattr(message, "text", "") or "").strip()
    if not text.startswith("/start"):
        return False
    command = text.split(maxsplit=1)[0].split("@", 1)[0]
    return command == "/start"


async def start_dedup_middleware(handler, event, data):
    if not isinstance(event, Message):
        return await handler(event, data)

    if not _is_start_command(event):
        return await handler(event, data)

    user = getattr(event, "from_user", None)
    if not user:
        return await handler(event, data)

    user_id = int(user.id)
    now = time.monotonic()
    window = max(0.0, float(getattr(Config, "START_COMMAND_DEDUP_WINDOW_SEC", 2.0) or 0.0))
    last_seen = _last_start_by_user.get(user_id)
    _last_start_by_user[user_id] = now
    if last_seen is not None and now - last_seen <= window:
        return None
    return await handler(event, data)
