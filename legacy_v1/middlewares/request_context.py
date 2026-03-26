import re
from typing import Any, Dict

from app.log_context import bind_log_context, reset_log_context

_SUPPORT_TICKET_RE = re.compile(r"^support:(?:reply|close|user_reply|user_close|view):(\d+)")
_PAYMENT_RE = re.compile(r"(?:^|:)(pay-[A-Za-z0-9_-]+)$")


def extract_log_context(event: Any) -> Dict[str, str]:
    context: Dict[str, str] = {}
    has_message_shape = hasattr(event, "chat") and hasattr(event, "message_id") and hasattr(event, "from_user")
    if has_message_shape:
        context["request_id"] = f"msg:{getattr(event.chat, 'id', '-') }:{getattr(event, 'message_id', '-')}"
        context["user_id"] = str(getattr(getattr(event, "from_user", None), "id", "-"))
        return context

    has_callback_shape = hasattr(event, "id") and hasattr(event, "data") and hasattr(event, "from_user")
    if has_callback_shape:
        context["request_id"] = f"cb:{getattr(event, 'id', '-')}"
        context["user_id"] = str(getattr(getattr(event, "from_user", None), "id", "-"))
        data = (getattr(event, "data", "") or "").strip()
        ticket_match = _SUPPORT_TICKET_RE.match(data)
        if ticket_match:
            context["ticket_id"] = ticket_match.group(1)
        payment_match = _PAYMENT_RE.search(data)
        if payment_match:
            context["payment_id"] = payment_match.group(1)
        return context

    return context


async def request_context_middleware(handler, event, data):
    token = bind_log_context(**extract_log_context(event))
    try:
        return await handler(event, data)
    finally:
        reset_log_context(token)
