import contextvars
from typing import Dict

_DEFAULT_CONTEXT: Dict[str, str] = {
    "request_id": "-",
    "ticket_id": "-",
    "payment_id": "-",
    "user_id": "-",
}

_log_context: contextvars.ContextVar[Dict[str, str]] = contextvars.ContextVar(
    "log_context",
    default=_DEFAULT_CONTEXT,
)


def get_log_context() -> Dict[str, str]:
    current = _log_context.get()
    return {
        "request_id": current.get("request_id", "-"),
        "ticket_id": current.get("ticket_id", "-"),
        "payment_id": current.get("payment_id", "-"),
        "user_id": current.get("user_id", "-"),
    }


def bind_log_context(**kwargs):
    current = get_log_context()
    merged = {**current, **{k: str(v) for k, v in kwargs.items() if v is not None}}
    return _log_context.set(merged)


def reset_log_context(token) -> None:
    _log_context.reset(token)
