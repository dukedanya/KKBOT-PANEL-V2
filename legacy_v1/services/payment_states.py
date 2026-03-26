from __future__ import annotations

TERMINAL_STATUSES = {"accepted", "rejected", "refunded", "cancelled"}
TRANSITIONS = {
    "pending": {"processing", "rejected", "cancelled"},
    "processing": {"pending", "accepted", "rejected", "cancelled"},
    "accepted": {"refunded"},
    "rejected": set(),
    "cancelled": set(),
    "refunded": set(),
}


def can_transition(current_status: str | None, next_status: str) -> bool:
    current = (current_status or "").strip().lower()
    nxt = (next_status or "").strip().lower()
    if not current:
        return True
    if current == nxt:
        return True
    return nxt in TRANSITIONS.get(current, set())
