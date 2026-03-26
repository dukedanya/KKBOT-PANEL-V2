SUPPORT_STATUS_LABELS = {
    "open": "Открыт",
    "in_progress": "В работе",
    "closed": "Закрыт",
    "archived": "Архив",
}

SUPPORT_RESTRICTION_REASON_LABELS = {
    "spam": "Спам",
    "flood": "Флуд",
    "abuse": "Оскорбления",
    "fraud": "Мошенничество",
}


def format_support_status(status: str, *, lowercase: bool = False) -> str:
    label = SUPPORT_STATUS_LABELS.get((status or "").lower(), status or "Неизвестно")
    return label.lower() if lowercase else label


def format_support_restriction_reason(reason: str) -> str:
    raw = (reason or "").strip()
    if not raw:
        return "-"
    if " by admin " in raw:
        base, _, admin_id = raw.partition(" by admin ")
        base_label = SUPPORT_RESTRICTION_REASON_LABELS.get(base.strip(), base.strip())
        admin_id = admin_id.strip()
        if admin_id:
            return f"{base_label}, установлено администратором {admin_id}"
        return base_label
    return SUPPORT_RESTRICTION_REASON_LABELS.get(raw, raw)
