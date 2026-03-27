from datetime import datetime
from html import escape
from typing import Any, Dict, List, Optional

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import Config
from db import Database
from services.yookassa import YooKassaAPI
from utils.payments import get_provider_payment_id


SUPPORT_RESTRICTION_PRESETS = {
    "spam": {"label": "Спам", "hours": 1, "reason": "spam"},
    "flood": {"label": "Флуд", "hours": 24, "reason": "flood"},
    "abuse": {"label": "Оскорбления", "hours": 168, "reason": "abuse"},
    "fraud": {"label": "Мошенничество", "hours": 720, "reason": "fraud"},
}


PROVIDER_LABELS = {
    "itpay": "ITPAY",
    "yookassa": "ЮKassa",
    "telegram_stars": "Telegram Stars",
}


def _format_dt(value: Any) -> str:
    if not value:
        return "-"
    if isinstance(value, datetime):
        return value.strftime("%d.%m.%Y %H:%M")
    text = str(value).strip()
    if not text:
        return "-"
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return text


def _format_history(history: List[Dict[str, Any]]) -> str:
    if not history:
        return "—"
    lines = []
    for item in history[-6:]:
        lines.append(
            f"• {item.get('created_at', '')}: <code>{item.get('from_status') or '-'}</code> → <code>{item.get('to_status') or '-'}</code>"
            f" [{item.get('source') or '-'}]"
        )
    return "\n".join(lines)


def _format_events(events: List[Dict[str, Any]]) -> str:
    if not events:
        return "—"
    lines = []
    for item in events[:6]:
        lines.append(
            f"• {item.get('created_at', '')}: <code>{item.get('event_type') or '-'}</code> [{item.get('source') or '-'}]"
        )
    return "\n".join(lines)


def _format_global_admin_actions(actions: List[Dict[str, Any]]) -> str:
    if not actions:
        return "—"
    lines = []
    for item in actions[:15]:
        lines.append(
            f"• <code>{item.get('payment_id')}</code> — <code>{item.get('action') or '-'}</code> [{item.get('provider') or '-'}] → <code>{item.get('result') or '-'}</code> (admin <code>{item.get('admin_user_id')}</code>)"
        )
    return "\n".join(lines)


def _format_admin_actions(actions: List[Dict[str, Any]]) -> str:
    if not actions:
        return "—"
    lines = []
    for item in actions[:6]:
        lines.append(
            f"• {item.get('created_at', '')}: <code>{item.get('action') or '-'}</code> by <code>{item.get('admin_user_id')}</code> → <code>{item.get('result') or '-'}</code>"
        )
    return "\n".join(lines)


def _format_bool_badge(value: object) -> str:
    return "да" if bool(value) else "нет"


def _trim_text(value: str, limit: int = 80) -> str:
    text = " ".join((value or "").split())
    if len(text) <= limit:
        return text or "—"
    return text[: limit - 1].rstrip() + "…"


def _admin_user_card_url(user_id: int) -> str:
    username = str(getattr(Config, "BOT_PUBLIC_USERNAME", "") or "").strip()
    if not username:
        return ""
    return f"https://t.me/{username}?start=admincard_{int(user_id)}"


def _admin_user_id_html(user_id: int, *, label: str | None = None) -> str:
    safe_label = escape(label or str(int(user_id)))
    url = _admin_user_card_url(int(user_id))
    if not url:
        return f"<code>{safe_label}</code>"
    return f"<a href=\"{url}\">{safe_label}</a>"


def _format_access_mode_label(mode: str) -> str:
    normalized = str(mode or "").strip().lower()
    if normalized == "grace":
        return "🐢 Grace"
    if normalized == "disabled":
        return "⛔ Disabled"
    if normalized == "normal":
        return "🟢 Normal"
    return normalized or "неизвестно"


def _humanize_timeline_title(item: Dict[str, Any]) -> str:
    kind = str(item.get("kind") or "")
    title = str(item.get("title") or "")
    if kind == "admin_action":
        return {
            "balance_adjustment": "Изменён баланс",
            "ban": "Пользователь заблокирован",
            "unban": "Блокировка снята",
            "reset_expiry_notifications": "Сброшены уведомления о продлении",
            "support_restriction_set": "Ограничена поддержка",
            "support_restriction_cleared": "Ограничение поддержки снято",
            "grant_tariff": "Выдан тариф",
            "extend_tariff": "Продлён тариф",
            "add_bonus_days": "Добавлены бонусные дни",
            "revoke_subscription": "Подписка отключена",
            "reset_trial": "Сброшен пробный период",
        }.get(title, f"Действие администратора: {title}")
    if kind == "support_ticket":
        ticket_no, _, status = title.partition(":")
        status_label = {
            "open": "открыт",
            "in_progress": "в работе",
            "closed": "закрыт",
            "archived": "в архиве",
        }.get(status, status or "обновлён")
        return f"Тикет {ticket_no.replace('ticket#', '#')} {status_label}"
    if kind == "payment":
        _, _, status = title.partition(":")
        return {
            "pending": "Создан платёж",
            "processing": "Платёж в обработке",
            "accepted": "Платёж подтверждён",
            "rejected": "Платёж отклонён",
            "refunded": "Платёж возвращён",
        }.get(status, f"Платёж: {status or 'обновлён'}")
    if kind == "withdraw":
        _, _, status = title.partition(":")
        return {
            "pending": "Создан запрос на вывод",
            "completed": "Вывод завершён",
            "rejected": "Вывод отклонён",
        }.get(status, f"Вывод: {status or 'обновлён'}")
    if kind == "balance_adjustment":
        return "Корректировка партнёрского баланса"
    return title or "Событие"


def _humanize_timeline_details(item: Dict[str, Any]) -> str:
    kind = str(item.get("kind") or "")
    details = str(item.get("details") or "").strip()
    if not details:
        return ""
    if kind == "admin_action" and details.startswith("admin="):
        admin_part, _, rest = details.partition(" ")
        admin_id = admin_part.replace("admin=", "").strip()
        rest = rest.strip()
        return f"Админ {admin_id}" + (f": {rest}" if rest else "")
    if kind == "payment":
        payment_id, _, amount_part = details.partition(" ")
        amount_text = amount_part.replace("RUB", "₽").strip()
        return f"{payment_id}" + (f" • {amount_text}" if amount_text else "")
    if kind == "withdraw":
        request_id, _, amount_part = details.partition(" ")
        amount_text = amount_part.replace("RUB", "₽").strip()
        return f"Запрос {request_id}" + (f" • {amount_text}" if amount_text else "")
    if kind == "balance_adjustment":
        return details.replace("RUB", "₽")
    return details


def _format_user_timeline(items: List[Dict[str, Any]]) -> str:
    if not items:
        return "—"
    lines = []
    for item in items[:20]:
        title = _humanize_timeline_title(item)
        details = _humanize_timeline_details(item)
        lines.append(
            f"• <code>{item.get('created_at') or '-'}</code> — <b>{escape(title)}</b>"
            + (f"\n  {escape(details)}" if details else "")
        )
    return "\n".join(lines)


def _operation_label(requested_status: str) -> str:
    if requested_status == "refund_requested":
        return "refund"
    if requested_status == "cancel_requested":
        return "cancel"
    return requested_status or "-"


def _format_pending_operations(items: List[Dict[str, Any]]) -> str:
    if not items:
        return "—"
    lines = []
    for item in items[:15]:
        lines.append(
            f"• <code>{item.get('payment_id')}</code> — {_operation_label(str(item.get('requested_status') or ''))} "
            f"[{PROVIDER_LABELS.get(item.get('provider') or '', item.get('provider') or '-')}] → "
            f"local <code>{item.get('status') or '-'}</code> / requested <code>{item.get('requested_at') or '-'}</code>"
        )
    return "\n".join(lines)


def _format_stale_processing(items: List[Dict[str, Any]]) -> str:
    if not items:
        return "—"
    lines = []
    for item in items[:10]:
        lines.append(
            f"• <code>{item.get('payment_id')}</code> [{PROVIDER_LABELS.get(item.get('provider') or '', item.get('provider') or '-')}] "
            f"— started <code>{item.get('processing_started_at') or '-'}</code> / user <code>{item.get('user_id')}</code>"
        )
    return "\n".join(lines)


def _format_attention_mismatches(items: List[Dict[str, Any]]) -> str:
    if not items:
        return "—"
    lines = []
    for item in items[:10]:
        lines.append(
            f"• <code>{item.get('payment_id')}</code> [{PROVIDER_LABELS.get(item.get('provider') or '', item.get('provider') or '-')}] "
            f"— event <code>{item.get('event_type') or '-'}</code> at <code>{item.get('event_created_at') or '-'}</code> / local <code>{item.get('status') or '-'}</code>"
        )
    return "\n".join(lines)


def _attention_keyboard(*, provider: str = "all", issue_type: str = "all", items: Optional[List[Dict[str, Any]]] = None) -> InlineKeyboardMarkup:
    items = items or []
    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(text="Все", callback_data=f"payattention:list:all:{issue_type}"),
            InlineKeyboardButton(text="ЮKassa", callback_data=f"payattention:list:yookassa:{issue_type}"),
            InlineKeyboardButton(text="Stars", callback_data=f"payattention:list:telegram_stars:{issue_type}"),
            InlineKeyboardButton(text="ITPAY", callback_data=f"payattention:list:itpay:{issue_type}"),
        ],
        [
            InlineKeyboardButton(text="Все типы", callback_data=f"payattention:list:{provider}:all"),
            InlineKeyboardButton(text="Processing", callback_data=f"payattention:list:{provider}:processing"),
            InlineKeyboardButton(text="Refund/Cancel", callback_data=f"payattention:list:{provider}:operations"),
            InlineKeyboardButton(text="Mismatch", callback_data=f"payattention:list:{provider}:mismatch"),
        ],
    ]
    seen = set()
    for item in items[:8]:
        payment_id = str(item.get("payment_id") or "")
        if not payment_id or payment_id in seen:
            continue
        seen.add(payment_id)
        rows.append([InlineKeyboardButton(text=f"🧾 {payment_id}", callback_data=f"paydiag_refresh:{payment_id}")])
    rows.append([InlineKeyboardButton(text="🛠️ Авто-резолв", callback_data=f"payattention:resolve:{provider}:{issue_type}")])
    rows.append([InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _pending_operations_keyboard(*, provider: str = "all", operation: str = "all", items: Optional[List[Dict[str, Any]]] = None) -> InlineKeyboardMarkup:
    items = items or []
    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(text="Все", callback_data=f"payops:list:all:{operation}"),
            InlineKeyboardButton(text="ЮKassa", callback_data=f"payops:list:yookassa:{operation}"),
            InlineKeyboardButton(text="Stars", callback_data=f"payops:list:telegram_stars:{operation}"),
        ],
        [
            InlineKeyboardButton(text="Все операции", callback_data=f"payops:list:{provider}:all"),
            InlineKeyboardButton(text="Refund", callback_data=f"payops:list:{provider}:refund"),
            InlineKeyboardButton(text="Cancel", callback_data=f"payops:list:{provider}:cancel"),
        ],
    ]
    for item in items[:8]:
        rows.append([InlineKeyboardButton(text=f"🧾 {item.get('payment_id')}", callback_data=f"paydiag_refresh:{item.get('payment_id')}")])
    rows.append([InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_pending_operations_text(db: Database, *, provider: str = "all", operation: str = "all") -> tuple[str, List[Dict[str, Any]]]:
    items = await db.get_pending_payment_operations(limit=15, provider=provider, operation=operation)
    provider_label = "Все" if provider == "all" else PROVIDER_LABELS.get(provider, provider)
    operation_label = {"all": "Все", "refund": "Refund", "cancel": "Cancel"}.get(operation, operation)
    text = (
        "⏳ <b>Pending refund/cancel операции</b>\n\n"
        f"🏦 Provider: <b>{provider_label}</b>\n"
        f"🧩 Операция: <b>{operation_label}</b>\n"
        f"📈 Найдено: <b>{len(items)}</b>\n\n"
        f"{_format_pending_operations(items)}"
    )
    return text, items


async def _render_attention_text(db: Database, *, provider: str = "all", issue_type: str = "all") -> tuple[str, List[Dict[str, Any]]]:
    stale_processing = []
    overdue_operations = []
    mismatches = []
    issue_type = issue_type or "all"
    if issue_type in {"all", "processing"}:
        stale_processing = await db.get_stale_processing_payments(
            minutes=Config.STALE_PROCESSING_TIMEOUT_MIN,
            limit=10,
            provider=provider,
        )
    if issue_type in {"all", "operations"}:
        overdue_operations = await db.get_overdue_payment_operations(
            minutes=Config.PAYMENT_ATTENTION_OPERATION_AGE_MIN,
            limit=10,
            provider=provider,
        )
    if issue_type in {"all", "mismatch"}:
        mismatches = await db.get_confirmed_payment_status_mismatches(
            hours=Config.PAYMENT_ATTENTION_EVENT_LOOKBACK_HOURS,
            limit=10,
            provider=provider,
        )

    provider_label = "Все" if provider == "all" else PROVIDER_LABELS.get(provider, provider)
    issue_label = {
        "all": "Все типы",
        "processing": "Stale processing",
        "operations": "Зависшие refund/cancel",
        "mismatch": "Webhook/status mismatch",
    }.get(issue_type, issue_type)

    combined_items: List[Dict[str, Any]] = []
    for row in stale_processing:
        combined_items.append({**row, "attention_type": "processing"})
    for row in overdue_operations:
        combined_items.append({**row, "attention_type": "operations"})
    for row in mismatches:
        combined_items.append({**row, "attention_type": "mismatch"})

    text = (
        "🚨 <b>Платежи: Требует внимания</b>\n\n"
        f"🏦 Provider: <b>{provider_label}</b>\n"
        f"🧩 Фильтр: <b>{issue_label}</b>\n"
        f"⚙️ Stale processing: <b>{len(stale_processing)}</b> (>{Config.STALE_PROCESSING_TIMEOUT_MIN} мин)\n"
        f"⏳ Refund/Cancel pending: <b>{len(overdue_operations)}</b> (>{Config.PAYMENT_ATTENTION_OPERATION_AGE_MIN} мин)\n"
        f"🔀 Confirmed mismatch: <b>{len(mismatches)}</b> (за {Config.PAYMENT_ATTENTION_EVENT_LOOKBACK_HOURS} ч)\n\n"
        f"<b>Stale processing</b>\n{_format_stale_processing(stale_processing)}\n\n"
        f"<b>Зависшие refund/cancel</b>\n{_format_pending_operations(overdue_operations)}\n\n"
        f"<b>Webhook/status mismatch</b>\n{_format_attention_mismatches(mismatches)}"
    )
    return text, combined_items


async def _build_provider_summary(db: Database) -> str:
    provider = Config.PAYMENT_PROVIDER
    counts = await db.get_payment_provider_counts()
    current_row = next((row for row in counts if (row.get("provider") or provider) == provider), None)
    if not current_row:
        current_row = {"total": 0, "pending": 0, "processing": 0, "accepted": 0, "rejected": 0}

    capabilities = []
    if provider == "yookassa":
        capabilities.extend(["refund", "cancel waiting_for_capture"])
    elif provider == "telegram_stars":
        capabilities.append("refundStarPayment")
    else:
        capabilities.append("manual reconcile")

    return (
        "💳 <b>Платёжная диагностика</b>\n\n"
        f"🏦 Активный провайдер: <b>{PROVIDER_LABELS.get(provider, provider)}</b>\n"
        f"🧩 Возможности: <code>{', '.join(capabilities)}</code>\n"
        f"📈 Всего платежей: <b>{int(current_row.get('total', 0) or 0)}</b>\n"
        f"🕒 Pending: <b>{int(current_row.get('pending', 0) or 0)}</b>\n"
        f"⚙️ Processing: <b>{int(current_row.get('processing', 0) or 0)}</b>\n"
        f"✅ Accepted: <b>{int(current_row.get('accepted', 0) or 0)}</b>\n"
        f"❌ Rejected: <b>{int(current_row.get('rejected', 0) or 0)}</b>\n\n"
        "Команды: <code>/paydiag PAYMENT_ID</code>, <code>/payactions</code>, <code>/payops</code>, <code>/payattention</code>, <code>/payresolve</code>."
    )


async def _build_payment_diagnostics(payment_id: str, db: Database, payment_gateway) -> Optional[Dict[str, Any]]:
    payment = await db.get_pending_payment(payment_id)
    if not payment:
        return None

    provider = payment.get("provider") or Config.PAYMENT_PROVIDER
    provider_payment_id = get_provider_payment_id(payment)
    remote_payment = None
    remote_status = "n/a"
    checkout_url = ""

    if provider_payment_id and hasattr(payment_gateway, "provider_name") and getattr(payment_gateway, "provider_name", "") == provider:
        remote_payment = None
        try:
            remote_payment = await payment_gateway.get_payment(provider_payment_id)
        except Exception:
            remote_payment = None
        if remote_payment:
            remote_status = str(payment_gateway.extract_status(remote_payment) or "unknown")
            checkout_url = payment_gateway.get_checkout_url(remote_payment) or ""

    history = await db.get_payment_status_history(payment_id)
    events = await db.get_recent_payment_events(payment_id)
    admin_actions = await db.get_payment_admin_actions(payment_id)

    plan_id = payment.get("plan_id") or "-"
    amount = payment.get("amount")
    text = (
        "🧾 <b>Диагностика платежа</b>\n\n"
        f"🆔 Payment ID: <code>{payment_id}</code>\n"
        f"🏦 Provider: <b>{PROVIDER_LABELS.get(provider, provider)}</b>\n"
        f"🧷 Provider payment ID: <code>{provider_payment_id or '-'}</code>\n"
        f"👤 User: <code>{payment.get('user_id')}</code>\n"
        f"📦 Plan: <b>{plan_id}</b>\n"
        f"💰 Amount: <b>{amount}</b>\n"
        f"📍 Local status: <code>{payment.get('status') or '-'}</code>\n"
        f"🌐 Remote status: <code>{remote_status}</code>\n"
        f"🕐 Created: <code>{payment.get('created_at') or '-'}</code>\n\n"
        f"<b>История статусов</b>\n{_format_history(history)}\n\n"
        f"<b>Последние dedup-события</b>\n{_format_events(events)}\n\n"
        f"<b>Admin actions</b>\n{_format_admin_actions(admin_actions)}"
    )
    return {
        "payment": payment,
        "remote_payment": remote_payment,
        "text": text,
        "checkout_url": checkout_url,
    }


async def _build_admin_dashboard_text(db: Database, panel=None, payment_gateway=None) -> str:
    provider = Config.PAYMENT_PROVIDER
    counts = await db.get_payment_provider_counts()
    current_row = next((row for row in counts if (row.get("provider") or provider) == provider), None) or {}
    pending_ops = await db.get_pending_payment_operations(limit=5, provider="all", operation="all")
    stale_processing = await db.get_stale_processing_payments(
        minutes=Config.STALE_PROCESSING_TIMEOUT_MIN,
        limit=5,
        provider="all",
    )
    mismatches = await db.get_confirmed_payment_status_mismatches(
        hours=Config.PAYMENT_ATTENTION_EVENT_LOOKBACK_HOURS,
        limit=5,
        provider="all",
    )
    recent_actions = await db.get_recent_payment_admin_actions(limit=5)
    finance = await db.get_total_revenue_summary()
    today_users = await db.get_daily_user_acquisition_report(days_ago=0)
    today_sales = await db.get_daily_subscription_sales_report(days_ago=0)
    return (
        "🧭 <b>Админ дашборд</b>\n\n"
        "<b>Платежи</b>\n"
        f"🏦 Активный провайдер: <b>{PROVIDER_LABELS.get(provider, provider)}</b>\n"
        f"📈 Всего: <b>{int(current_row.get('total', 0) or 0)}</b>\n"
        f"🕒 Pending: <b>{int(current_row.get('pending', 0) or 0)}</b>\n"
        f"⚙️ Processing: <b>{int(current_row.get('processing', 0) or 0)}</b>\n"
        f"✅ Accepted: <b>{int(current_row.get('accepted', 0) or 0)}</b>\n"
        f"❌ Rejected: <b>{int(current_row.get('rejected', 0) or 0)}</b>\n\n"
        "<b>Финансы</b>\n"
        f"💰 Заработано всего: <b>{finance.get('gross_revenue', 0.0):.2f} ₽</b>\n"
        f"💳 Покупки с баланса: <b>{finance.get('internal_balance_spent', 0.0):.2f} ₽</b> • <b>{int(finance.get('internal_balance_payments', 0) or 0)}</b>\n"
        f"🧾 Выдано админом на баланс: <b>{finance.get('admin_balance_issued', 0.0):.2f} ₽</b>\n"
        f"↩️ Возвраты: <b>{finance.get('refunded_revenue', 0.0):.2f} ₽</b>\n"
        f"📊 Чистая выручка: <b>{finance.get('net_revenue', 0.0):.2f} ₽</b>\n"
        f"🧮 Предположительная прибыль: <b>{finance.get('estimated_profit', 0.0):.2f} ₽</b>\n\n"
        "<b>Сегодня</b>\n"
        f"👥 Новые пользователи: <b>{today_users.get('new_users', 0)}</b>\n"
        f"🤝 Из рефералки: <b>{today_users.get('referred_new_users', 0)}</b>\n"
        f"🎁 Подключили trial: <b>{today_users.get('trial_started_new_users', 0)}</b>\n"
        f"🛒 Куплено подписок: <b>{today_sales.get('subscriptions_bought', 0)}</b>\n"
        f"💳 Куплено с баланса: <b>{today_sales.get('internal_balance_subscriptions', 0)}</b>\n"
        f"💵 Заработано сегодня: <b>{today_sales.get('gross_revenue', 0.0):.2f} ₽</b>\n\n"
        "<b>Операционное внимание</b>\n"
        f"⏳ Pending refund/cancel: <b>{len(pending_ops)}</b>\n"
        f"🚨 Stale processing: <b>{len(stale_processing)}</b>\n"
        f"🔀 Status mismatch: <b>{len(mismatches)}</b>\n\n"
        "<b>Последние admin actions</b>\n"
        f"{_format_global_admin_actions(recent_actions)}"
    )


def _admin_dashboard_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👥 Пользователи", callback_data="adminmenu:users"),
            InlineKeyboardButton(text="💳 Платежи", callback_data="adminmenu:payments"),
        ],
        [
            InlineKeyboardButton(text="📈 Аналитика", callback_data="adminmenu:analytics"),
            InlineKeyboardButton(text="📝 Контент и продажи", callback_data="adminmenu:content"),
        ],
        [InlineKeyboardButton(text="⚙️ Система и панель", callback_data="adminmenu:service")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="admin:exit")],
    ])


def _admin_payments_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🏦 По провайдерам", callback_data="paydiag_provider_summary"),
            InlineKeyboardButton(text="🔎 По ID платежа", callback_data="paydiag_prompt"),
        ],
        [
            InlineKeyboardButton(text="🧾 Журнал действий", callback_data="payactions_recent"),
            InlineKeyboardButton(text="⏳ Pending операции", callback_data="payops:list:all:all"),
        ],
        [
            InlineKeyboardButton(text="🚨 Требует внимания", callback_data="payattention:list:all:all"),
            InlineKeyboardButton(text="🛠️ Авто-резолв", callback_data="payattention:resolve:all:all"),
        ],
        [InlineKeyboardButton(text="🕒 Ожидающие платежи", callback_data="admin:pending_payments")],
        [InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")],
    ])


def _admin_users_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👤 Карточка пользователя", callback_data="admin:user_lookup"),
            InlineKeyboardButton(text="💸 Запросы на вывод", callback_data="admin:withdraw_requests"),
        ],
        [InlineKeyboardButton(text="🆘 Тикеты поддержки", callback_data="admin:support_tickets")],
        [
            InlineKeyboardButton(text="🆘 Ограничения поддержки", callback_data="admin:support_restrictions:list"),
            InlineKeyboardButton(text="🛡 Blacklist поддержки", callback_data="admin:support_blacklist"),
        ],
        [InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")],
    ])


def _admin_analytics_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📈 Ежедневный отчёт", callback_data="admindash:daily:0"),
            InlineKeyboardButton(text="📊 Периоды", callback_data="admindash:period:7"),
        ],
        [
            InlineKeyboardButton(text="📊 Бот и подписки", callback_data="admindash:bot"),
            InlineKeyboardButton(text="🩺 Health", callback_data="admindash:health"),
        ],
        [
            InlineKeyboardButton(text="🤝 Рефералка и выводы", callback_data="admindash:referrals"),
            InlineKeyboardButton(text="🏆 Топ-10 рефералов", callback_data="admindash:topref"),
        ],
        [InlineKeyboardButton(text="🚨 Инциденты за день", callback_data="admindash:incidents:0")],
        [InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")],
    ])


def _diagnostics_keyboard(payment: Dict[str, Any], remote_payment: Optional[Dict[str, Any]], checkout_url: str) -> InlineKeyboardMarkup:
    payment_id = payment["payment_id"]
    provider = payment.get("provider") or Config.PAYMENT_PROVIDER
    rows: List[List[InlineKeyboardButton]] = []
    if checkout_url:
        rows.append([InlineKeyboardButton(text="💳 Открыть оплату", url=checkout_url)])
    rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data=f"paydiag_refresh:{payment_id}")])

    if provider == "yookassa" and remote_payment:
        remote_status = YooKassaAPI.extract_status(remote_payment)
        if remote_status == "succeeded":
            rows.append([InlineKeyboardButton(text="↩️ Refund ЮKassa", callback_data=f"paydiag_refund:{payment_id}")])
        elif remote_status == "waiting_for_capture":
            rows.append([InlineKeyboardButton(text="🛑 Cancel ЮKassa", callback_data=f"paydiag_cancel:{payment_id}")])
    elif provider == "telegram_stars" and payment.get("provider_payment_id"):
        rows.append([InlineKeyboardButton(text="↩️ Refund Stars", callback_data=f"paydiag_refund_stars:{payment_id}")])

    rows.append([InlineKeyboardButton(text="⬅️ В админ меню", callback_data="back_to_admin")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _incident_report_keyboard(days_ago: int = 0) -> InlineKeyboardMarkup:
    rows = []
    if days_ago > 0:
        rows.append([InlineKeyboardButton(text="➡️ Ближе к сегодня", callback_data=f"admindash:incidents:{max(0, days_ago - 1)}")])
    rows.append([InlineKeyboardButton(text="⬅️ Более ранний день", callback_data=f"admindash:incidents:{days_ago + 1}")])
    rows.append([InlineKeyboardButton(text="⬅️ К аналитике", callback_data="adminmenu:analytics")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _provider_summary_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔎 Платёж по ID", callback_data="paydiag_prompt")],
        [InlineKeyboardButton(text="🧾 Последние действия", callback_data="payactions_recent")],
        [InlineKeyboardButton(text="⏳ Pending операции", callback_data="payops:list:all:all")],
        [InlineKeyboardButton(text="🚨 Требует внимания", callback_data="payattention:list:all:all")],
        [InlineKeyboardButton(text="🛠️ Авто-резолв attention", callback_data="payattention:resolve:all:all")],
        [InlineKeyboardButton(text="⬅️ В админ меню", callback_data="back_to_admin")],
    ])
