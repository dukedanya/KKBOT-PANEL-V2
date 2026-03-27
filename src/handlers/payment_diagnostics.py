import logging
from datetime import datetime
from html import escape
from typing import Any, Dict, List, Optional

from aiogram import F, Router, Bot
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config import Config
from db import Database
from keyboards import main_menu_keyboard
from kkbot.services.subscriptions import panel_base_email
from kkbot.services.subscriptions import create_subscription
from services.yookassa import YooKassaAPI
from kkbot.services.subscriptions import revoke_subscription
from services.payment_attention_resolver import auto_resolve_payment_attention
from services.health import collect_health_snapshot
from services.traffic_state import format_grace_until, get_total_traffic_snapshot_for_user
from tariffs import get_all_active, get_by_id
from utils.helpers import replace_message, notify_admins, notify_user
from utils.payments import get_provider_payment_id
from utils.support import format_support_restriction_reason, format_support_status
from utils.telegram_ui import smart_edit_message

logger = logging.getLogger(__name__)
router = Router()


class PaymentDiagnosticsFSM(StatesGroup):
    waiting_payment_id = State()
    waiting_user_id = State()
    waiting_support_blacklist = State()
    waiting_user_balance_adjustment = State()
    waiting_inbound_count = State()
    waiting_inbound_ids = State()


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


def is_admin(user_id: int) -> bool:
    return user_id in Config.ADMIN_USER_IDS


def _write_env_variable(key: str, value: str) -> None:
    import os

    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    lines = []
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as file:
            lines = file.read().splitlines()
    replaced = False
    for idx, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[idx] = f"{key}={value}"
            replaced = True
            break
    if not replaced:
        lines.append(f"{key}={value}")
    with open(env_path, "w", encoding="utf-8") as file:
        file.write("\n".join(lines) + "\n")


def _parse_panel_inbound_ids(raw: str) -> List[int]:
    result: List[int] = []
    for item in str(raw or "").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            value = int(item)
        except ValueError:
            continue
        if value not in result:
            result.append(value)
    return result


def _effective_panel_inbound_ids() -> List[int]:
    ids = _parse_panel_inbound_ids(Config.PANEL_TARGET_INBOUND_IDS)
    count = max(0, int(getattr(Config, "PANEL_TARGET_INBOUND_COUNT", 0) or 0))
    if count > 0:
        return ids[:count]
    return ids


def _panel_inbounds_settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔢 Изменить количество", callback_data="admin:panel_inbounds:count")],
        [InlineKeyboardButton(text="🆔 Изменить ID инбаундов", callback_data="admin:panel_inbounds:ids")],
        [InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")],
    ])


def _panel_inbounds_settings_text() -> str:
    configured = _parse_panel_inbound_ids(Config.PANEL_TARGET_INBOUND_IDS)
    effective = _effective_panel_inbound_ids()
    count = max(0, int(getattr(Config, "PANEL_TARGET_INBOUND_COUNT", 0) or 0))
    return (
        "🛰 <b>Регистрация в инбаундах</b>\n\n"
        f"Список ID: <code>{', '.join(str(item) for item in configured) or 'не задан'}</code>\n"
        f"Активное количество: <b>{count if count > 0 else len(configured)}</b>"
        + (" <i>(режим: все из списка)</i>" if count == 0 else "")
        + "\n"
        f"Сейчас используются: <code>{', '.join(str(item) for item in effective) or 'не заданы'}</code>\n\n"
        "Бот может работать с любым количеством инбаундов.\n"
        "Если указать количество <code>0</code>, будут использованы все ID из списка."
    )


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


def _format_access_mode_label(mode: str) -> str:
    normalized = str(mode or "").strip().lower()
    if normalized == "grace":
        return "🐢 Grace"
    if normalized == "disabled":
        return "⛔ Disabled"
    if normalized == "normal":
        return "🟢 Normal"
    return normalized or "неизвестно"


def _format_total_traffic_block(snapshot: Optional[Any]) -> str:
    if not snapshot:
        return (
            "<b>Общий traffic-state</b>\n"
            "📡 Статус: <b>нет данных</b>\n"
            "🧮 Общий трафик: <b>—</b>\n"
            "📦 Квота: <b>—</b>\n"
            "📉 Остаток: <b>—</b>\n"
            "🚦 Режим: <b>—</b>\n"
            "⏳ Grace до: <code>-</code>"
        )

    freshness = "свежий" if getattr(snapshot, "fresh", False) else "устарел"
    if getattr(snapshot, "quota_bytes", 0) > 0:
        quota_line = f"<b>{snapshot.quota_gb:.1f} ГБ</b>"
        remaining_line = f"<b>{snapshot.remaining_gb:.1f} ГБ</b>"
    else:
        quota_line = "<b>не задана</b>"
        remaining_line = "<b>—</b>"

    return (
        "<b>Общий traffic-state</b>\n"
        f"📡 Статус: <b>{freshness}</b>\n"
        f"🧮 Общий трафик: <b>{snapshot.total_gb:.1f} ГБ</b>\n"
        f"📦 Квота: {quota_line}\n"
        f"📉 Остаток: {remaining_line}\n"
        f"🚦 Режим: <b>{_format_access_mode_label(getattr(snapshot, 'mode', ''))}</b>\n"
        f"⚠️ Over limit: <b>{_format_bool_badge(getattr(snapshot, 'over_limit', False))}</b>\n"
        f"⌛ Expired: <b>{_format_bool_badge(getattr(snapshot, 'expired', False))}</b>\n"
        f"⏳ Grace до: <code>{format_grace_until(getattr(snapshot, 'grace_until', '')) or '-'}</code>"
    )


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
    if kind == "admin_action":
        if details.startswith("admin="):
            admin_part, _, rest = details.partition(" ")
            admin_id = admin_part.replace("admin=", "").strip()
            rest = rest.strip()
            if rest:
                return f"Админ {admin_id}: {rest}"
            return f"Админ {admin_id}"
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


def _operation_label(requested_status: str) -> str:
    return "refund" if requested_status == "refund_requested" else "cancel" if requested_status == "cancel_requested" else requested_status or "-"


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
        payment_id = str(item.get('payment_id') or '')
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
        capabilities.append("refund")
        capabilities.append("cancel waiting_for_capture")
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
        try:
            remote_payment = await payment_gateway.get_payment(provider_payment_id)
        except Exception as exc:
            logger.warning("payment diagnostics get_payment failed payment=%s: %s", payment_id, exc)
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
    users_7d = await db.get_period_user_acquisition_report(days=7)
    sales_7d = await db.get_period_subscription_sales_report(days=7)
    users_30d = await db.get_period_user_acquisition_report(days=30)
    sales_30d = await db.get_period_subscription_sales_report(days=30)
    return (
        "🧭 <b>Админ дашборд</b>\n\n"
        "<b>Навигация</b>\n"
        "👥 Пользователи: карточки, поддержка, ограничения, выводы\n"
        "💳 Платежи: статусы, pending, диагностика, действия\n"
        "📈 Аналитика: отчёты, health, рефералка, инциденты\n"
        "📝 Контент: тарифы, промокоды, шаблоны, рассылки\n"
        "⚙️ Система: панель, safe mode, Stars, служебные настройки\n\n"
        "<b>Платежи</b>\n"
        f"🏦 Активный провайдер: <b>{PROVIDER_LABELS.get(provider, provider)}</b>\n"
        f"📈 Всего: <b>{int(current_row.get('total', 0) or 0)}</b>\n"
        f"🕒 Pending: <b>{int(current_row.get('pending', 0) or 0)}</b>\n"
        f"⚙️ Processing: <b>{int(current_row.get('processing', 0) or 0)}</b>\n"
        f"✅ Accepted: <b>{int(current_row.get('accepted', 0) or 0)}</b>\n"
        f"❌ Rejected: <b>{int(current_row.get('rejected', 0) or 0)}</b>\n\n"
        "<b>Финансы</b>\n"
        f"💰 Заработано всего: <b>{finance.get('gross_revenue', 0.0):.2f} ₽</b>\n"
        f"↩️ Возвраты: <b>{finance.get('refunded_revenue', 0.0):.2f} ₽</b>\n"
        f"📊 Чистая выручка: <b>{finance.get('net_revenue', 0.0):.2f} ₽</b>\n"
        f"🧮 Предположительная прибыль: <b>{finance.get('estimated_profit', 0.0):.2f} ₽</b>\n\n"
        "<b>Сегодня</b>\n"
        f"👥 Новые пользователи: <b>{today_users.get('new_users', 0)}</b>\n"
        f"🤝 Из рефералки: <b>{today_users.get('referred_new_users', 0)}</b>\n"
        f"🎁 Подключили trial: <b>{today_users.get('trial_started_new_users', 0)}</b>\n"
        f"🛒 Куплено подписок: <b>{today_sales.get('subscriptions_bought', 0)}</b>\n"
        f"💵 Заработано сегодня: <b>{today_sales.get('gross_revenue', 0.0):.2f} ₽</b>\n\n"
        "<b>Операционное внимание</b>\n"
        f"⏳ Pending refund/cancel: <b>{len(pending_ops)}</b>\n"
        f"🚨 Stale processing: <b>{len(stale_processing)}</b>\n"
        f"🔀 Status mismatch: <b>{len(mismatches)}</b>\n\n"
        "<b>Последние admin actions</b>\n"
        f"{_format_global_admin_actions(recent_actions)}"
    )


async def _build_bot_stats_detail(db: Database) -> str:
    total_users = await db.get_total_users()
    subscribed = len(await db.get_subscribed_user_ids())
    banned = await db.get_banned_users_count()
    return (
        "📊 <b>Бот и подписки</b>\n\n"
        f"👥 Всего пользователей: <b>{total_users}</b>\n"
        f"📦 Активных подписок: <b>{subscribed}</b>\n"
        f"⛔ Заблокировано: <b>{banned}</b>"
    )


async def _build_referral_detail(db: Database) -> str:
    pending_withdraws = await db.get_pending_withdraw_requests()
    top_referrers = await db.get_top_referrers_extended(limit=5)
    total_pending_amount = sum(float(item.get("amount") or 0) for item in pending_withdraws)
    top_text = "\n".join(
        f"• <code>{row.get('ref_by')}</code> — <b>{int(row.get('paid_count', 0) or 0)}</b> оплат — <b>{float(row.get('earned_rub') or 0):.2f} ₽</b>"
        for row in top_referrers
    ) if top_referrers else "—"
    return (
        "🤝 <b>Рефералка и выводы</b>\n\n"
        f"💸 Pending запросы на вывод: <b>{len(pending_withdraws)}</b>\n"
        f"💰 Сумма pending выводов: <b>{total_pending_amount:.2f} ₽</b>\n\n"
        f"<b>Топ рефереры</b>\n{top_text}"
    )


async def _build_top_referrers_detail(db: Database, *, limit: int = 10) -> str:
    top_referrers = await db.get_top_referrers_extended(limit=limit)
    top_text = "\n".join(
        f"{index}. <code>{row.get('ref_by')}</code> — <b>{int(row.get('paid_count', 0) or 0)}</b> оплат — <b>{float(row.get('earned_rub') or 0):.2f} ₽</b>"
        for index, row in enumerate(top_referrers, start=1)
    ) if top_referrers else "—"
    return (
        f"🏆 <b>Топ-{limit} рефералов</b>\n\n"
        f"{top_text}"
    )



async def _build_daily_report_detail(db: Database, *, days_ago: int = 0) -> str:
    users = await db.get_daily_user_acquisition_report(days_ago=days_ago)
    sales = await db.get_daily_subscription_sales_report(days_ago=days_ago)
    day_label = "Сегодня" if days_ago == 0 else "Вчера" if days_ago == 1 else users.get("report_date") or f"-{days_ago}d"
    return (
        f"📈 <b>Ежедневный отчёт — {day_label}</b>\n\n"
        f"📅 Дата: <code>{users.get('report_date') or sales.get('report_date') or '-'}</code>\n"
        f"👥 Новые пользователи: <b>{users.get('new_users', 0)}</b>\n"
        f"🤝 Пришли по реферальной системе: <b>{users.get('referred_new_users', 0)}</b>\n"
        f"🎁 Подключили пробный период: <b>{users.get('trial_started_new_users', 0)}</b>\n\n"
        f"🛒 Приобретено подписок: <b>{sales.get('subscriptions_bought', 0)}</b>\n"
        f"💰 Заработано: <b>{sales.get('gross_revenue', 0.0):.2f} ₽</b>\n"
        f"↩️ Возвраты: <b>{sales.get('refunded_revenue', 0.0):.2f} ₽</b>\n"
        f"📊 Чистая выручка: <b>{sales.get('net_revenue', 0.0):.2f} ₽</b>\n"
        f"🤝 Реферальные начисления: <b>{sales.get('referral_cost', 0.0):.2f} ₽</b>\n"
        f"🧮 Предположительная прибыль: <b>{sales.get('estimated_profit', 0.0):.2f} ₽</b>"
    )

def _format_pct(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0.0%"
    return f"{(float(numerator) / float(denominator) * 100):.1f}%"


async def _build_period_report_detail(db: Database, *, days: int = 7, end_days_ago: int = 0) -> str:
    users = await db.get_period_user_acquisition_report(days=days, end_days_ago=end_days_ago)
    sales = await db.get_period_subscription_sales_report(days=days, end_days_ago=end_days_ago)
    new_users = int(users.get("new_users", 0) or 0)
    referred_new_users = int(users.get("referred_new_users", 0) or 0)
    trial_started = int(users.get("trial_started_new_users", 0) or 0)
    subscriptions_bought = int(sales.get("subscriptions_bought", 0) or 0)
    title = f"{days} дней" if end_days_ago == 0 else f"{days} дней (окно до -{end_days_ago}д)"
    return f"""📊 <b>Отчёт по периоду — {title}</b>

📅 Период: <code>{users.get('start_date') or sales.get('start_date') or '-'}</code> → <code>{users.get('end_date') or sales.get('end_date') or '-'}</code>

<b>Воронка</b>
👥 Новые пользователи: <b>{new_users}</b>
🤝 Пришли по реферальной системе: <b>{referred_new_users}</b> ({_format_pct(referred_new_users, new_users)})
🎁 Подключили trial: <b>{trial_started}</b> ({_format_pct(trial_started, new_users)})
🛒 Купили подписку: <b>{subscriptions_bought}</b> ({_format_pct(subscriptions_bought, new_users)})

<b>Финансы</b>
💰 Заработано: <b>{sales.get('gross_revenue', 0.0):.2f} ₽</b>
↩️ Возвраты: <b>{sales.get('refunded_revenue', 0.0):.2f} ₽</b>
📊 Чистая выручка: <b>{sales.get('net_revenue', 0.0):.2f} ₽</b>
🤝 Реферальные начисления: <b>{sales.get('referral_cost', 0.0):.2f} ₽</b>
🧮 Предположительная прибыль: <b>{sales.get('estimated_profit', 0.0):.2f} ₽</b>

<b>Конверсия</b>
➡️ Новые → trial: <b>{_format_pct(trial_started, new_users)}</b>
➡️ Новые → покупка: <b>{_format_pct(subscriptions_bought, new_users)}</b>
➡️ Trial → покупка: <b>{_format_pct(subscriptions_bought, trial_started)}</b>"""

def _daily_report_keyboard(days_ago: int = 0) -> InlineKeyboardMarkup:
    prev_days = max(0, int(days_ago) + 1)
    next_days = max(0, int(days_ago) - 1)
    rows = [
        [
            InlineKeyboardButton(text="Сегодня", callback_data="admindash:daily:0"),
            InlineKeyboardButton(text="Вчера", callback_data="admindash:daily:1"),
        ],
        [
            InlineKeyboardButton(text="7 дней", callback_data="admindash:period:7:0"),
            InlineKeyboardButton(text="30 дней", callback_data="admindash:period:30:0"),
        ],
    ]
    if days_ago >= 1:
        rows.append([InlineKeyboardButton(text="⬅️ Более ранний день", callback_data=f"admindash:daily:{prev_days}")])
        rows.append([InlineKeyboardButton(text="➡️ Ближе к сегодня", callback_data=f"admindash:daily:{next_days}")])
    rows.append([InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")])
    return InlineKeyboardMarkup(inline_keyboard=rows)




def _period_report_keyboard(days: int = 7, end_days_ago: int = 0) -> InlineKeyboardMarkup:
    prev_window_end = max(0, int(end_days_ago) + int(days))
    next_window_end = max(0, int(end_days_ago) - int(days))
    rows = [
        [
            InlineKeyboardButton(text="7 дней", callback_data=f"admindash:period:7:{end_days_ago}"),
            InlineKeyboardButton(text="30 дней", callback_data=f"admindash:period:30:{end_days_ago}"),
        ],
        [
            InlineKeyboardButton(text="Сегодня", callback_data="admindash:daily:0"),
            InlineKeyboardButton(text="Вчера", callback_data="admindash:daily:1"),
        ],
    ]
    rows.append([InlineKeyboardButton(text="⬅️ Более ранний период", callback_data=f"admindash:period:{days}:{prev_window_end}")])
    if end_days_ago > 0:
        rows.append([InlineKeyboardButton(text="➡️ Ближе к сегодня", callback_data=f"admindash:period:{days}:{next_window_end}")])
    rows.append([InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _build_health_detail(db: Database, panel, payment_gateway) -> str:
    snapshot = await collect_health_snapshot(db, panel, payment_gateway)
    status = "OK" if snapshot.get("ok") else "WARN"
    return (
        "🩺 <b>Состояние системы</b>\n\n"
        f"Статус: <b>{status}</b>\n"
        f"БД: <b>{'OK' if snapshot.get('database') else 'FAIL'}</b>\n"
        f"Panel: <b>{'OK' if snapshot.get('panel') else 'FAIL'}</b>\n"
        f"Провайдер: <b>{'OK' if snapshot.get('payment_provider') else 'FAIL'}</b>\n"
        f"Schema version: <code>{snapshot.get('schema_version', 0)}</code>\n"
        f"Processing: <b>{snapshot.get('processing_count', 0)}</b>\n"
        f"Старые pending: <b>{snapshot.get('old_pending_count', 0)}</b>\n"
        f"Ошибки за 24ч: <b>{snapshot.get('payment_error_count', 0)}</b>"
    )


async def _build_incident_report_detail(db: Database, panel, payment_gateway, *, days_ago: int = 0) -> str:
    incidents = await db.get_daily_incident_report(days_ago=days_ago)
    snapshot = await collect_health_snapshot(db, panel, payment_gateway)
    schema_issues = await db.get_schema_drift_issues() if hasattr(db, "get_schema_drift_issues") else []
    safe_mode_enabled = str(await db.get_setting("system:safe_mode", "0") or "0") == "1"
    safe_mode_reason = str(await db.get_setting("system:safe_mode_reason", "") or "")
    return (
        "🚨 <b>Отчёт по инцидентам</b>\n\n"
        f"📅 Дата: <code>{incidents.get('report_date', '-')}</code>\n"
        f"⚠️ Ошибки платежей: <b>{incidents.get('payment_errors', 0)}</b>\n"
        f"🛡 Срабатывания blacklist поддержки: <b>{incidents.get('support_blacklist_hits', 0)}</b>\n"
        f"🕒 Stale processing: <b>{incidents.get('stale_processing', 0)}</b>\n"
        f"⏳ Старые pending: <b>{incidents.get('old_pending', 0)}</b>\n"
        f"🧯 Safe mode: <b>{'включён' if safe_mode_enabled else 'выключен'}</b>\n"
        f"📝 Причина: <code>{safe_mode_reason or '-'}</code>\n"
        f"🧬 Проблемы схемы: <b>{len(schema_issues)}</b>\n"
        f"🛠 Processing сейчас: <b>{snapshot.get('processing_count', 0)}</b>\n"
        f"📛 Ошибки за 24ч: <b>{snapshot.get('payment_error_count', 0)}</b>"
    )


async def _build_user_card_text(db: Database, user_id: int, *, display_name_override: str | None = None) -> str:
    payload = await db.get_user_card(user_id) if hasattr(db, "get_user_card") else {}
    if not payload:
        return f"👤 <b>Карточка пользователя | Какой-то VPN бот</b>\n\nПользователь <code>{user_id}</code> не найден."
    user = payload.get("user") or {}
    referral = payload.get("referral_summary") or {}
    partner = payload.get("partner_settings") or {}
    support_tickets = payload.get("support_tickets") or []
    support_restriction = payload.get("support_restriction") or {}
    payments = payload.get("payments") or []
    withdraws = payload.get("withdraws") or []
    adjustments = payload.get("adjustments") or []
    total_snapshot = await get_total_traffic_snapshot_for_user(user_id, db)
    support_text = "\n".join(
        f"• <code>#{item.get('id')}</code> — <b>{format_support_status(str(item.get('status') or ''), lowercase=True)}</b> — {item.get('updated_at') or '-'}"
        for item in support_tickets[:4]
    ) or "—"
    payments_text = "\n".join(
        f"• <code>{item.get('payment_id')}</code> — <code>{item.get('status') or '-'}</code> — <b>{float(item.get('amount') or 0):.2f} ₽</b>"
        for item in payments[:4]
    ) or "—"
    withdraws_text = "\n".join(
        f"• <code>#{item.get('id')}</code> — <code>{item.get('status') or '-'}</code> — <b>{float(item.get('amount') or 0):.2f} ₽</b>"
        for item in withdraws[:4]
    ) or "—"
    adjustments_text = "\n".join(
        f"• <b>{float(item.get('amount') or 0):.2f} ₽</b> — {_trim_text(str(item.get('reason') or 'без причины'), 45)}"
        for item in adjustments[:4]
    ) or "—"
    status_label = "активна" if bool(user.get("has_subscription")) else "не активна"
    if user.get("frozen_until"):
        status_label = "заморожена"
    plan_label = str(user.get("plan_text") or "—")
    ref_by = int(user.get("ref_by") or 0)
    source_label = f"ref {ref_by}" if ref_by > 0 else "прямой вход"
    last_payment_line = "—"
    if payments:
        latest_payment = payments[0]
        payment_amount = float(latest_payment.get("amount") or 0)
        payment_status = str(latest_payment.get("status") or "-")
        last_payment_line = (
            f"<code>{latest_payment.get('payment_id') or '-'}</code> — "
            f"<b>{payment_amount:.2f} ₽</b> — <code>{payment_status}</code>"
        )
    username = str(user.get("username") or "").strip()
    first_name = str(user.get("first_name") or "").strip()
    display_parts: List[str] = []
    if username:
        display_parts.append(f"@{username}")
    if first_name:
        display_parts.append(first_name)
    display_name = display_name_override or (" | ".join(display_parts) if display_parts else "—")
    return (
        "👤 <b>Карточка пользователя | Какой-то VPN бот</b>\n\n"
        f"ID: <code>{user_id}</code> • <a href=\"tg://user?id={user_id}\">Открыть чат</a>\n"
        f"Имя: <code>{escape(display_name)}</code>\n"
        f"Дата входа: <code>{user.get('join_date') or '-'}</code>\n"
        f"Статус: <b>{status_label}</b>\n"
        f"Тариф: <b>{escape(plan_label)}</b>\n"
        f"Источник: <code>{source_label}</code>\n"
        f"Последний платёж: {last_payment_line}\n"
        f"Подписка активна: <b>{_format_bool_badge(user.get('has_subscription'))}</b>\n"
        f"VPN URL есть: <b>{_format_bool_badge(user.get('vpn_url'))}</b>\n"
        f"Истекает: <code>{user.get('expiry') or '-'}</code>\n"
        f"Заморожено до: <code>{user.get('frozen_until') or '-'}</code>\n"
        f"Баланс: <b>{float(user.get('balance') or 0):.2f} ₽</b>\n"
        f"Пробный период использован: <b>{_format_bool_badge(user.get('trial_used'))}</b>\n"
        f"Пришёл от ref: <code>{user.get('ref_by') or 0}</code>\n"
        f"Реф. код: <code>{user.get('ref_code') or '-'}</code>\n\n"
        "<b>Ограничения</b>\n"
        f"🧱 Общий бан: <b>{_format_bool_badge(user.get('banned'))}</b>\n"
        f"🚫 Причина бана: <code>{_trim_text(str(user.get('ban_reason') or '-'), 60)}</code>\n"
        f"🆘 Поддержка ограничена: <b>{_format_bool_badge(support_restriction.get('active'))}</b>\n"
        f"⏳ До: <code>{support_restriction.get('expires_at') or '-'}</code>\n"
        f"📝 Причина: <code>{_trim_text(format_support_restriction_reason(str(support_restriction.get('reason') or '-')), 60)}</code>\n\n"
        "<b>Рефералка</b>\n"
        f"👥 Всего рефералов: <b>{int(referral.get('total_refs', 0) or 0)}</b>\n"
        f"💸 Оплативших: <b>{int(referral.get('paid_refs', 0) or 0)}</b>\n"
        f"💰 Заработано: <b>{float(referral.get('earned_rub', 0.0) or 0.0):.2f} ₽</b>\n"
        f"🏷 Статус партнёра: <b>{partner.get('status') or 'standard'}</b>\n"
        f"📝 Заметка: <code>{_trim_text(str(partner.get('note') or '-'), 60)}</code>\n\n"
        f"{_format_total_traffic_block(total_snapshot)}\n\n"
        f"<b>Поддержка</b>\n{support_text}\n\n"
        f"<b>Платежи</b>\n{payments_text}\n\n"
        f"<b>Выводы</b>\n{withdraws_text}\n\n"
        f"<b>Корректировки</b>\n{adjustments_text}"
    )


async def _build_support_restrictions_list_text(db: Database) -> str:
    rows = await db.list_support_restricted_users(limit=20) if hasattr(db, "list_support_restricted_users") else []
    notify_enabled = await db.support_restriction_notifications_enabled() if hasattr(db, "support_restriction_notifications_enabled") else True
    lines = [
        "🆘 <b>Ограничения поддержки</b>",
        "",
        f"Уведомления админам: <b>{'включены' if notify_enabled else 'выключены'}</b>",
        "",
    ]
    if not rows:
        lines.append("Активных ограничений сейчас нет.")
    else:
        lines.append("Активные ограничения:")
        for row in rows:
            lines.append(
                f"\n• user <code>{row.get('user_id')}</code> до <code>{row.get('expires_at') or '-'}</code>"
                f"\n  {escape(format_support_restriction_reason(str(row.get('reason') or '-')))}"
            )
    return "\n".join(lines)


async def _build_user_timeline_text(db: Database, user_id: int) -> str:
    items = await db.get_user_timeline(user_id, limit=25) if hasattr(db, "get_user_timeline") else []
    return (
        "🕓 <b>История пользователя</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n\n"
        f"{_format_user_timeline(items)}"
    )


async def _notify_support_restriction_admins(db: Database, bot: Bot, text: str) -> None:
    enabled = await db.support_restriction_notifications_enabled() if hasattr(db, "support_restriction_notifications_enabled") else True
    if not enabled:
        return
    await notify_admins(text, bot=bot)


async def _build_safe_mode_text(db: Database) -> str:
    enabled = str(await db.get_setting("system:safe_mode", "0") or "0") == "1"
    reason = str(await db.get_setting("system:safe_mode_reason", "") or "")
    manual = str(await db.get_setting("system:safe_mode_manual_override", "") or "")
    manual_label = {
        "1": "принудительно включён",
        "0": "принудительно выключен",
    }.get(manual, "авто-режим")
    return (
        "🧯 <b>Safe mode</b>\n\n"
        f"Сейчас: <b>{'включён' if enabled else 'выключен'}</b>\n"
        f"Режим работы: <b>{manual_label}</b>\n"
        f"Причина: <code>{reason or '-'}</code>\n\n"
        "При включённом safe mode новые покупки временно блокируются."
    )


async def _build_support_blacklist_text(db: Database) -> str:
    raw = await db.get_setting("support:blacklist_phrases", "") if hasattr(db, "get_setting") else ""
    phrases = [line.strip() for line in raw.splitlines() if line.strip()]
    recent_hits = await db.get_recent_support_blacklist_hits(limit=5) if hasattr(db, "get_recent_support_blacklist_hits") else []
    hits_text = "\n".join(
        f"• <code>{item.get('created_at') or '-'}</code> — user <code>{item.get('user_id')}</code> — {_trim_text(str(item.get('details') or '-'), 60)}"
        for item in recent_hits
    ) or "—"
    phrases_text = "\n".join(f"• <code>{escape(item)}</code>" for item in phrases[:20]) or "• список пуст"
    return (
        "🛡 <b>Blacklist поддержки</b>\n\n"
        "Фразы проверяются только в сообщениях, которые пользователь отправляет в поддержку.\n\n"
        f"<b>Текущий список</b>\n{phrases_text}\n\n"
        f"<b>Последние срабатывания</b>\n{hits_text}"
    )


def _admin_section_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")]])


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
            InlineKeyboardButton(text="📊 Периоды", callback_data="admindash:period:7:0"),
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


def _admin_content_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 Тарифы", callback_data="admin:tariffs"),
            InlineKeyboardButton(text="🏷 Промокоды", callback_data="admin:promo_menu"),
        ],
        [
            InlineKeyboardButton(text="📝 Шаблоны", callback_data="admin:templates"),
            InlineKeyboardButton(text="📣 Рассылки", callback_data="adminmenu:bulk"),
        ],
        [InlineKeyboardButton(text="📨 Главное сообщение", callback_data="admin:main_message")],
        [InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")],
    ])


def _admin_bulk_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📣 Рассылка всем", callback_data="bulk:prompt:all"),
            InlineKeyboardButton(text="📣 Рассылка активным", callback_data="bulk:prompt:active"),
        ],
        [
            InlineKeyboardButton(text="💤 Без подписки", callback_data="bulk:prompt:broadcast:inactive"),
            InlineKeyboardButton(text="💰 С балансом", callback_data="bulk:prompt:broadcast:with_balance"),
        ],
        [
            InlineKeyboardButton(text="🤝 Из рефералки", callback_data="bulk:prompt:broadcast:referred"),
            InlineKeyboardButton(text="⌛ Истекла подписка", callback_data="bulk:prompt:broadcast:expired"),
        ],
        [InlineKeyboardButton(text="🎁 Пробный период без покупки", callback_data="bulk:prompt:broadcast:trial_only")],
        [InlineKeyboardButton(text="⏱ Продлить всем активным", callback_data="bulk:prompt:extend")],
        [InlineKeyboardButton(text="🧵 Очередь фоновых задач", callback_data="bulk:jobs")],
        [InlineKeyboardButton(text="⬅️ К контенту", callback_data="adminmenu:content")],
    ])


def _admin_service_menu_keyboard(*, safe_mode_enabled: bool = False) -> InlineKeyboardMarkup:
    safe_mode_label = "🧯 Safe mode: выключить" if safe_mode_enabled else "🧯 Safe mode: включить"
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⭐ Stars", callback_data="admin:stars_settings"),
            InlineKeyboardButton(text="🤝 Реф. настройки", callback_data="admin:ref_settings"),
        ],
        [InlineKeyboardButton(text=safe_mode_label, callback_data="admin:safe_mode:toggle")],
        [
            InlineKeyboardButton(text="🧩 Inbound панели", callback_data="admin:panel_inbounds"),
            InlineKeyboardButton(text="🗓 Инциденты за день", callback_data="admindash:incidents:0"),
        ],
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


def _user_card_keyboard(user_id: int, *, banned: bool = False, support_blocked: bool = False) -> InlineKeyboardMarkup:
    ban_label = "✅ Снять бан" if banned else "⛔ Забанить"
    support_label = "🆘 Снять ограничение" if support_blocked else "🆘 Ограничить поддержку"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить карточку", callback_data=f"admin:usercard:{user_id}")],
            [
                InlineKeyboardButton(text="💰 Скорректировать баланс", callback_data=f"admin:usercard:balance_prompt:{user_id}"),
                InlineKeyboardButton(text="⛔ Отключить подписку", callback_data=f"admin:usercard:revoke_subscription:{user_id}"),
            ],
            [
                InlineKeyboardButton(text=support_label, callback_data=f"admin:usercard:support_menu:{user_id}"),
                InlineKeyboardButton(text=ban_label, callback_data=f"admin:usercard:ban_toggle:{user_id}"),
            ],
            [
                InlineKeyboardButton(text="🕓 История", callback_data=f"admin:usercard:timeline:{user_id}"),
                InlineKeyboardButton(text="📜 Тикеты", callback_data=f"admin:usercard:tickets:{user_id}"),
            ],
            [
                InlineKeyboardButton(text="🎁 Выдать тариф", callback_data=f"admin:usercard:grant_tariff:{user_id}"),
                InlineKeyboardButton(text="♻️ Сбросить trial", callback_data=f"admin:usercard:reset_trial:{user_id}"),
            ],
            [InlineKeyboardButton(text="🔔 Сбросить уведомления", callback_data=f"admin:usercard:reset_notify:{user_id}")],
            [InlineKeyboardButton(text="🗑 Удалить пользователя", callback_data=f"admin:usercard:delete_prompt:{user_id}")],
            [InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")],
        ]
    )


def _user_delete_confirm_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⚠️ Да, удалить полностью", callback_data=f"admin:usercard:delete_confirm:{user_id}")],
            [InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")],
        ]
    )


def _user_card_support_keyboard(user_id: int, *, support_blocked: bool) -> InlineKeyboardMarkup:
    rows = []
    if support_blocked:
        rows.append([InlineKeyboardButton(text="✅ Снять ограничение", callback_data=f"admin:usercard:support_unblock:{user_id}")])
    else:
        rows.append([
            InlineKeyboardButton(text="Спам · 1ч", callback_data=f"admin:usercard:support_block:{user_id}:spam"),
            InlineKeyboardButton(text="Флуд · 24ч", callback_data=f"admin:usercard:support_block:{user_id}:flood"),
        ])
        rows.append([
            InlineKeyboardButton(text="Оскорбления · 7д", callback_data=f"admin:usercard:support_block:{user_id}:abuse"),
            InlineKeyboardButton(text="Мошенничество · 30д", callback_data=f"admin:usercard:support_block:{user_id}:fraud"),
        ])
    rows.append([InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _user_card_grant_tariff_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for plan in get_all_active():
        plan_id = str(plan.get("id") or "").strip()
        if not plan_id:
            continue
        plan_name = str(plan.get("name") or plan_id)
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🎁 {plan_name}",
                    callback_data=f"admin:usercard:grant_tariff_confirm:{user_id}:{plan_id}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _support_restrictions_keyboard(rows: List[Dict[str, Any]], *, notify_enabled: bool) -> InlineKeyboardMarkup:
    toggle_label = "🔕 Выключить уведомления" if notify_enabled else "🔔 Включить уведомления"
    keyboard_rows: List[List[InlineKeyboardButton]] = []
    for row in rows[:8]:
        keyboard_rows.append([
            InlineKeyboardButton(text=f"👤 user {row.get('user_id')}", callback_data=f"admin:usercard:{row.get('user_id')}"),
        ])
    keyboard_rows.append([InlineKeyboardButton(text=toggle_label, callback_data="admin:support_restrictions:toggle_notify")])
    keyboard_rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="admin:support_restrictions:list")])
    keyboard_rows.append([InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


def _support_blacklist_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Изменить список", callback_data="admin:support_blacklist:edit")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin:support_blacklist")],
            [InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")],
        ]
    )


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


@router.message(F.text == "💳 Диагностика платежей")
async def payment_provider_diagnostics(message: Message, db: Database, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    text = await _build_provider_summary(db)
    await replace_message(
        message.from_user.id,
        text,
reply_markup=_provider_summary_keyboard(),
        delete_user_msg=message,
        bot=bot,
    )


@router.message(F.text == "🧭 Админ дашборд")
async def admin_dashboard_message(message: Message, db: Database, panel, payment_gateway, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    text = await _build_admin_dashboard_text(db, panel, payment_gateway)
    await replace_message(
        message.from_user.id,
        text,
        reply_markup=_admin_dashboard_keyboard(),
        delete_user_msg=message,
        bot=bot,
    )


@router.message(Command("admindash"))
@router.message(Command("admin"))
async def admin_dashboard_command(message: Message, db: Database, panel, payment_gateway):
    if not is_admin(message.from_user.id):
        return
    text = await _build_admin_dashboard_text(db, panel, payment_gateway)
    await message.answer(text, reply_markup=_admin_dashboard_keyboard(), parse_mode="HTML")


@router.message(F.text == "🏆 Топ-10 рефералов")
async def admin_top_referrers_message(message: Message, db: Database, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    text = await _build_top_referrers_detail(db, limit=10)
    await replace_message(
        message.from_user.id,
        text,
        reply_markup=_admin_section_keyboard(),
        delete_user_msg=message,
        bot=bot,
    )


@router.callback_query(F.data == "adminmenu:payments")
async def admin_payments_menu_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(callback.message, 
        "💳 <b>Платежи и диагностика</b>\n\nПроверка статусов, ручные операции и разбор проблемных платежей.",
        reply_markup=_admin_payments_menu_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "adminmenu:users")
async def admin_users_menu_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        "👥 <b>Пользователи и поддержка</b>\n\nКарточки пользователей, поддержка, ограничения и запросы на вывод.",
        reply_markup=_admin_users_menu_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "adminmenu:analytics")
async def admin_analytics_menu_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(callback.message, 
        "📈 <b>Аналитика и отчёты</b>\n\nЕжедневные и периодные отчёты, воронка и состояние системы.",
        reply_markup=_admin_analytics_menu_keyboard(),
        parse_mode="HTML",
    )
    try:
        await callback.answer()
    except Exception as exc:
        logger.warning("Analytics menu callback ack failed: %s", exc)


@router.callback_query(F.data == "adminmenu:content")
async def admin_content_menu_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        "📝 <b>Контент и продажи</b>\n\nТарифы, промокоды, шаблоны, рассылки и главное сообщение.",
        reply_markup=_admin_content_menu_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "adminmenu:bulk")
async def admin_bulk_menu_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(callback.message, 
        "📣 <b>Рассылки и массовые действия</b>\n\nСообщения для сегментов и массовое продление активных подписок.",
        reply_markup=_admin_bulk_menu_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "adminmenu:service")
async def admin_service_menu_callback(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    safe_mode_enabled = str(await db.get_setting("system:safe_mode", "0") or "0") == "1"
    await smart_edit_message(callback.message, 
        "⚙️ <b>Система и панель</b>\n\nПанель, safe mode, Stars, реферальные настройки и служебные действия.",
        reply_markup=_admin_service_menu_keyboard(safe_mode_enabled=safe_mode_enabled),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:panel_inbounds")
async def admin_panel_inbounds(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        _panel_inbounds_settings_text(),
        reply_markup=_panel_inbounds_settings_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:panel_inbounds:count")
async def admin_panel_inbounds_count_prompt(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    configured = _parse_panel_inbound_ids(Config.PANEL_TARGET_INBOUND_IDS)
    await state.set_state(PaymentDiagnosticsFSM.waiting_inbound_count)
    await smart_edit_message(
        callback.message,
        (
            "🔢 <b>Количество активных инбаундов</b>\n\n"
            f"Сейчас в списке ID: <code>{', '.join(str(item) for item in configured) or 'не заданы'}</code>\n"
            f"Отправьте число от <b>0</b> до <b>{len(configured)}</b>.\n"
            "<code>0</code> = использовать все ID из списка.\n"
            "Любое другое число = использовать первые N ID из списка."
        ),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:panel_inbounds")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:panel_inbounds:ids")
async def admin_panel_inbounds_ids_prompt(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await state.set_state(PaymentDiagnosticsFSM.waiting_inbound_ids)
    await smart_edit_message(
        callback.message,
        (
            "🆔 <b>ID инбаундов для регистрации</b>\n\n"
            "Отправьте список ID через запятую.\n"
            "Можно указывать сколько угодно ID.\n"
            "Пример: <code>2,3,4,7,8,11</code>"
        ),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:panel_inbounds")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:user_lookup")
async def admin_user_lookup_prompt(callback: CallbackQuery, state: FSMContext, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await state.set_state(PaymentDiagnosticsFSM.waiting_user_id)
    recent = await db.get_recent_user_ids(limit=5) if hasattr(db, "get_recent_user_ids") else []
    recent_labels = [await _format_user_id_with_name(callback.bot, db, int(item)) for item in recent]
    recent_text = ", ".join(recent_labels) if recent_labels else "нет данных"
    await smart_edit_message(
        callback.message,
        "👤 <b>Поиск пользователя</b>\n\nОтправьте <code>user_id</code> одним сообщением.\n\n"
        f"Последние регистрации: {recent_text}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")]]),
        parse_mode="HTML",
    )
    await callback.answer()


async def _send_user_card(message: Message, db: Database, user_id: int, *, state: FSMContext | None = None) -> None:
    user = await db.get_user(user_id)
    display_name = await _resolve_user_display_name(message.bot, user_id, user)
    text = await _build_user_card_text(db, user_id, display_name_override=display_name)
    restriction = await db.get_support_restriction(user_id) if hasattr(db, "get_support_restriction") else {}
    if state is not None:
        await state.clear()
    await message.answer(
        text,
        parse_mode="HTML",
        reply_markup=_user_card_keyboard(
            user_id,
            banned=bool((user or {}).get("banned")),
            support_blocked=bool(restriction.get("active")),
        ),
    )


@router.message(PaymentDiagnosticsFSM.waiting_user_id)
async def admin_user_lookup_receive(message: Message, state: FSMContext, db: Database):
    if not is_admin(message.from_user.id):
        return
    raw = (message.text or "").strip()
    try:
        user_id = int(raw)
    except ValueError:
        await message.answer("❌ Отправьте числовой user_id.")
        return
    await _send_user_card(message, db, user_id, state=state)


@router.message(F.text.regexp(r"^\d{5,}$"))
async def admin_user_lookup_quick(message: Message, state: FSMContext, db: Database):
    if not is_admin(message.from_user.id):
        return
    current_state = await state.get_state()
    if current_state:
        return
    raw = (message.text or "").strip()
    try:
        user_id = int(raw)
    except ValueError:
        return
    user = await db.get_user(user_id)
    if not user:
        return
    await _send_user_card(message, db, user_id)


async def _resolve_user_display_name(bot: Bot, user_id: int, user: Dict[str, Any] | None = None) -> str:
    payload = user or {}
    username = str(payload.get("username") or "").strip()
    first_name = str(payload.get("first_name") or "").strip()
    parts: List[str] = []
    if username:
        parts.append(f"@{username}")
    if first_name:
        parts.append(first_name)
    if parts:
        return " | ".join(parts)
    try:
        chat = await bot.get_chat(user_id)
    except Exception as exc:
        logger.warning("User card get_chat failed for %s: %s", user_id, exc)
        return "—"
    fresh_parts: List[str] = []
    chat_username = str(getattr(chat, "username", "") or "").strip()
    chat_first_name = str(getattr(chat, "first_name", "") or "").strip()
    if chat_username:
        fresh_parts.append(f"@{chat_username}")
    if chat_first_name:
        fresh_parts.append(chat_first_name)
    return " | ".join(fresh_parts) if fresh_parts else "—"


async def _format_user_id_with_name(bot: Bot, db: Database, user_id: int) -> str:
    user = await db.get_user(user_id)
    display_name = await _resolve_user_display_name(bot, user_id, user)
    if display_name == "—":
        return f"<code>{user_id}</code>"
    return f"<code>{user_id}</code> ({escape(display_name)})"


@router.callback_query(F.data.regexp(r"^admin:usercard:\d+$"))
async def admin_user_card_callback(callback: CallbackQuery, db: Database, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[2])
    user = await db.get_user(user_id)
    display_name = await _resolve_user_display_name(bot, user_id, user)
    text = await _build_user_card_text(db, user_id, display_name_override=display_name)
    restriction = await db.get_support_restriction(user_id) if hasattr(db, "get_support_restriction") else {}
    await smart_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=_user_card_keyboard(
            user_id,
            banned=bool((user or {}).get("banned")),
            support_blocked=bool(restriction.get("active")),
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:timeline:"))
async def admin_user_card_timeline(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    await smart_edit_message(
        callback.message,
        await _build_user_timeline_text(db, user_id),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")]]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:tickets:"))
async def admin_user_card_tickets(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    tickets = await db.list_user_support_tickets(user_id, limit=10)
    lines = ["📜 <b>Тикеты пользователя</b>", "", f"Пользователь: <code>{user_id}</code>"]
    if not tickets:
        lines.append("\nТикетов пока нет.")
    else:
        for ticket in tickets:
            lines.append(
                f"\n• <code>#{ticket.get('id')}</code> — <b>{format_support_status(str(ticket.get('status') or ''), lowercase=True)}</b> — <code>{ticket.get('updated_at') or '-'}</code>"
            )
    await smart_edit_message(
        callback.message,
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")]]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:balance_prompt:"))
async def admin_user_card_balance_prompt(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    await state.set_state(PaymentDiagnosticsFSM.waiting_user_balance_adjustment)
    await state.update_data(target_user_id=user_id)
    await smart_edit_message(
        callback.message,
        "💰 <b>Корректировка баланса</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n"
        "Отправьте сумму и причину одним сообщением.\n"
        "Пример: <code>+150 бонус за кампанию</code>\n"
        "Пример: <code>-50 ручная корректировка</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К карточке", callback_data=f"admin:usercard:{user_id}")]]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:grant_tariff:"))
async def admin_user_card_grant_tariff_prompt(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    user = await db.get_user(user_id)
    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return
    active_plans = get_all_active()
    if not active_plans:
        await callback.answer("Нет активных тарифов", show_alert=True)
        return
    plan_lines = [
        f"• <b>{escape(str(plan.get('name') or plan.get('id') or 'Тариф'))}</b>"
        for plan in active_plans[:8]
    ]
    text = (
        "🎁 <b>Выдача тарифа</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n\n"
        "Выберите тариф, который нужно выдать вручную:\n"
        + "\n".join(plan_lines)
    )
    await smart_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=_user_card_grant_tariff_keyboard(user_id),
    )
    await callback.answer()


@router.callback_query(F.data.regexp(r"^admin:usercard:grant_tariff_confirm:\d+:[A-Za-z0-9_.-]+$"))
async def admin_user_card_grant_tariff_confirm(callback: CallbackQuery, db: Database, panel, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return

    parts = callback.data.split(":")
    user_id = int(parts[-2])
    plan_id = parts[-1]
    user = await db.get_user(user_id)
    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return

    plan = get_by_id(plan_id)
    if not plan:
        await callback.answer("Тариф не найден", show_alert=True)
        return

    vpn_url = await create_subscription(
        user_id,
        plan,
        db=db,
        panel=panel,
        plan_suffix=" (выдан админом)",
        preserve_active_days=True,
    )
    if not vpn_url:
        await callback.answer("Не удалось выдать тариф", show_alert=True)
        return

    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(
            user_id,
            callback.from_user.id,
            "grant_tariff",
            f"plan_id={plan_id}",
        )

    plan_name = str(plan.get("name") or plan_id)
    await notify_user(
        bot,
        user_id,
        (
            "🎁 <b>Вам выдан тариф</b>\n\n"
            f"Тариф: <b>{escape(plan_name)}</b>\n"
            "Подключение уже готово в личном кабинете."
        ),
    )

    text = await _build_user_card_text(db, user_id)
    refreshed = await db.get_user(user_id)
    restriction = await db.get_support_restriction(user_id) if hasattr(db, "get_support_restriction") else {}
    await smart_edit_message(
        callback.message,
        "✅ Тариф выдан вручную.\n\n" + text,
        parse_mode="HTML",
        reply_markup=_user_card_keyboard(
            user_id,
            banned=bool((refreshed or {}).get("banned")),
            support_blocked=bool(restriction.get("active")),
        ),
    )
    await callback.answer("Тариф выдан")


@router.callback_query(F.data.startswith("admin:usercard:reset_trial:"))
async def admin_user_card_reset_trial(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return

    user_id = int(callback.data.split(":")[-1])
    user = await db.get_user(user_id)
    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return

    await db.update_user(user_id, trial_used=0, trial_declined=0)
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(
            user_id,
            callback.from_user.id,
            "reset_trial",
            "trial_used=0 trial_declined=0",
        )

    text = await _build_user_card_text(db, user_id)
    refreshed = await db.get_user(user_id)
    restriction = await db.get_support_restriction(user_id) if hasattr(db, "get_support_restriction") else {}
    await smart_edit_message(
        callback.message,
        "♻️ Возможность активировать пробный период сброшена.\n\n" + text,
        parse_mode="HTML",
        reply_markup=_user_card_keyboard(
            user_id,
            banned=bool((refreshed or {}).get("banned")),
            support_blocked=bool(restriction.get("active")),
        ),
    )
    await callback.answer("Пробный период сброшен")


@router.message(PaymentDiagnosticsFSM.waiting_user_balance_adjustment)
async def admin_user_card_balance_save(message: Message, state: FSMContext, db: Database):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    user_id = int(data.get("target_user_id") or 0)
    parts = (message.text or "").split(maxsplit=1)
    if user_id <= 0 or not parts:
        await message.answer("❌ Не удалось определить пользователя.")
        return
    try:
        amount = float(parts[0].replace(",", "."))
    except ValueError:
        await message.answer("❌ Укажите сумму числом, например <code>+150</code>.", parse_mode="HTML")
        return
    reason = parts[1].strip() if len(parts) > 1 else "Быстрая корректировка из карточки"
    await db.add_user(user_id)
    await db.add_referral_balance_adjustment(user_id, message.from_user.id, amount, reason)
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(user_id, message.from_user.id, "balance_adjustment", f"{amount:.2f} {reason}")
    await state.clear()
    text = await _build_user_card_text(db, user_id)
    user = await db.get_user(user_id)
    restriction = await db.get_support_restriction(user_id) if hasattr(db, "get_support_restriction") else {}
    await message.answer(
        "✅ Баланс обновлён.\n\n" + text,
        parse_mode="HTML",
        reply_markup=_user_card_keyboard(
            user_id,
            banned=bool((user or {}).get("banned")),
            support_blocked=bool(restriction.get("active")),
        ),
    )


@router.callback_query(F.data.startswith("admin:usercard:ban_toggle:"))
async def admin_user_card_ban_toggle(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    user = await db.get_user(user_id)
    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return
    if bool(user.get("banned")):
        await db.unban_user(user_id)
        if hasattr(db, "add_admin_user_action"):
            await db.add_admin_user_action(user_id, callback.from_user.id, "unban", "")
        await callback.answer("Бан снят")
    else:
        await db.ban_user(user_id, reason=f"admin_quick_action:{callback.from_user.id}")
        if hasattr(db, "add_admin_user_action"):
            await db.add_admin_user_action(user_id, callback.from_user.id, "ban", "quick action")
        await callback.answer("Пользователь забанен")
    text = await _build_user_card_text(db, user_id)
    refreshed = await db.get_user(user_id)
    restriction = await db.get_support_restriction(user_id) if hasattr(db, "get_support_restriction") else {}
    await smart_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=_user_card_keyboard(
            user_id,
            banned=bool((refreshed or {}).get("banned")),
            support_blocked=bool(restriction.get("active")),
        ),
    )


@router.callback_query(F.data.startswith("admin:usercard:reset_notify:"))
async def admin_user_card_reset_notifications(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    await db.reset_expiry_notifications(user_id)
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(user_id, callback.from_user.id, "reset_expiry_notifications", "")
    await callback.answer("Уведомления сброшены")
    text = await _build_user_card_text(db, user_id)
    user = await db.get_user(user_id)
    restriction = await db.get_support_restriction(user_id) if hasattr(db, "get_support_restriction") else {}
    await smart_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=_user_card_keyboard(
            user_id,
            banned=bool((user or {}).get("banned")),
            support_blocked=bool(restriction.get("active")),
        ),
    )


@router.callback_query(F.data.startswith("admin:usercard:revoke_subscription:"))
async def admin_user_card_revoke_subscription(callback: CallbackQuery, db: Database, panel, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    user = await db.get_user(user_id)
    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return
    ok = await revoke_subscription(
        user_id,
        db=db,
        panel=panel,
        reason="Отключено администратором",
    )
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(
            user_id,
            callback.from_user.id,
            "revoke_subscription",
            f"ok={int(bool(ok))}",
        )
    if ok:
        await notify_user(
            bot,
            user_id,
            "⛔ <b>Подписка отключена администратором</b>\n\nЕсли это ошибка, напишите в поддержку.",
        )
    refreshed = await db.get_user(user_id)
    display_name = await _resolve_user_display_name(bot, user_id, refreshed)
    text = await _build_user_card_text(db, user_id, display_name_override=display_name)
    restriction = await db.get_support_restriction(user_id) if hasattr(db, "get_support_restriction") else {}
    await smart_edit_message(
        callback.message,
        ("⛔ Подписка отключена.\n\n" if ok else "⚠️ Не удалось отключить подписку полностью.\n\n") + text,
        parse_mode="HTML",
        reply_markup=_user_card_keyboard(
            user_id,
            banned=bool((refreshed or {}).get("banned")),
            support_blocked=bool(restriction.get("active")),
        ),
    )
    await callback.answer("Подписка отключена" if ok else "Не удалось отключить")


@router.callback_query(F.data.startswith("admin:usercard:delete_prompt:"))
async def admin_user_card_delete_prompt(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    user = await db.get_user(user_id)
    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return
    base_email = await panel_base_email(user_id, db)
    text = (
        "🗑 <b>Удаление пользователя</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n"
        f"Panel email: <code>{escape(base_email or '-')}</code>\n\n"
        "Будет удалён из бота, PostgreSQL/SQLite и из панели 3x-ui.\n"
        "Действие необратимо."
    )
    await smart_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=_user_delete_confirm_keyboard(user_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin:usercard:delete_confirm:"))
async def admin_user_card_delete_confirm(callback: CallbackQuery, db: Database, panel):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    base_email = await panel_base_email(user_id, db)
    panel_deleted = False
    panel_error = ""
    if base_email:
        try:
            panel_deleted = await panel.delete_client(base_email)
        except Exception as exc:
            panel_error = str(exc)
            logger.error("User delete: panel cleanup failed user=%s error=%s", user_id, exc)

    stats = await db.delete_user_everywhere(user_id)
    text = (
        "✅ <b>Пользователь удалён</b>\n\n"
        f"User ID: <code>{user_id}</code>\n"
        f"Panel cleanup: <b>{'ok' if panel_deleted else 'not found / skipped'}</b>\n"
        f"Удалено записей в БД: <b>{int(stats.get('deleted', 0))}</b>"
    )
    if panel_error:
        text += f"\nОшибка панели: <code>{escape(panel_error[:300])}</code>"
    await smart_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")]]),
    )
    await callback.answer("Пользователь удалён")


@router.callback_query(F.data.startswith("admin:usercard:support_menu:"))
async def admin_user_card_support_menu(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    restriction = await db.get_support_restriction(user_id) if hasattr(db, "get_support_restriction") else {}
    text = (
        "🆘 <b>Ограничение поддержки</b>\n\n"
        f"Пользователь: <code>{user_id}</code>\n"
        f"Сейчас ограничено: <b>{'да' if restriction.get('active') else 'нет'}</b>\n"
        f"До: <code>{restriction.get('expires_at') or '-'}</code>\n"
        f"Причина: <code>{escape(format_support_restriction_reason(str(restriction.get('reason') or '-')))}</code>"
    )
    await smart_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=_user_card_support_keyboard(user_id, support_blocked=bool(restriction.get("active"))),
    )
    await callback.answer()


@router.callback_query(F.data == "admin:support_restrictions:list")
async def admin_support_restrictions_list(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    rows = await db.list_support_restricted_users(limit=20) if hasattr(db, "list_support_restricted_users") else []
    notify_enabled = await db.support_restriction_notifications_enabled() if hasattr(db, "support_restriction_notifications_enabled") else True
    await smart_edit_message(
        callback.message,
        await _build_support_restrictions_list_text(db),
        parse_mode="HTML",
        reply_markup=_support_restrictions_keyboard(rows, notify_enabled=notify_enabled),
    )
    await callback.answer()


@router.callback_query(F.data == "admin:support_restrictions:toggle_notify")
async def admin_support_restrictions_toggle_notify(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    current = await db.support_restriction_notifications_enabled() if hasattr(db, "support_restriction_notifications_enabled") else True
    await db.set_support_restriction_notifications_enabled(not current)
    notify_enabled = await db.support_restriction_notifications_enabled()
    rows = await db.list_support_restricted_users(limit=20) if hasattr(db, "list_support_restricted_users") else []
    await smart_edit_message(
        callback.message,
        await _build_support_restrictions_list_text(db),
        parse_mode="HTML",
        reply_markup=_support_restrictions_keyboard(rows, notify_enabled=notify_enabled),
    )
    await callback.answer("Настройка обновлена")


@router.callback_query(F.data.startswith("admin:usercard:support_block:"))
async def admin_user_card_support_block(callback: CallbackQuery, db: Database, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    _, _, _, user_id_raw, preset_key = callback.data.split(":")
    user_id = int(user_id_raw)
    preset = SUPPORT_RESTRICTION_PRESETS.get(preset_key)
    if not preset:
        await callback.answer("Неизвестная причина", show_alert=True)
        return
    await db.set_support_restriction(
        user_id,
        hours=int(preset["hours"]),
        reason=f"{preset['reason']} by admin {callback.from_user.id}",
    )
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(user_id, callback.from_user.id, "support_restriction_set", f"{preset['reason']} {preset['hours']}h")
    await _notify_support_restriction_admins(
        db,
        bot,
        (
            "🆘 <b>Ограничение поддержки включено</b>\n\n"
            f"Пользователь: <code>{user_id}</code>\n"
            f"Причина: <code>{preset['reason']}</code>\n"
            f"Срок: <b>{preset['hours']} ч</b>\n"
            f"Админ: <code>{callback.from_user.id}</code>"
        ),
    )
    await callback.answer("Ограничение поддержки включено")
    text = await _build_user_card_text(db, user_id)
    user = await db.get_user(user_id)
    restriction = await db.get_support_restriction(user_id)
    await smart_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=_user_card_keyboard(
            user_id,
            banned=bool((user or {}).get("banned")),
            support_blocked=bool(restriction.get("active")),
        ),
    )


@router.callback_query(F.data.startswith("admin:usercard:support_unblock:"))
async def admin_user_card_support_unblock(callback: CallbackQuery, db: Database, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    await db.clear_support_restriction(user_id)
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(user_id, callback.from_user.id, "support_restriction_cleared", "")
    await _notify_support_restriction_admins(
        db,
        bot,
        (
            "✅ <b>Ограничение поддержки снято</b>\n\n"
            f"Пользователь: <code>{user_id}</code>\n"
            f"Админ: <code>{callback.from_user.id}</code>"
        ),
    )
    await callback.answer("Ограничение снято")
    text = await _build_user_card_text(db, user_id)
    user = await db.get_user(user_id)
    await smart_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=_user_card_keyboard(
            user_id,
            banned=bool((user or {}).get("banned")),
            support_blocked=False,
        ),
    )


@router.callback_query(F.data == "admin:safe_mode:toggle")
async def admin_safe_mode_toggle(callback: CallbackQuery, db: Database, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    enabled = str(await db.get_setting("system:safe_mode", "0") or "0") == "1"
    next_enabled = not enabled
    await db.set_setting("system:safe_mode", "1" if next_enabled else "0")
    await db.set_setting("system:safe_mode_reason", "manual_admin_toggle")
    await db.set_setting("system:safe_mode_manual_override", "1" if next_enabled else "0")
    await notify_admins(
        (
            f"⚠️ <b>Safe mode {'включён' if next_enabled else 'выключен'} вручную</b>\n\n"
            f"Админ: <code>{callback.from_user.id}</code>\n"
            "Причина: <code>manual_admin_toggle</code>"
        ),
        bot=bot,
    )
    text = await _build_safe_mode_text(db)
    await smart_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔁 Переключить ещё раз", callback_data="admin:safe_mode:toggle")],
                [InlineKeyboardButton(text="♻️ Вернуть авто-режим", callback_data="admin:safe_mode:auto")],
                [InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")],
            ]
        ),
    )
    await callback.answer("Режим обновлён")


@router.callback_query(F.data == "admin:safe_mode:auto")
async def admin_safe_mode_auto(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await db.set_setting("system:safe_mode_manual_override", "")
    text = await _build_safe_mode_text(db)
    await smart_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔁 Переключить вручную", callback_data="admin:safe_mode:toggle")],
                [InlineKeyboardButton(text="⬅️ К сервису", callback_data="adminmenu:service")],
            ]
        ),
    )
    await callback.answer("Авто-режим восстановлен")


@router.callback_query(F.data == "admin:support_blacklist")
async def admin_support_blacklist(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    text = await _build_support_blacklist_text(db)
    await smart_edit_message(callback.message, text, parse_mode="HTML", reply_markup=_support_blacklist_keyboard())
    await callback.answer()


@router.callback_query(F.data == "admin:support_blacklist:edit")
async def admin_support_blacklist_edit(callback: CallbackQuery, state: FSMContext, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await state.set_state(PaymentDiagnosticsFSM.waiting_support_blacklist)
    raw = await db.get_setting("support:blacklist_phrases", "") if hasattr(db, "get_setting") else ""
    preview = raw.strip() or "список пуст"
    await smart_edit_message(
        callback.message,
        "🛡 <b>Редактор blacklist поддержки</b>\n\n"
        "Отправьте список фраз, по одной на строке.\n"
        "Пустое сообщение не подходит. Чтобы очистить список, отправьте <code>clear</code>.\n\n"
        f"Текущий список:\n<code>{escape(preview[:1500])}</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:support_blacklist")]]),
    )
    await callback.answer()


@router.message(PaymentDiagnosticsFSM.waiting_support_blacklist)
async def admin_support_blacklist_save(message: Message, state: FSMContext, db: Database):
    if not is_admin(message.from_user.id):
        return
    raw = (message.text or "").strip()
    value = "" if raw.lower() == "clear" else raw
    await db.set_setting("support:blacklist_phrases", value)
    await state.clear()
    text = await _build_support_blacklist_text(db)
    await message.answer(text, parse_mode="HTML", reply_markup=_support_blacklist_keyboard())


@router.callback_query(F.data.in_({"admin_dashboard", "admin:dashboard"}))
async def admin_dashboard_callback(callback: CallbackQuery, db: Database, panel, payment_gateway):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    text = await _build_admin_dashboard_text(db, panel, payment_gateway)
    await smart_edit_message(callback.message, text, reply_markup=_admin_dashboard_keyboard(), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data == "admin:exit")
async def admin_exit_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(callback.message, 
        "🛠️ <b>Админ панель закрыта</b>\n\nИспользуйте /admin или кнопку «🛠️ Админ меню», чтобы открыть её снова.",
        reply_markup=None,
        parse_mode="HTML",
    )


@router.message(PaymentDiagnosticsFSM.waiting_inbound_count)
async def admin_panel_inbounds_count_save(message: Message, state: FSMContext, bot: Bot, db: Database):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    configured = _parse_panel_inbound_ids(Config.PANEL_TARGET_INBOUND_IDS)
    if not configured:
        await message.answer("❌ Сначала задайте список ID инбаундов.")
        return
    try:
        value = int((message.text or "").strip())
    except ValueError:
        await message.answer("❌ Введите целое число.")
        return
    if value < 0 or value > len(configured):
        await message.answer(f"❌ Введите число от 0 до {len(configured)}.")
        return
    Config.set_panel_target_inbound_count(value)
    await db.set_setting("system:panel_target_inbound_count", str(value))
    _write_env_variable("PANEL_TARGET_INBOUND_COUNT", str(value))
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    await bot.send_message(
        message.from_user.id,
        _panel_inbounds_settings_text(),
        reply_markup=_panel_inbounds_settings_keyboard(),
        parse_mode="HTML",
    )


@router.message(PaymentDiagnosticsFSM.waiting_inbound_ids)
async def admin_panel_inbounds_ids_save(message: Message, state: FSMContext, bot: Bot, db: Database):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    ids = _parse_panel_inbound_ids(message.text or "")
    if not ids:
        await message.answer("❌ Отправьте хотя бы один корректный числовой ID.")
        return
    Config.set_panel_target_inbound_ids(",".join(str(item) for item in ids))
    await db.set_setting("system:panel_target_inbound_ids", ",".join(str(item) for item in ids))
    if Config.PANEL_TARGET_INBOUND_COUNT > len(ids):
        Config.set_panel_target_inbound_count(len(ids))
        await db.set_setting("system:panel_target_inbound_count", str(len(ids)))
        _write_env_variable("PANEL_TARGET_INBOUND_COUNT", str(len(ids)))
    _write_env_variable("PANEL_TARGET_INBOUND_IDS", ",".join(str(item) for item in ids))
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    await bot.send_message(
        message.from_user.id,
        _panel_inbounds_settings_text(),
        reply_markup=_panel_inbounds_settings_keyboard(),
        parse_mode="HTML",
    )


@router.message(F.text == "🔎 Платёж по ID")
async def payment_diagnostics_prompt(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(PaymentDiagnosticsFSM.waiting_payment_id)
    await replace_message(
        message.from_user.id,
        "🔎 <b>Диагностика платежа</b>\n\nОтправьте <code>PAYMENT_ID</code> одним сообщением.",
        reply_markup=main_menu_keyboard(True),
        delete_user_msg=message,
        bot=bot,
    )


@router.callback_query(F.data == "paydiag_prompt")
async def payment_diagnostics_prompt_callback(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await state.set_state(PaymentDiagnosticsFSM.waiting_payment_id)
    await smart_edit_message(callback.message, 
        "🔎 <b>Диагностика платежа</b>\n\nОтправьте <code>PAYMENT_ID</code> одним сообщением.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(PaymentDiagnosticsFSM.waiting_payment_id)
async def payment_diagnostics_lookup_by_id(message: Message, state: FSMContext, db: Database, payment_gateway):
    if not is_admin(message.from_user.id):
        return
    payment_id = (message.text or "").strip()
    if not payment_id or payment_id.startswith("/"):
        await message.answer("Отправьте корректный <code>PAYMENT_ID</code>.")
        return
    await state.clear()
    result = await _build_payment_diagnostics(payment_id, db, payment_gateway)
    if not result:
        await message.answer(f"❌ Платёж <code>{payment_id}</code> не найден", parse_mode="HTML")
        return
    await message.answer(result["text"], reply_markup=_diagnostics_keyboard(result["payment"], result["remote_payment"], result["checkout_url"]), parse_mode="HTML")


@router.message(F.text == "🧾 Последние действия")
async def payment_actions_text(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    actions = await db.get_recent_payment_admin_actions(limit=15)
    await message.answer(
        "🧾 <b>Последние admin actions по платежам</b>\n\n" + _format_global_admin_actions(actions),
        parse_mode="HTML",
    )


@router.message(F.text == "⏳ Pending операции")
async def payment_operations_text(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    text, items = await _render_pending_operations_text(db, provider="all", operation="all")
    await message.answer(text, reply_markup=_pending_operations_keyboard(provider="all", operation="all", items=items), parse_mode="HTML")


@router.message(F.text == "🚨 Требует внимания")
async def payment_attention_text(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    text, items = await _render_attention_text(db, provider="all", issue_type="all")
    await message.answer(text, reply_markup=_attention_keyboard(provider="all", issue_type="all", items=items), parse_mode="HTML")


@router.message(F.text == "🛠️ Авто-резолв attention")
async def payment_attention_resolve_text(message: Message, db: Database, payment_gateway, bot: Bot, panel):
    if not is_admin(message.from_user.id):
        return
    summary = await auto_resolve_payment_attention(
        db=db,
        panel=panel,
        payment_gateway=payment_gateway,
        bot=bot,
        provider="all",
        issue_type="all",
        limit=Config.PAYMENT_ATTENTION_RESOLVE_LIMIT,
    )
    await message.answer(
        (
            "🛠️ <b>Attention auto-resolver</b>\n\n"
            f"Resolved: <b>{summary.get('total_resolved', 0)}</b>\n"
            f"Skipped: <b>{summary.get('total_skipped', 0)}</b>\n"
            f"Processing: <b>{summary['processing']['resolved']}</b> / skip {summary['processing']['skipped']}\n"
            f"Operations: <b>{summary['operations']['resolved']}</b> / skip {summary['operations']['skipped']}\n"
            f"Mismatch: <b>{summary['mismatch']['resolved']}</b> / skip {summary['mismatch']['skipped']}"
        ),
        parse_mode="HTML",
    )


@router.message(Command("paydiag"))
async def payment_diagnostics_command(message: Message, db: Database, payment_gateway, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Используйте: <code>/paydiag PAYMENT_ID</code>")
        return
    payment_id = parts[1].strip()
    result = await _build_payment_diagnostics(payment_id, db, payment_gateway)
    if not result:
        await message.answer(f"❌ Платёж <code>{payment_id}</code> не найден")
        return
    await message.answer(result["text"], reply_markup=_diagnostics_keyboard(result["payment"], result["remote_payment"], result["checkout_url"]), parse_mode="HTML")


async def _render_payment_diagnostics(callback: CallbackQuery, payment_id: str, db: Database, payment_gateway) -> bool:
    result = await _build_payment_diagnostics(payment_id, db, payment_gateway)
    if not result:
        return False
    await smart_edit_message(callback.message, result["text"], reply_markup=_diagnostics_keyboard(result["payment"], result["remote_payment"], result["checkout_url"]), parse_mode="HTML")
    return True




@router.message(Command("payactions"))
async def payment_actions_command(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    actions = await db.get_recent_payment_admin_actions(limit=15)
    await message.answer(
        "🧾 <b>Последние admin actions по платежам</b>\n\n" + _format_global_admin_actions(actions),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "payactions_recent")
async def payment_actions_recent(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    actions = await db.get_recent_payment_admin_actions(limit=15)
    await smart_edit_message(callback.message, 
        "🧾 <b>Последние admin actions по платежам</b>\n\n" + _format_global_admin_actions(actions),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔎 Платёж по ID", callback_data="paydiag_prompt")], [InlineKeyboardButton(text="⏳ Pending операции", callback_data="payops:list:all:all")], [InlineKeyboardButton(text="🚨 Требует внимания", callback_data="payattention:list:all:all")], [InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(Command("payops"))
async def payment_operations_command(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    text, items = await _render_pending_operations_text(db, provider="all", operation="all")
    await message.answer(text, reply_markup=_pending_operations_keyboard(provider="all", operation="all", items=items), parse_mode="HTML")


@router.callback_query(F.data.startswith("payops:list:"))
async def payment_operations_list(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    _, _, provider, operation = callback.data.split(":", 3)
    text, items = await _render_pending_operations_text(db, provider=provider, operation=operation)
    await smart_edit_message(callback.message, text, reply_markup=_pending_operations_keyboard(provider=provider, operation=operation, items=items), parse_mode="HTML")
    await callback.answer()


@router.message(Command("payattention"))
async def payment_attention_command(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    text, items = await _render_attention_text(db, provider="all", issue_type="all")
    await message.answer(text, reply_markup=_attention_keyboard(provider="all", issue_type="all", items=items), parse_mode="HTML")


@router.callback_query(F.data.startswith("payattention:list:"))
async def payment_attention_list(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    _, _, provider, issue_type = callback.data.split(":", 3)
    text, items = await _render_attention_text(db, provider=provider, issue_type=issue_type)
    await smart_edit_message(callback.message, text, reply_markup=_attention_keyboard(provider=provider, issue_type=issue_type, items=items), parse_mode="HTML")
    await callback.answer()


@router.message(Command("payresolve"))
async def payment_attention_resolve_command(message: Message, db: Database, payment_gateway, bot: Bot, panel):
    if not is_admin(message.from_user.id):
        return
    summary = await auto_resolve_payment_attention(
        db=db,
        panel=panel,
        payment_gateway=payment_gateway,
        bot=bot,
        provider="all",
        issue_type="all",
        limit=Config.PAYMENT_ATTENTION_RESOLVE_LIMIT,
    )
    await message.answer(
        (
            "🛠️ <b>Attention auto-resolver</b>\n\n"
            f"Resolved: <b>{summary.get('total_resolved', 0)}</b>\n"
            f"Skipped: <b>{summary.get('total_skipped', 0)}</b>\n"
            f"Processing: <b>{summary['processing']['resolved']}</b> / skip {summary['processing']['skipped']}\n"
            f"Operations: <b>{summary['operations']['resolved']}</b> / skip {summary['operations']['skipped']}\n"
            f"Mismatch: <b>{summary['mismatch']['resolved']}</b> / skip {summary['mismatch']['skipped']}"
        ),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("payattention:resolve:"))
async def payment_attention_resolve_callback(callback: CallbackQuery, db: Database, payment_gateway, bot: Bot, panel):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    _, _, provider, issue_type = callback.data.split(":", 3)
    summary = await auto_resolve_payment_attention(
        db=db,
        panel=panel,
        payment_gateway=payment_gateway,
        bot=bot,
        provider=provider,
        issue_type=issue_type,
        limit=Config.PAYMENT_ATTENTION_RESOLVE_LIMIT,
    )
    text, items = await _render_attention_text(db, provider=provider, issue_type=issue_type)
    text += (
        "\n\n🛠️ <b>Auto-resolver</b>\n"
        f"Resolved: <b>{summary.get('total_resolved', 0)}</b>\n"
        f"Skipped: <b>{summary.get('total_skipped', 0)}</b>"
    )
    await smart_edit_message(callback.message, text, reply_markup=_attention_keyboard(provider=provider, issue_type=issue_type, items=items), parse_mode="HTML")
    await callback.answer(f"Resolved {summary.get('total_resolved', 0)}")


@router.callback_query(F.data == "paydiag_provider_summary")
async def payment_provider_summary_callback(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    text = await _build_provider_summary(db)
    await smart_edit_message(callback.message, 
        text,
reply_markup=_provider_summary_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("paydiag_refresh:"))
async def payment_diagnostics_refresh(callback: CallbackQuery, db: Database, payment_gateway):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    payment_id = callback.data.split(":", 1)[1]
    if not await _render_payment_diagnostics(callback, payment_id, db, payment_gateway):
        await callback.answer("Платёж не найден", show_alert=True)
        return
    await callback.answer("Обновлено")


@router.callback_query(F.data.startswith("paydiag_refund:"))
async def payment_diagnostics_refund_yookassa(callback: CallbackQuery, db: Database, payment_gateway, bot: Bot, panel):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    payment_id = callback.data.split(":", 1)[1]
    payment = await db.get_pending_payment(payment_id)
    if not payment or (payment.get("provider") or "") != "yookassa":
        await callback.answer("Это не платёж ЮKassa", show_alert=True)
        return
    provider_payment_id = get_provider_payment_id(payment)
    if not provider_payment_id:
        await callback.answer("Нет внешнего ID платежа", show_alert=True)
        return
    refund = await payment_gateway.create_refund(
        payment_id=provider_payment_id,
        amount=float(payment.get("amount") or 0),
        reason=f"admin refund for {payment_id}",
    )
    if not refund:
        await db.add_payment_admin_action(payment_id, callback.from_user.id, "yookassa_refund", provider="yookassa", result="failed", details="provider refund call failed")
        await callback.answer("Не удалось создать refund в ЮKassa", show_alert=True)
        return

    await db.record_payment_status_transition(
        payment_id,
        from_status=payment.get("status"),
        to_status="refund_requested",
        source="payment_admin/yookassa_refund",
        reason=f"admin={callback.from_user.id}",
        metadata=f"provider_refund_id={refund.get('id', '')};status={refund.get('status', '')};awaiting_confirmation=1",
    )
    await db.add_payment_admin_action(
        payment_id,
        callback.from_user.id,
        "yookassa_refund",
        provider="yookassa",
        result="ok",
        details=f"refund_id={refund.get('id', '')};status={refund.get('status', '')};awaiting_confirmation=1",
    )
    await callback.answer("Refund в ЮKassa создан")
    await notify_admins(
        (
            f"↩️ <b>Создан refund ЮKassa</b>\n"
            f"💳 <code>{payment_id}</code>\n"
            f"🧷 <code>{provider_payment_id}</code>\n"
            f"🆔 Refund: <code>{refund.get('id', '-')}</code>"
        ),
        bot=bot,
    )
    await _render_payment_diagnostics(callback, payment_id, db, payment_gateway)


@router.callback_query(F.data.startswith("paydiag_cancel:"))
async def payment_diagnostics_cancel_yookassa(callback: CallbackQuery, db: Database, payment_gateway, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    payment_id = callback.data.split(":", 1)[1]
    payment = await db.get_pending_payment(payment_id)
    if not payment or (payment.get("provider") or "") != "yookassa":
        await callback.answer("Это не платёж ЮKassa", show_alert=True)
        return
    provider_payment_id = get_provider_payment_id(payment)
    if not provider_payment_id:
        await callback.answer("Нет внешнего ID платежа", show_alert=True)
        return
    cancelled = await payment_gateway.cancel_payment(provider_payment_id)
    if not cancelled:
        await db.add_payment_admin_action(payment_id, callback.from_user.id, "yookassa_cancel", provider="yookassa", result="failed", details="provider cancel call failed")
        await callback.answer("Не удалось отменить платёж в ЮKassa", show_alert=True)
        return
    local_cancelled = await db.update_payment_status(
        payment_id,
        "cancelled",
        allowed_current_statuses=["pending", "processing"],
        source="payment_admin/yookassa_cancel",
        reason=f"admin={callback.from_user.id}",
        metadata=f"provider_payment_id={provider_payment_id};status={cancelled.get('status', '')}",
    )
    await db.record_payment_status_transition(
        payment_id,
        from_status=payment.get("status"),
        to_status="cancel_requested",
        source="payment_admin/yookassa_cancel",
        reason=f"admin={callback.from_user.id}",
        metadata=f"provider_payment_id={provider_payment_id};status={cancelled.get('status', '')};local_cancelled={int(local_cancelled)}",
    )
    await db.add_payment_admin_action(payment_id, callback.from_user.id, "yookassa_cancel", provider="yookassa", result="ok", details=f"local_cancelled={int(local_cancelled)}")
    await callback.answer("Платёж отменён в ЮKassa")
    await notify_admins(
        (
            f"🛑 <b>Отменён платёж ЮKassa</b>\n"
            f"💳 <code>{payment_id}</code>\n"
            f"🧷 <code>{provider_payment_id}</code>"
        ),
        bot=bot,
    )
    await _render_payment_diagnostics(callback, payment_id, db, payment_gateway)


@router.callback_query(F.data.startswith("paydiag_refund_stars:"))
async def payment_diagnostics_refund_stars(callback: CallbackQuery, db: Database, payment_gateway, bot: Bot, panel):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    payment_id = callback.data.split(":", 1)[1]
    payment = await db.get_pending_payment(payment_id)
    if not payment or (payment.get("provider") or "") != "telegram_stars":
        await callback.answer("Это не Stars-платёж", show_alert=True)
        return
    charge_id = payment.get("provider_payment_id")
    if not charge_id:
        await callback.answer("Нет Telegram charge id", show_alert=True)
        return
    ok = await payment_gateway.refund_payment(bot=bot, user_id=int(payment["user_id"]), telegram_payment_charge_id=charge_id)
    if not ok:
        await db.add_payment_admin_action(payment_id, callback.from_user.id, "stars_refund", provider="telegram_stars", result="failed", details="refund_star_payment returned false")
        await callback.answer("Не удалось выполнить refund Stars", show_alert=True)
        return
    local_refunded = False
    subscription_revoked = False
    if payment.get("status") == "accepted":
        local_refunded = await db.update_payment_status(
            payment_id,
            "refunded",
            allowed_current_statuses=["accepted"],
            source="payment_admin/stars_refund",
            reason=f"admin={callback.from_user.id}",
            metadata=f"telegram_payment_charge_id={charge_id}",
        )
        subscription_revoked = await revoke_subscription(
            int(payment["user_id"]), db=db, panel=panel, reason="Возврат Telegram Stars"
        )
    await db.record_payment_status_transition(
        payment_id,
        from_status=payment.get("status"),
        to_status="stars_refunded",
        source="payment_admin/stars_refund",
        reason=f"admin={callback.from_user.id}",
        metadata=f"telegram_payment_charge_id={charge_id};local_refunded={int(local_refunded)};subscription_revoked={int(subscription_revoked)}",
    )
    await db.add_payment_admin_action(payment_id, callback.from_user.id, "stars_refund", provider="telegram_stars", result="ok", details=f"local_refunded={int(local_refunded)};subscription_revoked={int(subscription_revoked)}")
    await notify_user(
        int(payment["user_id"]),
        "↩️ Ваш платёж в Telegram Stars был возвращён администратором.",
        bot=bot,
    )
    await callback.answer("Refund Stars выполнен")
    await _render_payment_diagnostics(callback, payment_id, db, payment_gateway)


@router.callback_query(F.data == "admindash:bot")
async def admin_dash_bot(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(callback.message, await _build_bot_stats_detail(db), reply_markup=_admin_section_keyboard(), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data.in_({"admindash:referrals", "admindash:withdraws"}))
async def admin_dash_referrals(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(callback.message, await _build_referral_detail(db), reply_markup=_admin_section_keyboard(), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data == "admindash:topref")
async def admin_dash_top_referrers(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(callback.message, await _build_top_referrers_detail(db, limit=10), reply_markup=_admin_section_keyboard(), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data == "admindash:health")
async def admin_dash_health(callback: CallbackQuery, db: Database, panel, payment_gateway):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await smart_edit_message(callback.message, await _build_health_detail(db, panel, payment_gateway), reply_markup=_admin_section_keyboard(), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data.startswith("admindash:incidents:"))
async def admin_dash_incidents(callback: CallbackQuery, db: Database, panel, payment_gateway):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    try:
        days_ago = max(0, int(callback.data.split(":")[-1]))
    except Exception:
        days_ago = 0
    await smart_edit_message(
        callback.message,
        await _build_incident_report_detail(db, panel, payment_gateway, days_ago=days_ago),
        reply_markup=_incident_report_keyboard(days_ago),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admindash:daily:"))
async def admin_dash_daily(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    try:
        days_ago = max(0, int(callback.data.split(":")[-1]))
    except Exception:
        days_ago = 0
    await smart_edit_message(callback.message, await _build_daily_report_detail(db, days_ago=days_ago), reply_markup=_daily_report_keyboard(days_ago), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data.startswith("admindash:period:"))
async def admin_dash_period(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    try:
        _, _, _, days_str, end_days_str = callback.data.split(":", 4)
        days = max(1, int(days_str))
        end_days_ago = max(0, int(end_days_str))
    except Exception:
        days, end_days_ago = 7, 0
    await smart_edit_message(callback.message, 
        await _build_period_report_detail(db, days=days, end_days_ago=end_days_ago),
        reply_markup=_period_report_keyboard(days, end_days_ago),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(Command("periodreport"))
async def admin_period_report_command(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    await message.answer(await _build_period_report_detail(db, days=7, end_days_ago=0), reply_markup=_period_report_keyboard(7, 0), parse_mode="HTML")


@router.message(F.text == "📊 Периоды")
async def admin_period_report_message(message: Message, db: Database, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    await replace_message(
        message.from_user.id,
        await _build_period_report_detail(db, days=7, end_days_ago=0),
        reply_markup=_period_report_keyboard(7, 0),
        delete_user_msg=message,
        bot=bot,
    )


@router.message(Command("dailyreport"))
async def admin_daily_report_command(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    await message.answer(await _build_daily_report_detail(db, days_ago=0), reply_markup=_daily_report_keyboard(0), parse_mode="HTML")


@router.message(F.text == "📈 Ежедневный отчёт")
async def admin_daily_report_message(message: Message, db: Database, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    await replace_message(
        message.from_user.id,
        await _build_daily_report_detail(db, days_ago=0),
        reply_markup=_daily_report_keyboard(0),
        delete_user_msg=message,
        bot=bot,
    )
