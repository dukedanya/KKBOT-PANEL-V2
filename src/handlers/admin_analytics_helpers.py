from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from db import Database
from services.health import collect_health_snapshot


async def _build_bot_stats_detail(db: Database) -> str:
    total_users = await db.get_total_users()
    subscribed = len(await db.get_subscribed_user_ids())
    banned = await db.get_banned_users_count()
    inactive = max(0, int(total_users) - int(subscribed))
    return f"""📊 <b>Бот и подписки</b>

📅 Срез: <code>{datetime.now().date().isoformat()}</code>

<b>Пользователи</b>
👥 Всего пользователей: <b>{total_users}</b>
📦 Активных подписок: <b>{subscribed}</b>
🕳 Без активной подписки: <b>{inactive}</b>
⛔ Заблокировано: <b>{banned}</b>"""


async def _build_referral_detail(db: Database) -> str:
    pending_withdraws = await db.get_pending_withdraw_requests()
    top_referrers = await db.get_top_referrers_extended(limit=5)
    total_pending_amount = sum(float(item.get("amount") or 0) for item in pending_withdraws)
    top_text = "\n".join(
        f"• <code>{row.get('ref_by')}</code> — <b>{int(row.get('paid_count', 0) or 0)}</b> оплат — <b>{float(row.get('earned_rub') or 0):.2f} ₽</b>"
        for row in top_referrers
    ) if top_referrers else "—"
    return f"""🤝 <b>Рефералка и выводы</b>

📅 Срез: <code>{datetime.now().date().isoformat()}</code>

<b>Выводы</b>
💸 Pending запросы на вывод: <b>{len(pending_withdraws)}</b>
💰 Сумма pending выводов: <b>{total_pending_amount:.2f} ₽</b>

<b>Топ рефереры</b>
{top_text}"""


async def _build_top_referrers_detail(db: Database, *, limit: int = 10) -> str:
    top_referrers = await db.get_top_referrers_extended(limit=limit)
    top_text = "\n".join(
        f"{index}. <code>{row.get('ref_by')}</code> — <b>{int(row.get('paid_count', 0) or 0)}</b> оплат — <b>{float(row.get('earned_rub') or 0):.2f} ₽</b>"
        for index, row in enumerate(top_referrers, start=1)
    ) if top_referrers else "—"
    return f"""🏆 <b>Топ-{limit} рефералов</b>

📅 Срез: <code>{datetime.now().date().isoformat()}</code>

<b>Лидеры</b>
{top_text}"""


async def _build_daily_report_detail(db: Database, *, days_ago: int = 0) -> str:
    users = await db.get_daily_user_acquisition_report(days_ago=days_ago)
    sales = await db.get_daily_subscription_sales_report(days_ago=days_ago)
    day_label = "Сегодня" if days_ago == 0 else "Вчера" if days_ago == 1 else users.get("report_date") or f"-{days_ago}d"
    new_users = int(users.get("new_users", 0) or 0)
    referred_new_users = int(users.get("referred_new_users", 0) or 0)
    trial_started = int(users.get("trial_started_new_users", 0) or 0)
    subscriptions_bought = int(sales.get("subscriptions_bought", 0) or 0)
    return f"""📈 <b>Ежедневный отчёт — {day_label}</b>

📅 Период: <code>{users.get('report_date') or sales.get('report_date') or '-'}</code>

{_analytics_funnel_block(
    new_users=new_users,
    referred_new_users=referred_new_users,
    trial_started=trial_started,
    subscriptions_bought=subscriptions_bought,
    internal_balance_subscriptions=int(sales.get('internal_balance_subscriptions', 0) or 0),
)}

{_analytics_finance_block(sales)}

{_analytics_conversion_block(new_users=new_users, trial_started=trial_started, subscriptions_bought=subscriptions_bought)}"""


def _format_pct(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0.0%"
    return f"{(float(numerator) / float(denominator) * 100):.1f}%"


def _build_analytics_period_rows() -> list[list[InlineKeyboardButton]]:
    return [
        [
            InlineKeyboardButton(text="Сегодня", callback_data="admindash:daily:0"),
            InlineKeyboardButton(text="Вчера", callback_data="admindash:daily:1"),
        ],
        [
            InlineKeyboardButton(text="7 дней", callback_data="admindash:period:7"),
            InlineKeyboardButton(text="30 дней", callback_data="admindash:period:30"),
        ],
        [
            InlineKeyboardButton(text="Прошлый месяц", callback_data="admindash:period:last_month"),
            InlineKeyboardButton(text="Всего", callback_data="admindash:period:all"),
        ],
    ]


def _analytics_funnel_block(
    *,
    new_users: int,
    referred_new_users: int,
    trial_started: int,
    subscriptions_bought: int,
    internal_balance_subscriptions: int,
) -> str:
    return (
        "<b>Воронка</b>\n"
        f"👥 Новые пользователи: <b>{new_users}</b>\n"
        f"🤝 Пришли по реферальной системе: <b>{referred_new_users}</b> ({_format_pct(referred_new_users, new_users)})\n"
        f"🎁 Подключили trial: <b>{trial_started}</b> ({_format_pct(trial_started, new_users)})\n"
        f"🛒 Купили подписку: <b>{subscriptions_bought}</b> ({_format_pct(subscriptions_bought, new_users)})\n"
        f"💳 Купили с баланса: <b>{internal_balance_subscriptions}</b>"
    )


def _analytics_finance_block(sales: dict[str, Any]) -> str:
    return (
        "<b>Финансы</b>\n"
        f"💰 Заработано: <b>{sales.get('gross_revenue', 0.0):.2f} ₽</b>\n"
        f"🧾 Выдано админом на баланс: <b>{sales.get('admin_balance_issued', 0.0):.2f} ₽</b>\n"
        f"💳 Потрачено внутреннего баланса: <b>{sales.get('internal_balance_spent', 0.0):.2f} ₽</b>\n"
        f"↩️ Возвраты: <b>{sales.get('refunded_revenue', 0.0):.2f} ₽</b>\n"
        f"📊 Чистая выручка: <b>{sales.get('net_revenue', 0.0):.2f} ₽</b>\n"
        f"🤝 Реферальные начисления: <b>{sales.get('referral_cost', 0.0):.2f} ₽</b>\n"
        f"🧮 Предположительная прибыль: <b>{sales.get('estimated_profit', 0.0):.2f} ₽</b>"
    )


def _analytics_conversion_block(*, new_users: int, trial_started: int, subscriptions_bought: int) -> str:
    return (
        "<b>Конверсия</b>\n"
        f"➡️ Новые → trial: <b>{_format_pct(trial_started, new_users)}</b>\n"
        f"➡️ Новые → покупка: <b>{_format_pct(subscriptions_bought, new_users)}</b>\n"
        f"➡️ Trial → покупка: <b>{_format_pct(subscriptions_bought, trial_started)}</b>"
    )


def _resolve_last_month_dates() -> tuple[str, str]:
    today = datetime.now().date()
    first_day_this_month = today.replace(day=1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)
    return first_day_prev_month.isoformat(), last_day_prev_month.isoformat()


async def _build_period_report_detail(db: Database, *, period: str = "last_month") -> str:
    if period == "all":
        users = await db.get_total_user_acquisition_report()
        sales = await db.get_total_subscription_sales_report()
        title = "Всего"
    elif period == "30":
        users = await db.get_period_user_acquisition_report(days=30, end_days_ago=0)
        sales = await db.get_period_subscription_sales_report(days=30, end_days_ago=0)
        title = "30 дней"
    elif period == "7":
        users = await db.get_period_user_acquisition_report(days=7, end_days_ago=0)
        sales = await db.get_period_subscription_sales_report(days=7, end_days_ago=0)
        title = "7 дней"
    else:
        start_date, end_date = _resolve_last_month_dates()
        users = await db.get_user_acquisition_report_between(start_date=start_date, end_date=end_date)
        sales = await db.get_subscription_sales_report_between(start_date=start_date, end_date=end_date)
        title = "Прошлый месяц"
    new_users = int(users.get("new_users", 0) or 0)
    referred_new_users = int(users.get("referred_new_users", 0) or 0)
    trial_started = int(users.get("trial_started_new_users", 0) or 0)
    subscriptions_bought = int(sales.get("subscriptions_bought", 0) or 0)
    return f"""📊 <b>Отчёт по периоду — {title}</b>

📅 Период: <code>{users.get('start_date') or sales.get('start_date') or '-'}</code> → <code>{users.get('end_date') or sales.get('end_date') or '-'}</code>

{_analytics_funnel_block(
    new_users=new_users,
    referred_new_users=referred_new_users,
    trial_started=trial_started,
    subscriptions_bought=subscriptions_bought,
    internal_balance_subscriptions=int(sales.get('internal_balance_subscriptions', 0) or 0),
)}

{_analytics_finance_block(sales)}

{_analytics_conversion_block(new_users=new_users, trial_started=trial_started, subscriptions_bought=subscriptions_bought)}"""


def _daily_report_keyboard(days_ago: int = 0) -> InlineKeyboardMarkup:
    prev_days = max(0, int(days_ago) + 1)
    next_days = max(0, int(days_ago) - 1)
    rows = _build_analytics_period_rows()
    if days_ago >= 1:
        rows.append([InlineKeyboardButton(text="⬅️ Более ранний день", callback_data=f"admindash:daily:{prev_days}")])
        rows.append([InlineKeyboardButton(text="➡️ Ближе к сегодня", callback_data=f"admindash:daily:{next_days}")])
    rows.append([InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _period_report_keyboard(period: str = "last_month") -> InlineKeyboardMarkup:
    rows = _build_analytics_period_rows()
    rows.append([InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _build_health_detail(db: Database, panel, payment_gateway) -> str:
    snapshot = await collect_health_snapshot(db, panel, payment_gateway)
    status = "OK" if snapshot.get("ok") else "WARN"
    return f"""🩺 <b>Состояние системы</b>

📅 Срез: <code>{datetime.now().date().isoformat()}</code>

<b>Статус</b>
🟢 Общий статус: <b>{status}</b>
🗄 БД: <b>{'OK' if snapshot.get('database') else 'FAIL'}</b>
🧩 Panel: <b>{'OK' if snapshot.get('panel') else 'FAIL'}</b>
💳 Провайдер: <b>{'OK' if snapshot.get('payment_provider') else 'FAIL'}</b>

<b>Технические показатели</b>
🧬 Schema version: <code>{snapshot.get('schema_version', 0)}</code>
⚙️ Processing сейчас: <b>{snapshot.get('processing_count', 0)}</b>
⏳ Старые pending: <b>{snapshot.get('old_pending_count', 0)}</b>
🚨 Ошибки за 24ч: <b>{snapshot.get('payment_error_count', 0)}</b>"""


async def _build_incident_report_detail(db: Database, panel, payment_gateway, *, days_ago: int = 0) -> str:
    incidents = await db.get_daily_incident_report(days_ago=days_ago)
    snapshot = await collect_health_snapshot(db, panel, payment_gateway)
    schema_issues = await db.get_schema_drift_issues() if hasattr(db, "get_schema_drift_issues") else []
    safe_mode_enabled = str(await db.get_setting("system:safe_mode", "0") or "0") == "1"
    safe_mode_reason = str(await db.get_setting("system:safe_mode_reason", "") or "")
    return f"""🚨 <b>Отчёт по инцидентам</b>

📅 Период: <code>{incidents.get('report_date', '-')}</code>

<b>События</b>
⚠️ Ошибки платежей: <b>{incidents.get('payment_errors', 0)}</b>
🛡 Срабатывания blacklist поддержки: <b>{incidents.get('support_blacklist_hits', 0)}</b>
🕒 Stale processing: <b>{incidents.get('stale_processing', 0)}</b>
⏳ Старые pending: <b>{incidents.get('old_pending', 0)}</b>

<b>Система</b>
🧯 Safe mode: <b>{'включён' if safe_mode_enabled else 'выключен'}</b>
📝 Причина: <code>{safe_mode_reason or '-'}</code>
🧬 Проблемы схемы: <b>{len(schema_issues)}</b>
⚙️ Processing сейчас: <b>{snapshot.get('processing_count', 0)}</b>
🚨 Ошибки за 24ч: <b>{snapshot.get('payment_error_count', 0)}</b>"""


def _admin_section_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")]]
    )


def _admin_analytics_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
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
        ]
    )


def _incident_report_keyboard(days_ago: int = 0) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if days_ago > 0:
        rows.append(
            [InlineKeyboardButton(text="➡️ Ближе к сегодня", callback_data=f"admindash:incidents:{max(0, days_ago - 1)}")]
        )
    rows.append([InlineKeyboardButton(text="⬅️ Более ранний день", callback_data=f"admindash:incidents:{days_ago + 1}")])
    rows.append([InlineKeyboardButton(text="⬅️ К аналитике", callback_data="adminmenu:analytics")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
