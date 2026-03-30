import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import Config
from services.health import collect_health_snapshot, emit_health_alerts

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BackgroundContext:
    db: object
    panel: object
    payment_gateway: object
    bot: Bot
    health_alert_state: object


@dataclass(slots=True)
class JobSpec:
    name: str
    factory: Callable[[BackgroundContext], Awaitable[None]]
    enabled: bool = True


def _parse_sqlite_ts(value: object) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except ValueError:
        return None


def _is_payment_retry_due(payment: dict) -> bool:
    next_retry_at = _parse_sqlite_ts(payment.get("next_retry_at"))
    if not next_retry_at:
        return True
    return next_retry_at <= datetime.utcnow()


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _hours_since(value: object) -> float | None:
    parsed = _parse_sqlite_ts(value)
    if not parsed:
        return None
    return (_utc_now_naive() - parsed).total_seconds() / 3600.0


async def _setting_ts_due(db: object, key: str, *, repeat_hours: int) -> bool:
    raw = await db.get_setting(key, "") if hasattr(db, "get_setting") else ""
    if not raw:
        return True
    parsed = _parse_sqlite_ts(raw)
    if not parsed:
        return True
    return (_utc_now_naive() - parsed).total_seconds() >= max(1, repeat_hours) * 3600


async def _setting_age_hours(db: object, key: str) -> float | None:
    raw = await db.get_setting(key, "") if hasattr(db, "get_setting") else ""
    if not raw:
        return None
    parsed = _parse_sqlite_ts(raw)
    if not parsed:
        return None
    return (_utc_now_naive() - parsed).total_seconds() / 3600.0


async def _mark_setting_ts(db: object, key: str) -> None:
    if hasattr(db, "set_setting"):
        await db.set_setting(key, datetime.now(timezone.utc).isoformat())


def _abandoned_payment_reminder_key(payment_id: str) -> str:
    return f"payments:abandoned_reminder:{payment_id}"


def _inactive_reactivation_key(user_id: int) -> str:
    return f"marketing:inactive_reactivation:{int(user_id)}"


def _expired_reactivation_key(user_id: int) -> str:
    return f"marketing:expired_reactivation:{int(user_id)}"


def _trial_followup_key(user_id: int) -> str:
    return f"marketing:trial_followup:{int(user_id)}"


def _trial_recovery_promo_key(user_id: int) -> str:
    return f"marketing:trial_recovery_promo:{int(user_id)}"


def _stage_setting_key(prefix: str, entity_id: str | int, stage_id: str) -> str:
    return f"{prefix}:{entity_id}:{stage_id}"


async def _increment_setting_counter(db: object, key: str, delta: int = 1) -> None:
    if not hasattr(db, "get_setting") or not hasattr(db, "set_setting"):
        return
    raw = await db.get_setting(key, "0")
    try:
        current = int(str(raw or "0").strip())
    except ValueError:
        current = 0
    await db.set_setting(key, str(current + int(delta)))


async def _ensure_trial_recovery_promo(db: object, user_id: int) -> str:
    existing = await db.get_setting(_trial_recovery_promo_key(user_id), "") if hasattr(db, "get_setting") else ""
    code = str(existing or "").strip().upper()
    if code and hasattr(db, "get_promo_code"):
        promo = await db.get_promo_code(code)
        if promo:
            return code

    code = f"TRIAL{int(user_id)}"
    expires_at = (datetime.now(timezone.utc) + timedelta(hours=max(1, int(Config.TRIAL_RECOVERY_PROMO_EXPIRE_HOURS or 72)))).isoformat()
    if hasattr(db, "create_or_update_promo_code"):
        await db.create_or_update_promo_code(
            code,
            title=f"Trial recovery {user_id}",
            description=f"Персональный оффер после trial для user {user_id}",
            discount_percent=float(Config.TRIAL_RECOVERY_PROMO_PERCENT or 15.0),
            discount_type="percent",
            fixed_amount=0.0,
            only_new_users=False,
            plan_ids="",
            user_limit=1,
            max_uses=1,
            active=True,
            expires_at=expires_at,
        )
    if hasattr(db, "set_active_user_promo_code"):
        await db.set_active_user_promo_code(user_id, code)
    if hasattr(db, "set_setting"):
        await db.set_setting(_trial_recovery_promo_key(user_id), code)
    return code


async def _resolve_payment_checkout_url(payment: dict, payment_gateway) -> str:
    from services.payment_gateway import build_payment_gateway
    from utils.payments import get_provider_payment_id

    provider = str(payment.get("provider") or "").strip().lower()
    if not provider or provider in {"balance", "telegram_stars"}:
        return ""
    provider_payment_id = get_provider_payment_id(payment)
    if not provider_payment_id:
        return ""
    gateway = payment_gateway
    should_close = False
    if str(getattr(payment_gateway, "provider_name", "") or "").strip().lower() != provider:
        gateway = build_payment_gateway(provider)
        should_close = True
    try:
        remote_payment = await gateway.get_payment(provider_payment_id)
        if not remote_payment:
            return ""
        return gateway.get_checkout_url(remote_payment) or ""
    finally:
        if should_close and hasattr(gateway, "close"):
            await gateway.close()


async def _set_safe_mode(ctx: BackgroundContext, *, enabled: bool, reason: str) -> None:
    manual_raw = await ctx.db.get_setting("system:safe_mode_manual_override", "")
    if enabled and str(manual_raw or "").strip() == "0":
        return
    if (not enabled) and str(manual_raw or "").strip() == "1":
        return
    current_raw = await ctx.db.get_setting("system:safe_mode", "0")
    current_enabled = str(current_raw or "0") == "1"
    if current_enabled == enabled:
        return
    await ctx.db.set_setting("system:safe_mode", "1" if enabled else "0")
    await ctx.db.set_setting("system:safe_mode_reason", reason[:500] if enabled else "")
    state_label = "ВКЛЮЧЁН" if enabled else "ВЫКЛЮЧЕН"
    from utils.helpers import register_transient_message
    text = f"⚠️ <b>Safe mode {state_label}</b>\n\nПричина: <code>{reason[:700]}</code>"
    for admin_id in Config.ADMIN_USER_IDS:
        sent = await ctx.bot.send_message(admin_id, text, parse_mode="HTML")
        await register_transient_message(
            db=ctx.db,
            chat_id=admin_id,
            message_id=sent.message_id,
            category="safe_mode_notice",
            ttl_hours=48,
        )


def _should_send_daily_report(now_utc: datetime, last_sent_date: str) -> bool:
    target_hour = int(getattr(Config, "DAILY_ADMIN_REPORT_HOUR_UTC", 6) or 6)
    today = now_utc.date().isoformat()
    return now_utc.hour >= target_hour and last_sent_date != today


def _should_send_incident_report(now_utc: datetime, last_sent_date: str) -> bool:
    target_hour = int(getattr(Config, "DAILY_INCIDENT_REPORT_HOUR_UTC", 7) or 7)
    today = now_utc.date().isoformat()
    return now_utc.hour >= target_hour and last_sent_date != today



def _log_task_result(task: asyncio.Task) -> None:
    try:
        exc = task.exception()
    except asyncio.CancelledError:
        logger.info("Background task cancelled: %s", task.get_name())
        return
    except Exception as exc:  # pragma: no cover
        logger.error("Task callback failed: %s", exc)
        return

    if exc:
        logger.exception("Background task crashed: %s", task.get_name(), exc_info=exc)
    else:
        logger.info("Background task finished: %s", task.get_name())



def create_background_task(coro, *, name: str) -> asyncio.Task:
    logger.info("Starting background task: %s", name)
    task = asyncio.create_task(coro, name=name)
    task.add_done_callback(_log_task_result)
    return task


async def cancel_background_tasks(tasks: list[asyncio.Task]) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def check_expired_subscriptions(ctx: BackgroundContext) -> None:
    from kkbot.services.subscriptions import is_active_subscription

    while True:
        try:
            users = await ctx.db.get_all_subscribers()
            for user in users:
                try:
                    await is_active_subscription(int(user["user_id"]), db=ctx.db, panel=ctx.panel)
                except Exception as user_error:
                    logger.error("Subscription check failed: user=%s error=%s", user.get("user_id"), user_error)
            await asyncio.sleep(Config.EXPIRED_CHECK_INTERVAL_SEC)
        except Exception as exc:
            logger.error("Expired subscription loop failed: %s", exc)
            await asyncio.sleep(60)


async def reconcile_provider_payments(ctx: BackgroundContext) -> None:
    from kkbot.services.payment_flow import process_successful_payment, reject_pending_payment

    while True:
        try:
            pending = await ctx.db.get_all_pending_payments(statuses=["pending", "processing"])
            provider_name = getattr(ctx.payment_gateway, "provider_name", Config.PAYMENT_PROVIDER)
            for payment in pending:
                if payment.get("provider") not in (None, "", provider_name):
                    continue
                if not _is_payment_retry_due(payment):
                    continue
                provider_payment_id = payment.get("provider_payment_id") or payment.get("itpay_id")
                if not provider_payment_id:
                    continue

                remote_payment = await ctx.payment_gateway.get_payment(provider_payment_id)
                if not remote_payment:
                    continue

                remote_status = ctx.payment_gateway.extract_status(remote_payment)
                if ctx.payment_gateway.is_success_status(remote_payment):
                    result = await process_successful_payment(
                        payment=payment,
                        db=ctx.db,
                        panel=ctx.panel,
                        bot=ctx.bot,
                        admin_context=f"{provider_name.upper()} reconcile status={remote_status}",
                    )
                    if not result.get("ok") and result.get("reason") != "already_processing":
                        logger.warning(
                            "Payment reconcile: activation failed payment=%s status=%s reason=%s",
                            payment.get("payment_id"),
                            remote_status,
                            result.get("reason"),
                        )
                elif ctx.payment_gateway.is_failed_status(remote_payment):
                    result = await reject_pending_payment(
                        payment=payment,
                        db=ctx.db,
                        bot=ctx.bot,
                        reason_text=(
                            "❌ <b>Платёж не был завершён.</b>\n\n"
                            "Если деньги всё же списались, напишите в поддержку — мы проверим вручную."
                        ),
                        admin_context=f"{provider_name.upper()} reconcile status={remote_status}",
                    )
                    if not result.get("ok") and result.get("reason") != "already_processing":
                        logger.warning(
                            "Payment reconcile: reject failed payment=%s status=%s reason=%s",
                            payment.get("payment_id"),
                            remote_status,
                            result.get("reason"),
                        )
            await asyncio.sleep(Config.PAYMENT_RECONCILE_INTERVAL_SEC)
        except Exception as exc:
            logger.error("Payment reconcile loop failed: %s", exc)
            await asyncio.sleep(Config.PAYMENT_RECONCILE_INTERVAL_SEC)


async def recover_stuck_processing_payments(ctx: BackgroundContext) -> None:
    while True:
        try:
            released = await ctx.db.reclaim_stale_processing_payments(
                timeout_minutes=Config.STALE_PROCESSING_TIMEOUT_MIN,
                source="background/recover_stuck_processing_payments",
            )
            if released:
                logger.warning("Recovered stale processing payments: %s", released)
            await asyncio.sleep(Config.STALE_PROCESSING_RECOVERY_INTERVAL_SEC)
        except Exception as exc:
            logger.error("Stale processing recovery failed: %s", exc)
            await asyncio.sleep(120)


async def health_monitor(ctx: BackgroundContext) -> None:
    while True:
        try:
            snapshot = await collect_health_snapshot(ctx.db, ctx.panel, ctx.payment_gateway)
            await emit_health_alerts(snapshot=snapshot, state=ctx.health_alert_state, bot=ctx.bot)
            schema_issues = await ctx.db.get_schema_drift_issues() if hasattr(ctx.db, "get_schema_drift_issues") else []
            safe_mode_reason_parts = []
            if not snapshot.get("ok"):
                safe_mode_reason_parts.append("dependency_health_not_ok")
            if schema_issues:
                safe_mode_reason_parts.append("schema_drift")
            if safe_mode_reason_parts:
                await _set_safe_mode(
                    ctx,
                    enabled=True,
                    reason=";".join(safe_mode_reason_parts + schema_issues[:5]),
                )
            else:
                await _set_safe_mode(ctx, enabled=False, reason="health_restored")
            await asyncio.sleep(Config.HEALTHCHECK_INTERVAL_SEC)
        except Exception as exc:
            logger.error("Health monitor failed: %s", exc)
            await asyncio.sleep(60)


async def cleanup_old_payments(ctx: BackgroundContext) -> None:
    while True:
        try:
            deleted = await ctx.db.cleanup_old_pending_payments(days=30)
            dedup_deleted = await ctx.db.cleanup_old_payment_events(days=30)
            if deleted or dedup_deleted:
                logger.info("Cleanup old payment records | Удалено: payments=%s events=%s", deleted, dedup_deleted)
            await asyncio.sleep(259200)
        except Exception as exc:
            logger.error("Cleanup old payments failed: %s", exc)
            await asyncio.sleep(3600)


async def archive_closed_support_tickets_job(ctx: BackgroundContext) -> None:
    while True:
        try:
            archived = await ctx.db.archive_closed_support_tickets(days=Config.SUPPORT_ARCHIVE_AFTER_DAYS)
            if archived:
                from utils.helpers import notify_admins
                await notify_admins(
                    f"📦 <b>Архив тикетов поддержки</b>\n\nВ архив переведено: <b>{archived}</b>",
                    bot=ctx.bot,
                )
            await asyncio.sleep(21600)
        except Exception as exc:
            logger.error("Support archive job failed: %s", exc)
            await asyncio.sleep(1800)


async def remind_stale_support_tickets_job(ctx: BackgroundContext) -> None:
    while True:
        try:
            stale_tickets = await ctx.db.list_stale_support_tickets(
                minutes=Config.SUPPORT_TICKET_REMINDER_AFTER_MIN,
                limit=20,
            )
            for ticket in stale_tickets:
                ticket_id = int(ticket.get("id") or 0)
                if ticket_id <= 0:
                    continue
                reminder_state = await ctx.db.get_support_ticket_reminder_state(ticket_id)
                updated_at = str(ticket.get("updated_at") or "")
                if reminder_state == updated_at:
                    continue
                text = (
                    "⏰ <b>Тикет ждёт ответа</b>\n\n"
                    f"Тикет: <code>#{ticket_id}</code>\n"
                    f"Пользователь: <code>{ticket.get('user_id')}</code>\n"
                    f"Статус: <b>{ticket.get('status') or '-'}</b>\n"
                    f"Последнее обновление: <code>{updated_at or '-'}</code>"
                )
                target_admins = []
                assigned_admin_id = int(ticket.get("assigned_admin_id") or 0)
                if assigned_admin_id > 0:
                    target_admins = [assigned_admin_id]
                else:
                    target_admins = list(Config.ADMIN_USER_IDS)
                for admin_id in target_admins:
                    try:
                        await ctx.bot.send_message(admin_id, text, parse_mode="HTML")
                    except Exception:
                        continue
                await ctx.db.set_support_ticket_reminder_state(ticket_id, updated_at)
            await asyncio.sleep(max(300, Config.SUPPORT_TICKET_REMINDER_INTERVAL_MIN * 60))
        except Exception as exc:
            logger.error("Support reminder job failed: %s", exc)
            await asyncio.sleep(600)


async def cleanup_transient_messages_job(ctx: BackgroundContext) -> None:
    while True:
        try:
            expired = await ctx.db.get_expired_transient_messages(limit=100)
            for row in expired:
                try:
                    await ctx.bot.delete_message(int(row["chat_id"]), int(row["message_id"]))
                except Exception:
                    pass
                finally:
                    await ctx.db.delete_transient_message_record(int(row["id"]))
            await asyncio.sleep(Config.SERVICE_MESSAGE_CLEANUP_INTERVAL_SEC)
        except Exception as exc:
            logger.error("Transient message cleanup failed: %s", exc)
            await asyncio.sleep(600)


async def daily_admin_report_job(ctx: BackgroundContext) -> None:
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            last_sent_date = str(await ctx.db.get_setting("system:last_daily_admin_report_date", "") or "")
            if _should_send_daily_report(now_utc, last_sent_date):
                from handlers.admin_analytics_helpers import _build_daily_report_detail
                from services.health import format_health_text
                from utils.helpers import notify_admins, register_transient_message

                daily_report = await _build_daily_report_detail(ctx.db, days_ago=0)
                support = await ctx.db.get_support_daily_report(days_ago=0)
                health = await collect_health_snapshot(ctx.db, ctx.panel, ctx.payment_gateway)
                health_text = await format_health_text(health)
                text = (
                    "🗓 <b>Автоотчёт за сегодня</b>\n\n"
                    f"{daily_report}\n\n"
                    "<b>Поддержка</b>\n"
                    f"🆕 Новых тикетов: <b>{support.get('opened_tickets', 0)}</b>\n"
                    f"✅ Закрыто/архивировано: <b>{support.get('closed_tickets', 0)}</b>\n"
                    f"💬 Сообщений от пользователей: <b>{support.get('messages_from_users', 0)}</b>\n"
                    f"🛠 Ответов от админов: <b>{support.get('messages_from_admins', 0)}</b>\n"
                    f"📌 Сейчас открыто: <b>{support.get('open_tickets', 0)}</b>\n\n"
                    f"{health_text}"
                )
                for admin_id in Config.ADMIN_USER_IDS:
                    msg = await ctx.bot.send_message(admin_id, text, parse_mode="HTML")
                    await register_transient_message(
                        db=ctx.db,
                        chat_id=admin_id,
                        message_id=msg.message_id,
                        category="daily_admin_report",
                        ttl_hours=72,
                    )
                await ctx.db.set_setting("system:last_daily_admin_report_date", now_utc.date().isoformat())
            await asyncio.sleep(300)
        except Exception as exc:
            logger.error("Daily admin report job failed: %s", exc)
            await asyncio.sleep(900)


async def daily_incident_report_job(ctx: BackgroundContext) -> None:
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            last_sent_date = str(await ctx.db.get_setting("system:last_daily_incident_report_date", "") or "")
            if _should_send_incident_report(now_utc, last_sent_date):
                from services.health import format_health_text
                from utils.helpers import register_transient_message

                incidents = await ctx.db.get_daily_incident_report(days_ago=0)
                health = await collect_health_snapshot(ctx.db, ctx.panel, ctx.payment_gateway)
                health_text = await format_health_text(health)
                schema_issues = await ctx.db.get_schema_drift_issues() if hasattr(ctx.db, "get_schema_drift_issues") else []
                safe_mode_enabled = str(await ctx.db.get_setting("system:safe_mode", "0") or "0") == "1"
                safe_mode_reason = str(await ctx.db.get_setting("system:safe_mode_reason", "") or "")
                text = (
                    "🚨 <b>Incident-отчёт за сутки</b>\n\n"
                    f"📅 Дата: <code>{incidents.get('report_date', '-')}</code>\n"
                    f"⚠️ Ошибки платежей: <b>{incidents.get('payment_errors', 0)}</b>\n"
                    f"🛡 Срабатывания blacklist поддержки: <b>{incidents.get('support_blacklist_hits', 0)}</b>\n"
                    f"🕒 Stale processing: <b>{incidents.get('stale_processing', 0)}</b>\n"
                    f"⏳ Старые pending: <b>{incidents.get('old_pending', 0)}</b>\n"
                    f"🧯 Safe mode: <b>{'включён' if safe_mode_enabled else 'выключен'}</b>\n"
                    f"📝 Причина safe mode: <code>{safe_mode_reason or '-'}</code>\n"
                    f"🧬 Schema issues: <b>{len(schema_issues)}</b>\n\n"
                    f"{health_text}"
                )
                for admin_id in Config.ADMIN_USER_IDS:
                    msg = await ctx.bot.send_message(admin_id, text, parse_mode="HTML")
                    await register_transient_message(
                        db=ctx.db,
                        chat_id=admin_id,
                        message_id=msg.message_id,
                        category="daily_incident_report",
                        ttl_hours=72,
                    )
                await ctx.db.set_setting("system:last_daily_incident_report_date", now_utc.date().isoformat())
            await asyncio.sleep(300)
        except Exception as exc:
            logger.error("Daily incident report job failed: %s", exc)
            await asyncio.sleep(900)


async def remind_unpaid_referrals(ctx: BackgroundContext) -> None:
    from utils.helpers import notify_user

    while True:
        try:
            await asyncio.sleep(3600)
            users = await ctx.db.get_all_users()
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            for user in users:
                if user.get("ref_by") and not user.get("ref_rewarded") and not user.get("has_subscription"):
                    joined = user.get("join_date")
                    if joined:
                        join_dt = _parse_sqlite_ts(joined)
                        if not join_dt:
                            logger.debug("Referral reminder skipped: invalid join_date for %s: %r", user.get("user_id"), joined)
                            continue
                        diff = (now - join_dt).total_seconds()
                        if 86400 <= diff <= 90000:
                            await notify_user(
                                user["user_id"],
                                "👋 Привет! Вы пришли по реферальной ссылке.\n\n"
                                "Купите подписку и получите бонусные дни! 🎁\n"
                                "Нажмите /start чтобы начать.",
                            )
        except Exception as exc:
            logger.error("Referral reminder job failed: %s", exc)
            await asyncio.sleep(3600)


async def remind_abandoned_payments_job(ctx: BackgroundContext) -> None:
    from utils.helpers import notify_user

    stages = [
        ("20m", "💳 <b>Ваш доступ почти готов</b>\n\nПлатёж на <b>{amount:.2f} ₽</b>{provider_suffix} всё ещё ждёт подтверждения.\n\nЗавершите оплату сейчас, и подписка активируется автоматически.", 20 / 60),
        ("12h", "⏳ <b>Мы сохранили ваш платёж</b>\n\nПлатёж на <b>{amount:.2f} ₽</b>{provider_suffix} всё ещё можно завершить.\n\nЕсли VPN нужен, вернитесь к оплате по кнопке ниже.", 12),
        ("24h", "🔥 <b>Доступ всё ещё можно активировать</b>\n\nВаш платёж на <b>{amount:.2f} ₽</b>{provider_suffix} остался незавершённым.\n\nЗавершите его, и бот сразу выдаст рабочее подключение.", 24),
    ]
    kb_fallback = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Продолжить оплату", callback_data="open_buy_menu")],
            [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="user_menu:profile")],
        ]
    )

    while True:
        try:
            old_pending = await ctx.db.get_old_pending_payments(Config.ABANDONED_PAYMENT_REMINDER_AFTER_MIN)
            for payment in old_pending:
                payment_id = str(payment.get("payment_id") or "").strip()
                user_id = int(payment.get("user_id") or 0)
                if not payment_id or user_id <= 0:
                    continue
                created_hours = _hours_since(payment.get("created_at"))
                if created_hours is None:
                    continue
                checkout_url = await _resolve_payment_checkout_url(payment, ctx.payment_gateway)
                markup = kb_fallback
                if checkout_url:
                    markup = InlineKeyboardMarkup(
                        inline_keyboard=[
                            [InlineKeyboardButton(text="💳 Продолжить оплату", url=checkout_url)],
                            [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="user_menu:profile")],
                        ]
                    )
                amount = float(payment.get("amount") or 0.0)
                provider = str(payment.get("provider") or "").strip()
                provider_suffix = f" через <b>{provider}</b>" if provider else ""
                for stage_id, template, min_hours in stages:
                    if created_hours < min_hours:
                        continue
                    key = _stage_setting_key("payments:abandoned_reminder", payment_id, stage_id)
                    if not await _setting_ts_due(
                        ctx.db,
                        key,
                        repeat_hours=Config.ABANDONED_PAYMENT_REMINDER_REPEAT_HOURS * 10,
                    ):
                        continue
                    text = template.format(amount=amount, provider_suffix=provider_suffix)
                    await notify_user(user_id, text, reply_markup=markup, bot=ctx.bot)
                    await _mark_setting_ts(ctx.db, key)
                    await _increment_setting_counter(ctx.db, f"analytics:funnel:abandoned_payment:{stage_id}:sent")
            await asyncio.sleep(max(900, Config.ABANDONED_PAYMENT_REMINDER_AFTER_MIN * 60))
        except Exception as exc:
            logger.error("Abandoned payment reminder job failed: %s", exc)
            await asyncio.sleep(900)


async def reactivate_inactive_users_job(ctx: BackgroundContext) -> None:
    from utils.helpers import notify_user
    from tariffs import get_minimal_by_price

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оформить подписку", callback_data="open_buy_menu")],
            [InlineKeyboardButton(text="⚡ Как подключиться", callback_data="onboarding:start")],
        ]
    )

    while True:
        try:
            minimal_plan = get_minimal_by_price() or {}
            minimal_price = float(minimal_plan.get("price_rub") or 0.0)
            offer_line = f"\n\n💡 Оптимальный старт сейчас: <b>от {minimal_price:.0f} ₽</b>." if minimal_price > 0 else ""
            expired_stages = [
                ("12h", 12, None, 0, "⌛ <b>Подписка закончилась</b>\n\nПродлите её сейчас, и доступ вернётся сразу, без новой настройки и поиска ссылки." + offer_line),
                ("3d", 72, "12h", 24, "👋 <b>Можно вернуться за минуту</b>\n\nПодписка закончилась, но бот уже сохранил ваше подключение. Продлите её и снова пользуйтесь VPN без лишних действий." + offer_line),
                ("7d", 168, "3d", 48, "🔥 <b>VPN всё ещё ждёт вас</b>\n\nЕсли доступ снова нужен, просто вернитесь к продлению. Настройка уже готова, останется только оплатить." + offer_line),
            ]
            users = await ctx.db.get_users_for_broadcast_segment("inactive")
            for user in users:
                user_id = int(user.get("user_id") or 0)
                if user_id <= 0:
                    continue
                if bool(user.get("has_subscription")):
                    continue
                base_hours = _hours_since(user.get("expiry_date")) or _hours_since(user.get("join_date"))
                if base_hours is None or base_hours < Config.INACTIVE_USER_REACTIVATION_AFTER_HOURS:
                    continue
                payments = await ctx.db.get_pending_payments_by_user(user_id) if hasattr(ctx.db, "get_pending_payments_by_user") else []
                if any(str(item.get("status") or "").strip().lower() in {"pending", "processing", "waiting_for_capture"} for item in payments):
                    continue
                status = await ctx.db.get_user(user_id)
                if bool((status or {}).get("trial_used")) and not bool((status or {}).get("has_subscription")):
                    if base_hours < max(72, Config.INACTIVE_USER_REACTIVATION_AFTER_HOURS):
                        continue
                    promo_code = await _ensure_trial_recovery_promo(ctx.db, user_id)
                    key = _stage_setting_key("marketing:inactive_reactivation", user_id, "trial_offer")
                    if await _setting_ts_due(ctx.db, key, repeat_hours=Config.INACTIVE_USER_REACTIVATION_REPEAT_HOURS * 10):
                        await notify_user(
                            user_id,
                            "🎁 <b>Для вас сохранили персональный оффер</b>\n\n"
                            "Пробный период уже закончился, но вы можете вернуться со скидкой.\n"
                            f"Ваш промокод: <code>{promo_code}</code>\n"
                            f"Скидка: <b>{float(Config.TRIAL_RECOVERY_PROMO_PERCENT or 15.0):.0f}%</b>\n\n"
                            "Промокод уже привязан в боте, можно просто перейти к тарифам.",
                            reply_markup=kb,
                            bot=ctx.bot,
                        )
                        await _mark_setting_ts(ctx.db, key)
                        await _increment_setting_counter(ctx.db, "analytics:funnel:trial:promo_offer:sent")
                    continue
                for stage_id, min_hours, required_prev_stage, min_gap_hours, text in expired_stages:
                    if base_hours < min_hours:
                        continue
                    key = _stage_setting_key("marketing:inactive_reactivation", user_id, stage_id)
                    if not await _setting_ts_due(ctx.db, key, repeat_hours=Config.INACTIVE_USER_REACTIVATION_REPEAT_HOURS * 10):
                        continue
                    if required_prev_stage:
                        previous_key = _stage_setting_key("marketing:inactive_reactivation", user_id, required_prev_stage)
                        previous_age = await _setting_age_hours(ctx.db, previous_key)
                        if previous_age is None or previous_age < min_gap_hours:
                            break
                    await notify_user(user_id, text, reply_markup=kb, bot=ctx.bot)
                    await _mark_setting_ts(ctx.db, key)
                    await _increment_setting_counter(ctx.db, f"analytics:funnel:reactivation:{stage_id}:sent")
                    break
            await asyncio.sleep(6 * 3600)
        except Exception as exc:
            logger.error("Inactive user reactivation job failed: %s", exc)
            await asyncio.sleep(1800)


async def remind_unclaimed_gift_links_job(ctx: BackgroundContext) -> None:
    from utils.helpers import notify_user, notify_admins

    while True:
        try:
            expired_deleted = 0
            if hasattr(ctx.db, "cleanup_expired_admin_gift_links"):
                expired_deleted = await ctx.db.cleanup_expired_admin_gift_links(
                    days=getattr(Config, "ADMIN_GIFT_EXPIRE_DAYS", 3),
                    buyer_user_id=getattr(Config, "ADMIN_GIFT_REFERRER_ID", 794419497),
                )
            if expired_deleted:
                logger.info("Expired admin gift links deleted: %s", expired_deleted)
            rows = await ctx.db.list_unclaimed_gift_links_for_reminder(
                hours=Config.GIFT_LINK_REMINDER_AFTER_HOURS,
                limit=20,
            )
            for row in rows:
                token = str(row.get("token") or "")
                buyer_user_id = int(row.get("buyer_user_id") or 0)
                if not token or buyer_user_id <= 0:
                    continue
                username = (getattr(Config, "BOT_PUBLIC_USERNAME", "") or "").strip() or getattr(ctx.bot, "username", "") or ""
                if username:
                    gift_link = f"https://t.me/{username}?start=gift_{token}"
                else:
                    gift_link = f"https://t.me/?start=gift_{token}"
                plan_id = str(row.get("plan_id") or "")
                note = str(row.get("note") or "").strip()
                text = (
                    "🎁 <b>Напоминание о подарке</b>\n\n"
                    f"Ваш подарочный тариф <b>{plan_id}</b> ещё не активирован.\n"
                    "Вы можете снова отправить ссылку получателю:\n\n"
                    f"<code>{gift_link}</code>"
                )
                if note:
                    text += f"\n\n✍️ Подпись: <i>{note[:180]}</i>"
                await notify_user(buyer_user_id, text, bot=ctx.bot)
                await ctx.db.touch_gift_link_reminder(token)
                await notify_admins(
                    f"🎁 <b>Отправлено напоминание по подарку</b>\n"
                    f"🧾 Покупатель: <code>{buyer_user_id}</code>\n"
                    f"🔗 Токен: <code>{token}</code>",
                    bot=ctx.bot,
                )
            await asyncio.sleep(max(3600, Config.GIFT_LINK_REMINDER_INTERVAL_HOURS * 3600))
        except Exception as exc:
            logger.error("Gift reminder job failed: %s", exc)
            await asyncio.sleep(3600)


async def check_expiry_notifications(ctx: BackgroundContext) -> None:
    from tariffs import get_by_id, get_minimal_by_price, is_trial_plan
    from kkbot.services.subscriptions import get_subscription_status
    from utils.helpers import notify_user

    kb_renew = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Продлить подписку", callback_data="open_buy_menu")],
            [InlineKeyboardButton(text="📦 Моя подписка", callback_data="back_to_subscriptions")],
        ]
    )
    kb_buy_after_trial = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Выбрать тариф", callback_data="open_buy_menu")],
            [InlineKeyboardButton(text="⚡ Как подключиться", callback_data="onboarding:start")],
        ]
    )
    kb_trial_survey = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👍 Всё нравится", callback_data="trial:feedback:ok")],
            [InlineKeyboardButton(text="💸 Дороговато", callback_data="trial:feedback:price")],
            [InlineKeyboardButton(text="😕 Не разобрался", callback_data="trial:feedback:setup")],
            [InlineKeyboardButton(text="🆘 Нужна помощь", callback_data="support:start")],
        ]
    )

    while True:
        try:
            await asyncio.sleep(1800)
            users = await ctx.db.get_all_subscribers()
            minimal_plan = get_minimal_by_price() or {}
            minimal_price = float(minimal_plan.get("price_rub") or 0.0)
            offer_line = f"\n\n💡 Подойдёт стартовый тариф от <b>{minimal_price:.0f} ₽</b>." if minimal_price > 0 else ""

            for user in users:
                uid = user["user_id"]
                status = await get_subscription_status(uid, db=ctx.db, panel=ctx.panel)
                if status.get("is_frozen"):
                    continue

                expiry_dt = status.get("expiry_dt")
                if not expiry_dt:
                    continue

                plan_code = str((status.get("user") or {}).get("plan_text") or "").strip()
                current_plan = get_by_id(plan_code) if plan_code else None
                is_trial = is_trial_plan(current_plan) or plan_code.lower() == "trial"
                if is_trial:
                    trial_hours = _hours_since(user.get("join_date"))
                    if (
                        trial_hours is not None
                        and trial_hours >= 24
                        and await _setting_ts_due(ctx.db, _trial_followup_key(uid), repeat_hours=24 * 365)
                    ):
                        await notify_user(
                            uid,
                            "✨ <b>Как вам пробный тариф?</b>\n\n"
                            "Если всё работает как нужно, можно заранее выбрать платный тариф и продолжить пользоваться VPN без паузы после окончания пробного периода.\n\n"
                            "А если что-то мешает, просто нажмите подходящую кнопку ниже.",
                            reply_markup=kb_trial_survey,
                            bot=ctx.bot,
                        )
                        await _mark_setting_ts(ctx.db, _trial_followup_key(uid))
                        await _increment_setting_counter(ctx.db, "analytics:funnel:trial:day1_followup:sent")
                diff_sec = expiry_dt.timestamp() - time.time()
                if diff_sec <= 0:
                    expired_hours = abs(diff_sec) / 3600.0
                    if (
                        expired_hours >= Config.EXPIRED_SUBSCRIPTION_REACTIVATION_AFTER_HOURS
                        and await _setting_ts_due(
                            ctx.db,
                            _expired_reactivation_key(uid),
                            repeat_hours=Config.EXPIRED_SUBSCRIPTION_REACTIVATION_REPEAT_HOURS,
                        )
                    ):
                        await notify_user(
                            uid,
                            (
                                "🎁 <b>Пробный тариф закончился</b>\n\n"
                                "Чтобы продолжить пользоваться VPN, выберите платный тариф.\n"
                                "Подключение уже настроено, останется только оплатить."
                                + offer_line
                                if is_trial
                                else "⌛ <b>Подписка закончилась</b>\n\n"
                                "Продлите её сейчас, и доступ вернётся сразу, без новой настройки и поиска ссылки."
                            ),
                            reply_markup=kb_buy_after_trial if is_trial else kb_renew,
                            bot=ctx.bot,
                        )
                        await _mark_setting_ts(ctx.db, _expired_reactivation_key(uid))
                        await _increment_setting_counter(
                            ctx.db,
                            "analytics:funnel:trial:expired:sent" if is_trial else "analytics:funnel:renewal:expired:sent",
                        )
                    continue

                if not status.get("active"):
                    continue

                if diff_sec <= 3600 and not user.get("notified_1h"):
                    await notify_user(
                        uid,
                        (
                            "⏰ <b>До конца пробного тарифа остался 1 час</b>\n\n"
                            "Чтобы VPN продолжил работать без перерыва, выберите тариф заранее."
                            + offer_line
                            if is_trial
                            else "⏰ <b>До окончания подписки остался 1 час</b>\n\n"
                            "Продлите сейчас, чтобы не потерять доступ и не отвлекаться потом на повторное подключение."
                        ),
                        reply_markup=kb_buy_after_trial if is_trial else kb_renew,
                    )
                    await ctx.db.update_user(uid, notified_1h=1)
                    await _increment_setting_counter(
                        ctx.db,
                        "analytics:funnel:trial:1h:sent" if is_trial else "analytics:funnel:renewal:1h:sent",
                    )
                elif diff_sec <= 86400 and not user.get("notified_1d"):
                    await notify_user(
                        uid,
                        (
                            "⚠️ <b>Пробный тариф закончится через 1 день</b>\n\n"
                            "Чтобы продолжить пользоваться VPN после пробного периода, выберите платный тариф."
                            + offer_line
                            if is_trial
                            else "⚠️ <b>До окончания подписки остался 1 день</b>\n\n"
                            "Продлите заранее: доступ сохранится, а оставшиеся дни не сгорят."
                        ),
                        reply_markup=kb_buy_after_trial if is_trial else kb_renew,
                    )
                    await ctx.db.update_user(uid, notified_1d=1)
                    await _increment_setting_counter(
                        ctx.db,
                        "analytics:funnel:trial:1d:sent" if is_trial else "analytics:funnel:renewal:1d:sent",
                    )
                elif diff_sec <= 259200 and not user.get("notified_3d"):
                    await notify_user(
                        uid,
                        (
                            "📅 <b>До конца пробного тарифа осталось 3 дня</b>\n\n"
                            "Если VPN вам подошёл, можно заранее выбрать тариф и продолжить пользоваться без паузы."
                            + offer_line
                            if is_trial
                            else "📅 <b>До окончания подписки осталось 3 дня</b>\n\n"
                            "Можно продлить уже сейчас и спокойно пользоваться дальше без риска остаться без доступа в неудобный момент."
                        ),
                        reply_markup=kb_buy_after_trial if is_trial else kb_renew,
                    )
                    await ctx.db.update_user(uid, notified_3d=1)
                    await _increment_setting_counter(
                        ctx.db,
                        "analytics:funnel:trial:3d:sent" if is_trial else "analytics:funnel:renewal:3d:sent",
                    )

        except Exception as exc:
            logger.error("Expiry notification job failed: %s", exc)
            await asyncio.sleep(1800)



async def auto_resolve_payment_attention_job(ctx: BackgroundContext) -> None:
    from services.payment_attention_resolver import auto_resolve_payment_attention

    while True:
        try:
            summary = await auto_resolve_payment_attention(
                db=ctx.db,
                panel=ctx.panel,
                payment_gateway=ctx.payment_gateway,
                bot=ctx.bot,
                provider="all",
                issue_type="all",
                limit=Config.PAYMENT_ATTENTION_RESOLVE_LIMIT,
            )
            if summary.get("total_resolved"):
                logger.warning("Payment attention auto-resolved: resolved=%s skipped=%s", summary.get("total_resolved"), summary.get("total_skipped"))
            await asyncio.sleep(Config.PAYMENT_ATTENTION_RESOLVE_INTERVAL_SEC)
        except Exception as exc:
            logger.error("Payment attention resolver job failed: %s", exc)
            await asyncio.sleep(120)


async def sync_cidr_config_to_object_storage_job(ctx: BackgroundContext) -> None:
    from services.cidr_object_storage_sync import sync_cidr_config_to_object_storage

    while True:
        try:
            result = await sync_cidr_config_to_object_storage()
            logger.info(
                "CIDR object storage sync complete: lines=%s sources=%s url=%s",
                result.get("lines"),
                result.get("sources"),
                result.get("object_url"),
            )
            await asyncio.sleep(max(900, Config.CIDR_OBJECT_STORAGE_SYNC_INTERVAL_SEC))
        except Exception as exc:
            logger.error("CIDR object storage sync job failed: %s", exc)
            await asyncio.sleep(600)


def build_job_specs() -> list[JobSpec]:
    settings = Config.jobs_settings()
    return [
        JobSpec("check_expired_subscriptions", check_expired_subscriptions, settings.enable_expired_subscriptions_job),
        JobSpec("cleanup_old_payments", cleanup_old_payments, settings.enable_cleanup_payments_job),
        JobSpec("reconcile_itpay_payments", reconcile_provider_payments, settings.enable_payment_reconcile_job),
        JobSpec("recover_stuck_processing_payments", recover_stuck_processing_payments, settings.enable_stale_payment_recovery_job),
        JobSpec("remind_unpaid_referrals", remind_unpaid_referrals, settings.enable_referral_reminder_job),
        JobSpec("remind_unclaimed_gift_links_job", remind_unclaimed_gift_links_job, True),
        JobSpec("check_expiry_notifications", check_expiry_notifications, settings.enable_expiry_notifications_job),
        JobSpec("remind_abandoned_payments_job", remind_abandoned_payments_job, settings.enable_abandoned_payment_reminder_job),
        JobSpec("reactivate_inactive_users_job", reactivate_inactive_users_job, settings.enable_inactive_user_reactivation_job),
        JobSpec("health_monitor", health_monitor, settings.enable_health_monitor_job),
        JobSpec("auto_resolve_payment_attention_job", auto_resolve_payment_attention_job, getattr(settings, "enable_payment_attention_resolver_job", True)),
        JobSpec("sync_cidr_config_to_object_storage_job", sync_cidr_config_to_object_storage_job, getattr(settings, "enable_cidr_object_storage_sync_job", True)),
        JobSpec("archive_closed_support_tickets_job", archive_closed_support_tickets_job, True),
        JobSpec("remind_stale_support_tickets_job", remind_stale_support_tickets_job, True),
        JobSpec("cleanup_transient_messages_job", cleanup_transient_messages_job, True),
        JobSpec("daily_admin_report_job", daily_admin_report_job, True),
        JobSpec("daily_incident_report_job", daily_incident_report_job, True),
    ]



def start_background_tasks(ctx: BackgroundContext) -> list[asyncio.Task]:
    tasks: list[asyncio.Task] = []
    for job in build_job_specs():
        if not job.enabled:
            logger.info("Background job disabled: %s", job.name)
            continue
        tasks.append(create_background_task(job.factory(ctx), name=job.name))
    return tasks


# Backward-compatible alias for older imports/tests
reconcile_itpay_payments = reconcile_provider_payments
