import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable

from aiogram import Bot

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
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    from kkbot.services.subscriptions import get_subscription_status
    from utils.helpers import notify_user

    kb_renew = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Продлить подписку", callback_data="open_buy_menu")],
            [InlineKeyboardButton(text="📦 Моя подписка", callback_data="back_to_subscriptions")],
        ]
    )

    while True:
        try:
            await asyncio.sleep(1800)
            users = await ctx.db.get_all_subscribers()

            for user in users:
                uid = user["user_id"]
                status = await get_subscription_status(uid, db=ctx.db, panel=ctx.panel)
                if not status.get("active") or status.get("is_frozen"):
                    continue

                expiry_dt = status.get("expiry_dt")
                if not expiry_dt:
                    continue

                diff_sec = expiry_dt.timestamp() - time.time()
                if diff_sec <= 0:
                    continue

                if diff_sec <= 3600 and not user.get("notified_1h"):
                    await notify_user(
                        uid,
                        "⏰ <b>До истечения подписки остался 1 час!</b>\n\n"
                        "Нажмите кнопку ниже, чтобы продлить без потери оставшихся дней.",
                        reply_markup=kb_renew,
                    )
                    await ctx.db.update_user(uid, notified_1h=1)
                elif diff_sec <= 86400 and not user.get("notified_1d"):
                    await notify_user(
                        uid,
                        "⚠️ <b>До истечения подписки остался 1 день!</b>\n\n"
                        "Продлите заранее — оставшиеся дни сохранятся.",
                        reply_markup=kb_renew,
                    )
                    await ctx.db.update_user(uid, notified_1d=1)
                elif diff_sec <= 259200 and not user.get("notified_3d"):
                    await notify_user(
                        uid,
                        "📅 <b>До истечения подписки осталось 3 дня.</b>\n\n"
                        "Вы можете продлить прямо сейчас, не теряя текущий остаток.",
                        reply_markup=kb_renew,
                    )
                    await ctx.db.update_user(uid, notified_3d=1)

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
        JobSpec("health_monitor", health_monitor, settings.enable_health_monitor_job),
        JobSpec("auto_resolve_payment_attention_job", auto_resolve_payment_attention_job, getattr(settings, "enable_payment_attention_resolver_job", True)),
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
