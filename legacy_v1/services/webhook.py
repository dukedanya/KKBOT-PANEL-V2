import hashlib
import json
import logging
import os
import time

from aiohttp import web

from config import Config
from services.health import collect_health_snapshot
from services.itpay import ItpayAPI
from services.migrations import get_pending_migrations
from services.payment_flow import process_successful_payment, reject_pending_payment
from services.yookassa import YooKassaAPI
from utils.helpers import notify_admins

logger = logging.getLogger(__name__)
PROCESS_STARTED_AT = int(time.time())

BOT_APP_KEY = web.AppKey("bot", object)
DB_APP_KEY = web.AppKey("db", object)
PANEL_APP_KEY = web.AppKey("panel", object)
PAYMENT_GATEWAY_APP_KEY = web.AppKey("payment_gateway", object)
ITPAY_APP_KEY = PAYMENT_GATEWAY_APP_KEY


async def healthcheck_handler(request: web.Request) -> web.Response:
    now = int(time.time())
    return web.json_response(
        {
            "status": "live",
            "mode": Config.APP_MODE,
            "timestamp": now,
            "uptime_sec": max(0, now - PROCESS_STARTED_AT),
        }
    )


async def readiness_handler(request: web.Request) -> web.Response:
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    snapshot = await collect_health_snapshot(
        request.app[DB_APP_KEY],
        request.app[PANEL_APP_KEY],
        request.app[PAYMENT_GATEWAY_APP_KEY],
    )
    db = request.app[DB_APP_KEY]
    schema_issues = await db.get_schema_drift_issues() if hasattr(db, "get_schema_drift_issues") else []
    pending_migrations = await get_pending_migrations(db, base_dir) if hasattr(db, "get_applied_migration_versions") else []
    strict_schema_ok = not schema_issues
    strict_migrations_ok = not pending_migrations

    ready = bool(snapshot.get("ok")) and strict_schema_ok and strict_migrations_ok
    status = 200 if ready else 503
    return web.json_response(
        {
            "status": "ready" if ready else "degraded",
            "mode": Config.APP_MODE,
            "checks": snapshot,
            "schema_issues": schema_issues,
            "pending_migrations": [name for _, name in pending_migrations],
        },
        status=status,
    )


async def _notify_activation_problem(*, bot, payment: dict, reason: str, provider_label: str) -> None:
    payment_id = payment["payment_id"]
    user_id = payment["user_id"]
    plan_id = payment["plan_id"]
    try:
        await bot.send_message(
            user_id,
            "⚠️ Платёж получен, но активация ещё не завершена. Мы уже разбираемся, ничего повторно оплачивать не нужно.",
            parse_mode="HTML",
        )
    except Exception as exc:
        logger.warning("%s webhook: failed to notify user %s: %s", provider_label, user_id, exc)

    await notify_admins(
        f"⚠️ <b>Проблема активации после webhook</b>\n"
        f"💳 Платёж: <code>{payment_id}</code>\n"
        f"👤 Пользователь: <code>{user_id}</code>\n"
        f"📦 План: <b>{plan_id}</b>\n"
        f"🧩 Причина: <code>{reason}</code>\n"
        f"🏦 Провайдер: <b>{provider_label}</b>",
        bot=bot,
    )


async def itpay_webhook_handler(request: web.Request) -> web.Response:
    raw_body = await request.read()
    signature = request.headers.get("itpay-signature", "")
    if Config.ITPAY_WEBHOOK_SECRET:
        if not signature:
            return web.Response(status=403, text="missing signature")
        if not ItpayAPI.verify_webhook_signature(Config.ITPAY_WEBHOOK_SECRET, raw_body, signature):
            return web.Response(status=403, text="invalid signature")

    try:
        body = json.loads(raw_body)
    except json.JSONDecodeError:
        return web.Response(status=400, text="bad json")

    event_type = body.get("type", "")
    data = body.get("data") or {}
    if event_type not in ("payment.pay", "payment.completed"):
        return web.json_response({"status": 0})

    db = request.app[DB_APP_KEY]
    bot = request.app[BOT_APP_KEY]
    panel = request.app[PANEL_APP_KEY]
    itpay_id = data.get("id", "")
    dedup_payment_id = data.get("client_payment_id", "")
    dedup_key = itpay_id or dedup_payment_id or hashlib.sha256(raw_body).hexdigest()[:16]
    event_key = f"itpay:{event_type}:{dedup_key}"
    if not await db.register_payment_event(event_key, payment_id=dedup_payment_id, source="itpay/webhook", event_type=event_type, payload_excerpt=raw_body.decode("utf-8", errors="ignore")[:1000]):
        return web.json_response({"status": 0, "duplicate": True})

    payment = await db.get_pending_payment_by_provider_id("itpay", itpay_id)
    if not payment and dedup_payment_id:
        payment = await db.get_pending_payment(dedup_payment_id)
    if not payment:
        metadata = data.get("metadata") or {}
        if metadata.get("user_id") and metadata.get("plan_id") and dedup_payment_id:
            payment = {
                "payment_id": dedup_payment_id,
                "user_id": int(metadata["user_id"]),
                "plan_id": metadata["plan_id"],
                "amount": float(data.get("amount", 0) or 0),
                "status": "pending",
                "msg_id": None,
                "provider": "itpay",
                "provider_payment_id": itpay_id,
            }
        else:
            return web.json_response({"status": 0})

    if payment.get("status") != "pending":
        return web.json_response({"status": 0})

    result = await process_successful_payment(payment=payment, db=db, panel=panel, bot=bot, admin_context="ITPAY webhook")
    if result.get("ok"):
        return web.json_response({"status": 0})
    if result.get("reason") == "already_processing":
        return web.Response(status=202, text="already processing")
    await _notify_activation_problem(bot=bot, payment=payment, reason=result.get("reason", "unknown_error"), provider_label="ITPAY")
    return web.Response(status=500, text="activation failed")


async def yookassa_webhook_handler(request: web.Request) -> web.Response:
    raw_body = await request.read()
    try:
        body = json.loads(raw_body)
    except json.JSONDecodeError:
        return web.Response(status=400, text="bad json")

    event_type = body.get("event", "")
    if event_type not in ("payment.succeeded", "payment.canceled", "refund.succeeded"):
        return web.Response(status=200, text="ignored")
    obj = body.get("object") or {}
    provider_payment_id = obj.get("id", "")
    metadata = obj.get("metadata") or {}
    payment_id = metadata.get("client_payment_id", "")
    if event_type == "refund.succeeded":
        refund_id = obj.get("id", "")
        original_payment_id = obj.get("payment_id", "")
        dedup_key = refund_id or original_payment_id or hashlib.sha256(raw_body).hexdigest()[:16]
        event_key = f"yookassa:{event_type}:{dedup_key}"
        db = request.app[DB_APP_KEY]
        bot = request.app[BOT_APP_KEY]
        panel = request.app[PANEL_APP_KEY]
        gateway = request.app[PAYMENT_GATEWAY_APP_KEY]
        if not await db.register_payment_event(event_key, payment_id=payment_id, source="yookassa/webhook", event_type=event_type, payload_excerpt=raw_body.decode("utf-8", errors="ignore")[:1000]):
            return web.Response(status=200, text="duplicate")
        if Config.YOOKASSA_ENFORCE_IP_CHECK and not YooKassaAPI.is_allowed_notification_ip(request.remote or ""):
            return web.Response(status=403, text="invalid ip")
        remote_refund = await gateway.get_refund(refund_id) if hasattr(gateway, "get_refund") else None
        if not remote_refund or remote_refund.get("id") != refund_id or str(remote_refund.get("status", "")).lower() != "succeeded":
            return web.Response(status=403, text="verification failed")
        payment = await db.get_pending_payment_by_provider_id("yookassa", original_payment_id)
        if not payment:
            return web.Response(status=200, text="unknown payment")
        local_refunded = False
        subscription_revoked = False
        if payment.get("status") == "accepted":
            local_refunded = await db.update_payment_status(
                payment["payment_id"],
                "refunded",
                allowed_current_statuses=["accepted"],
                source="yookassa/webhook_refund_succeeded",
                reason=f"refund_id={refund_id}",
                metadata=f"original_provider_payment_id={original_payment_id}",
            )
            if local_refunded:
                from services.subscriptions import revoke_subscription
                subscription_revoked = await revoke_subscription(
                    int(payment["user_id"]),
                    db=db,
                    panel=panel,
                    reason="Подтверждённый refund YooKassa",
                )
        await db.record_payment_status_transition(
            payment["payment_id"],
            from_status=payment.get("status"),
            to_status="refund_succeeded",
            source="yookassa/webhook",
            reason=f"refund_id={refund_id}",
            metadata=f"local_refunded={int(local_refunded)};subscription_revoked={int(subscription_revoked)}",
        )
        await db.add_payment_admin_action(
            payment["payment_id"],
            0,
            "yookassa_refund_confirmed",
            provider="yookassa",
            result="ok",
            details=f"refund_id={refund_id};local_refunded={int(local_refunded)};subscription_revoked={int(subscription_revoked)}",
        )
        return web.Response(status=200, text="ok")
    db = request.app[DB_APP_KEY]
    bot = request.app[BOT_APP_KEY]
    panel = request.app[PANEL_APP_KEY]
    gateway = request.app[PAYMENT_GATEWAY_APP_KEY]

    dedup_key = provider_payment_id or payment_id or hashlib.sha256(raw_body).hexdigest()[:16]
    event_key = f"yookassa:{event_type}:{dedup_key}"
    if not await db.register_payment_event(event_key, payment_id=payment_id, source="yookassa/webhook", event_type=event_type, payload_excerpt=raw_body.decode("utf-8", errors="ignore")[:1000]):
        return web.Response(status=200, text="duplicate")

    if Config.YOOKASSA_ENFORCE_IP_CHECK and not YooKassaAPI.is_allowed_notification_ip(request.remote or ""):
        return web.Response(status=403, text="invalid ip")

    remote_payment = await gateway.get_payment(provider_payment_id)
    if not remote_payment or remote_payment.get("id") != provider_payment_id:
        return web.Response(status=403, text="verification failed")
    if gateway.extract_status(remote_payment) != str(obj.get("status", "")).lower():
        return web.Response(status=409, text="stale status")

    payment = await db.get_pending_payment_by_provider_id("yookassa", provider_payment_id)
    if not payment and payment_id:
        payment = await db.get_pending_payment(payment_id)
    if not payment:
        user_id_meta = metadata.get("user_id")
        plan_id_meta = metadata.get("plan_id")
        if user_id_meta and plan_id_meta and payment_id:
            payment = {
                "payment_id": payment_id,
                "user_id": int(user_id_meta),
                "plan_id": plan_id_meta,
                "amount": float((obj.get("amount") or {}).get("value", 0) or 0),
                "status": "pending",
                "msg_id": None,
                "provider": "yookassa",
                "provider_payment_id": provider_payment_id,
            }
        else:
            return web.Response(status=200, text="unknown payment")

    if payment.get("status") != "pending":
        return web.Response(status=200, text="already handled")

    if event_type == "payment.succeeded":
        result = await process_successful_payment(payment=payment, db=db, panel=panel, bot=bot, admin_context="YooKassa webhook")
        if result.get("ok"):
            return web.Response(status=200, text="ok")
        if result.get("reason") == "already_processing":
            return web.Response(status=202, text="already processing")
        await _notify_activation_problem(bot=bot, payment=payment, reason=result.get("reason", "unknown_error"), provider_label="YooKassa")
        return web.Response(status=500, text="activation failed")

    result = await reject_pending_payment(
        payment=payment,
        db=db,
        bot=bot,
        reason_text="❌ <b>Платёж был отменён в YooKassa.</b>",
        admin_context="YooKassa webhook canceled",
    )
    if result.get("ok") or result.get("already_processed"):
        return web.Response(status=200, text="ok")
    return web.Response(status=500, text="reject failed")


def build_webhook_app(bot, db, panel, payment_gateway=None) -> web.Application:
    app = web.Application()
    app[BOT_APP_KEY] = bot
    app[DB_APP_KEY] = db
    app[PANEL_APP_KEY] = panel
    app[PAYMENT_GATEWAY_APP_KEY] = payment_gateway
    app.router.add_post(Config.ITPAY_WEBHOOK_PATH, itpay_webhook_handler)
    app.router.add_post(Config.YOOKASSA_WEBHOOK_PATH, yookassa_webhook_handler)
    if Config.ENABLE_HEALTH_ENDPOINTS:
        app.router.add_get(Config.HEALTHCHECK_PATH, healthcheck_handler)
        app.router.add_get(Config.READINESS_PATH, readiness_handler)
    return app


async def start_webhook_server(bot, db, panel, *, bind_host: str = "0.0.0.0", port: int = 8080, payment_gateway=None) -> web.AppRunner:
    app = build_webhook_app(bot, db, panel, payment_gateway=payment_gateway)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, bind_host, port)
    await site.start()
    logger.info("Webhook server: %s:%s active paths: %s, %s", bind_host, port, Config.ITPAY_WEBHOOK_PATH, Config.YOOKASSA_WEBHOOK_PATH)
    if Config.ENABLE_HEALTH_ENDPOINTS:
        logger.info("Health endpoints: %s, %s", Config.HEALTHCHECK_PATH, Config.READINESS_PATH)
    return runner


async def stop_webhook_server(runner: web.AppRunner | None) -> None:
    if runner is not None:
        await runner.cleanup()
