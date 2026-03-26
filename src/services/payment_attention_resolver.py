from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any, Dict, Optional

from config import Config

from db import Database
from kkbot.services.payment_flow import process_successful_payment, reject_pending_payment
from kkbot.services.subscriptions import revoke_subscription

logger = logging.getLogger(__name__)

SUCCESS_EVENT_TYPES = {"payment.succeeded", "payment.completed", "payment.pay", "successful_payment"}
FAILED_EVENT_TYPES = {"payment.canceled"}
REFUND_EVENT_TYPES = {"refund.succeeded"}


def _parse_meta_pairs(raw_value: str | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for part in str(raw_value or "").split(";"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip()
        if key:
            result[key] = value.strip()
    return result




def _parse_dt(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    for candidate in (raw.replace("Z", "+00:00"), raw):
        try:
            dt = datetime.fromisoformat(candidate)
            return dt.astimezone(UTC).replace(tzinfo=None) if dt.tzinfo else dt
        except ValueError:
            continue
    return None


def _cooldown_minutes_for_attempt(attempt_number: int) -> int:
    attempt_number = max(1, int(attempt_number))
    base = max(1, int(Config.PAYMENT_ATTENTION_RETRY_BASE_MIN))
    mult = max(1.0, float(Config.PAYMENT_ATTENTION_RETRY_BACKOFF_MULTIPLIER))
    return max(1, int(round(base * (mult ** max(0, attempt_number - 1)))))


async def _retry_gate(db: Database, payment_id: str, action: str, provider: str) -> dict[str, Any]:
    if not hasattr(db, "get_auto_resolve_action_stats"):
        return {"allow": True, "reason": "ok", "attempts": 0, "cooldown_minutes": _cooldown_minutes_for_attempt(1), "next_attempt": 1}
    stats = await db.get_auto_resolve_action_stats(payment_id, action)
    attempts = int(stats.get("attempts") or 0)
    last_created_at = _parse_dt(stats.get("last_created_at"))
    max_attempts = max(1, int(Config.PAYMENT_ATTENTION_RETRY_MAX_ATTEMPTS))
    if attempts >= max_attempts:
        return {
            "allow": False,
            "reason": "max_attempts_reached",
            "attempts": attempts,
            "cooldown_minutes": None,
        }
    next_attempt_no = attempts + 1
    cooldown_minutes = _cooldown_minutes_for_attempt(next_attempt_no)
    if last_created_at is not None:
        available_at = last_created_at + timedelta(minutes=cooldown_minutes)
        now = datetime.now(UTC).replace(tzinfo=None)
        if available_at > now:
            wait_minutes = max(1, int((available_at - now).total_seconds() // 60) + 1)
            return {
                "allow": False,
                "reason": "cooldown_active",
                "attempts": attempts,
                "cooldown_minutes": cooldown_minutes,
                "retry_after_minutes": wait_minutes,
            }
    return {
        "allow": True,
        "reason": "ok",
        "attempts": attempts,
        "cooldown_minutes": cooldown_minutes,
        "next_attempt": next_attempt_no,
    }


async def _guarded_retry(db: Database, payment_id: str, action: str, provider: str) -> dict[str, Any]:
    gate = await _retry_gate(db, payment_id, action, provider)
    if gate.get("allow"):
        return gate
    details = f"attempts={gate.get('attempts', 0)}"
    if gate.get("reason") == "cooldown_active":
        details += f";retry_after_min={gate.get('retry_after_minutes', 0)};cooldown_min={gate.get('cooldown_minutes', 0)}"
    else:
        details += f";max_attempts={Config.PAYMENT_ATTENTION_RETRY_MAX_ATTEMPTS}"
    await _record_action(db, payment_id, action, provider, gate.get("reason", "retry_blocked"), details)
    return gate

async def _record_action(db: Database, payment_id: str, action: str, provider: str, result: str, details: str = "") -> None:
    try:
        await db.add_payment_admin_action(payment_id, 0, action, provider=provider, result=result, details=details)
    except Exception as exc:  # pragma: no cover
        logger.warning("payment attention action log failed payment=%s action=%s: %s", payment_id, action, exc)


async def _resolve_stale_processing(payment: Dict[str, Any], *, db: Database, panel: object, payment_gateway: object, bot=None) -> str:
    payment_id = str(payment.get("payment_id") or "")
    provider = str(payment.get("provider") or "")
    gate = await _guarded_retry(db, payment_id, "attention_auto_resolve_processing", provider)
    if not gate.get("allow"):
        return str(gate.get("reason") or "retry_blocked")
    gateway_provider = str(getattr(payment_gateway, "provider_name", "") or "")
    if provider and gateway_provider and provider != gateway_provider:
        await _record_action(db, payment_id, "attention_auto_resolve_processing", provider, "skipped_other_provider", f"gateway={gateway_provider}")
        return "skipped_other_provider"

    provider_payment_id = payment.get("provider_payment_id") or payment.get("itpay_id") or ""
    remote_payment = None
    if provider_payment_id and hasattr(payment_gateway, "get_payment"):
        try:
            remote_payment = await payment_gateway.get_payment(provider_payment_id)
        except Exception as exc:
            logger.warning("auto_resolve stale processing get_payment failed payment=%s: %s", payment_id, exc)

    if remote_payment and hasattr(payment_gateway, "is_success_status") and payment_gateway.is_success_status(remote_payment):
        released = await db.release_processing_payment(
            payment_id,
            error_text="attention auto-resolver: released for remote success reconcile",
            source="attention/auto_resolver",
            metadata=f"provider={provider};mode=stale_processing_success",
        )
        if not released:
            await _record_action(db, payment_id, "attention_auto_resolve_processing", provider, "release_failed", "remote_success=1")
            return "release_failed"
        refreshed = await db.get_pending_payment(payment_id)
        result = await process_successful_payment(
            payment=refreshed or payment,
            db=db,
            panel=panel,
            bot=bot,
            admin_context="attention auto-resolver stale processing remote success",
        )
        status = "accepted" if result.get("ok") else f"activation_failed:{result.get('reason', 'unknown')}"
        await _record_action(db, payment_id, "attention_auto_resolve_processing", provider, status, f"remote_status={payment_gateway.extract_status(remote_payment)}")
        return status

    if remote_payment and hasattr(payment_gateway, "is_failed_status") and payment_gateway.is_failed_status(remote_payment):
        result = await reject_pending_payment(
            payment=payment,
            db=db,
            bot=bot,
            admin_context="attention auto-resolver stale processing remote failed",
        )
        status = "rejected" if result.get("ok") else f"reject_failed:{result.get('reason', 'unknown')}"
        await _record_action(db, payment_id, "attention_auto_resolve_processing", provider, status, f"remote_status={payment_gateway.extract_status(remote_payment)}")
        return status

    released = await db.release_processing_payment(
        payment_id,
        error_text="attention auto-resolver released stale processing lock",
        source="attention/auto_resolver",
        metadata=f"provider={provider};mode=stale_processing_release",
    )
    result = "released_to_pending" if released else "release_failed"
    await _record_action(db, payment_id, "attention_auto_resolve_processing", provider, result)
    return result


async def _confirm_refund_for_payment(payment: Dict[str, Any], *, db: Database, panel: object, refund_id: str, provider: str, source: str) -> str:
    payment_id = str(payment.get("payment_id") or "")
    if payment.get("status") != "accepted":
        await _record_action(db, payment_id, "attention_auto_resolve_refund", provider, "skipped_non_accepted", f"status={payment.get('status')}")
        return "skipped_non_accepted"
    updated = await db.update_payment_status(
        payment_id,
        "refunded",
        allowed_current_statuses=["accepted"],
        source=source,
        reason=f"refund_id={refund_id}",
        metadata=f"provider={provider}",
    )
    revoked = False
    if updated:
        revoked = await revoke_subscription(
            int(payment["user_id"]),
            db=db,
            panel=panel,
            reason=f"Подтверждённый refund {provider}",
        )
    await db.record_payment_status_transition(
        payment_id,
        from_status=payment.get("status"),
        to_status="refund_succeeded",
        source=source,
        reason=f"refund_id={refund_id}",
        metadata=f"local_refunded={int(updated)};subscription_revoked={int(revoked)}",
    )
    status = "refunded" if updated else "refund_update_failed"
    await _record_action(db, payment_id, "attention_auto_resolve_refund", provider, status, f"refund_id={refund_id};subscription_revoked={int(revoked)}")
    return status


async def _resolve_operation(item: Dict[str, Any], *, db: Database, panel: object, payment_gateway: object, bot=None) -> str:
    payment_id = str(item.get("payment_id") or "")
    provider = str(item.get("provider") or "")
    gate = await _guarded_retry(db, payment_id, "attention_auto_resolve_operation", provider)
    if not gate.get("allow"):
        return str(gate.get("reason") or "retry_blocked")
    payment = await db.get_pending_payment(payment_id)
    if not payment:
        return "missing_payment"

    requested_status = str(item.get("requested_status") or "")
    gateway_provider = str(getattr(payment_gateway, "provider_name", "") or "")
    if provider and gateway_provider and provider != gateway_provider:
        await _record_action(db, payment_id, "attention_auto_resolve_operation", provider, "skipped_other_provider", f"gateway={gateway_provider};requested={requested_status}")
        return "skipped_other_provider"

    provider_payment_id = payment.get("provider_payment_id") or payment.get("itpay_id") or ""
    meta = _parse_meta_pairs(item.get("requested_metadata"))

    if requested_status == "refund_requested" and provider == "yookassa":
        refund_id = meta.get("provider_refund_id") or meta.get("refund_id") or ""
        if not refund_id or not hasattr(payment_gateway, "get_refund"):
            await _record_action(db, payment_id, "attention_auto_resolve_operation", provider, "missing_refund_id", str(item.get("requested_metadata") or "")[:500])
            return "missing_refund_id"
        remote_refund = await payment_gateway.get_refund(refund_id)
        remote_status = str((remote_refund or {}).get("status") or "").lower()
        if remote_refund and remote_status == "succeeded":
            return await _confirm_refund_for_payment(payment, db=db, panel=panel, refund_id=refund_id, provider=provider, source="attention/auto_resolver_refund")
        result = f"refund_still_{remote_status or 'unknown'}"
        await _record_action(db, payment_id, "attention_auto_resolve_operation", provider, result, f"refund_id={refund_id}")
        return result

    if requested_status == "cancel_requested" and provider == "yookassa":
        if not provider_payment_id or not hasattr(payment_gateway, "get_payment"):
            await _record_action(db, payment_id, "attention_auto_resolve_operation", provider, "missing_provider_payment_id")
            return "missing_provider_payment_id"
        remote_payment = await payment_gateway.get_payment(provider_payment_id)
        remote_status = str(payment_gateway.extract_status(remote_payment) if remote_payment else "").lower()
        if remote_payment and remote_status in {"canceled", "cancelled"}:
            updated = await db.update_payment_status(
                payment_id,
                "cancelled",
                allowed_current_statuses=["pending", "processing"],
                source="attention/auto_resolver_cancel",
                reason=f"provider_status={remote_status}",
                metadata=f"provider_payment_id={provider_payment_id}",
            )
            result = "cancelled" if updated else "cancel_update_failed"
            await _record_action(db, payment_id, "attention_auto_resolve_operation", provider, result, f"provider_status={remote_status}")
            return result
        result = f"cancel_still_{remote_status or 'unknown'}"
        await _record_action(db, payment_id, "attention_auto_resolve_operation", provider, result, f"provider_payment_id={provider_payment_id}")
        return result

    await _record_action(db, payment_id, "attention_auto_resolve_operation", provider, "unsupported_operation", requested_status)
    return "unsupported_operation"


async def _resolve_mismatch(item: Dict[str, Any], *, db: Database, panel: object, payment_gateway: object, bot=None) -> str:
    payment_id = str(item.get("payment_id") or "")
    provider = str(item.get("provider") or "")
    gate = await _guarded_retry(db, payment_id, "attention_auto_resolve_mismatch", provider)
    if not gate.get("allow"):
        return str(gate.get("reason") or "retry_blocked")
    payment = await db.get_pending_payment(payment_id)
    if not payment:
        return "missing_payment"

    gateway_provider = str(getattr(payment_gateway, "provider_name", "") or "")
    if provider and gateway_provider and provider != gateway_provider:
        await _record_action(db, payment_id, "attention_auto_resolve_mismatch", provider, "skipped_other_provider", f"gateway={gateway_provider}")
        return "skipped_other_provider"

    event_type = str(item.get("event_type") or "")
    if event_type in SUCCESS_EVENT_TYPES:
        local_status = str(payment.get("status") or "")
        if local_status == "processing":
            await db.release_processing_payment(
                payment_id,
                error_text="attention auto-resolver released processing for confirmed success",
                source="attention/auto_resolver_mismatch",
                metadata=f"event_type={event_type}",
            )
            payment = await db.get_pending_payment(payment_id) or payment
        result = await process_successful_payment(
            payment=payment,
            db=db,
            panel=panel,
            bot=bot,
            admin_context=f"attention auto-resolver mismatch event={event_type}",
        )
        status = "accepted" if result.get("ok") else f"activation_failed:{result.get('reason', 'unknown')}"
        await _record_action(db, payment_id, "attention_auto_resolve_mismatch", provider, status, f"event_type={event_type}")
        return status

    if event_type in FAILED_EVENT_TYPES:
        result = await reject_pending_payment(
            payment=payment,
            db=db,
            bot=bot,
            admin_context=f"attention auto-resolver mismatch event={event_type}",
        )
        status = "rejected" if result.get("ok") or result.get("already_processed") else f"reject_failed:{result.get('reason', 'unknown')}"
        await _record_action(db, payment_id, "attention_auto_resolve_mismatch", provider, status, f"event_type={event_type}")
        return status

    if event_type in REFUND_EVENT_TYPES:
        refund_meta = _parse_meta_pairs(item.get("payload_excerpt"))
        refund_id = refund_meta.get("id") or "webhook-confirmed"
        return await _confirm_refund_for_payment(payment, db=db, panel=panel, refund_id=refund_id, provider=provider, source="attention/auto_resolver_mismatch")

    await _record_action(db, payment_id, "attention_auto_resolve_mismatch", provider, "unsupported_event", event_type)
    return "unsupported_event"


async def auto_resolve_payment_attention(
    *,
    db: Database,
    panel: object,
    payment_gateway: object,
    bot=None,
    provider: str = "all",
    issue_type: str = "all",
    limit: int = 10,
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "processing": {"resolved": 0, "skipped": 0, "results": []},
        "operations": {"resolved": 0, "skipped": 0, "results": []},
        "mismatch": {"resolved": 0, "skipped": 0, "results": []},
    }

    if issue_type in {"all", "processing"}:
        for row in await db.get_stale_processing_payments(minutes=15, limit=limit, provider=provider):
            result = await _resolve_stale_processing(row, db=db, panel=panel, payment_gateway=payment_gateway, bot=bot)
            summary["processing"]["results"].append({"payment_id": row.get("payment_id"), "result": result})
            if result in {"accepted", "rejected", "released_to_pending"}:
                summary["processing"]["resolved"] += 1
            else:
                summary["processing"]["skipped"] += 1

    if issue_type in {"all", "operations"}:
        for row in await db.get_overdue_payment_operations(minutes=20, limit=limit, provider=provider):
            result = await _resolve_operation(row, db=db, panel=panel, payment_gateway=payment_gateway, bot=bot)
            summary["operations"]["results"].append({"payment_id": row.get("payment_id"), "result": result})
            if result in {"refunded", "cancelled"}:
                summary["operations"]["resolved"] += 1
            else:
                summary["operations"]["skipped"] += 1

    if issue_type in {"all", "mismatch"}:
        for row in await db.get_confirmed_payment_status_mismatches(hours=24, limit=limit, provider=provider):
            result = await _resolve_mismatch(row, db=db, panel=panel, payment_gateway=payment_gateway, bot=bot)
            summary["mismatch"]["results"].append({"payment_id": row.get("payment_id"), "result": result})
            if result in {"accepted", "rejected", "refunded"}:
                summary["mismatch"]["resolved"] += 1
            else:
                summary["mismatch"]["skipped"] += 1

    summary["total_resolved"] = sum(int(summary[key]["resolved"]) for key in ("processing", "operations", "mismatch"))
    summary["total_skipped"] = sum(int(summary[key]["skipped"]) for key in ("processing", "operations", "mismatch"))
    return summary
