import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from aiogram import Bot

from config import Config
from db import Database
from services.panel import PanelAPI
from services.traffic_state import check_lte_report_api_health, load_total_traffic_state
from utils.helpers import notify_admins

logger = logging.getLogger(__name__)


@dataclass
class HealthAlertState:
    last_sent: dict[str, float]

    def __init__(self) -> None:
        self.last_sent = {}

    def should_send(self, key: str) -> bool:
        now = time.time()
        last = self.last_sent.get(key, 0.0)
        if now - last >= Config.HEALTH_ALERT_COOLDOWN_SEC:
            self.last_sent[key] = now
            return True
        return False


async def collect_health_snapshot(db: Database, panel: PanelAPI, payment_gateway: object) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {
        "database": False,
        "panel": False,
        "lte_api": False,
        "traffic_state": False,
        "payment_provider": False,
        "schema_version": 0,
        "processing_count": 0,
        "old_pending_count": 0,
        "payment_error_count": 0,
    }

    try:
        snapshot["database"] = (await db.get_total_users()) >= 0
        snapshot["schema_version"] = await db.get_schema_version()
        snapshot["processing_count"] = await db.get_processing_payments_count()
        snapshot["old_pending_count"] = len(await db.get_old_pending_payments(Config.HEALTH_PENDING_AGE_MIN))
        snapshot["payment_error_count"] = len(await db.get_recent_payment_errors(24))
    except Exception as exc:
        logger.error("collect_health_snapshot database failed: %s", exc)

    try:
        inbounds = await panel.get_inbounds()
        snapshot["panel"] = bool(inbounds and inbounds.get("success"))
    except Exception as exc:
        logger.error("collect_health_snapshot panel failed: %s", exc)

    try:
        snapshot["lte_api"] = await check_lte_report_api_health()
    except Exception as exc:
        logger.error("collect_health_snapshot lte_api failed: %s", exc)

    try:
        state, source, fresh = await load_total_traffic_state()
        snapshot["traffic_state"] = bool(state and fresh)
        if source:
            snapshot["traffic_state_source"] = source
    except Exception as exc:
        logger.error("collect_health_snapshot traffic_state failed: %s", exc)

    try:
        session = await payment_gateway._get_session()  # noqa: SLF001
        snapshot["payment_provider"] = bool(session and not session.closed)
    except Exception as exc:
        logger.error("collect_health_snapshot payment provider failed: %s", exc)

    snapshot["ok"] = bool(
        snapshot["database"]
        and snapshot["panel"]
        and snapshot["lte_api"]
        and snapshot["traffic_state"]
        and snapshot["payment_provider"]
        and snapshot["processing_count"] <= Config.HEALTH_MAX_PROCESSING
    )
    return snapshot


async def format_health_text(snapshot: Dict[str, Any]) -> str:
    db_status = "OK" if snapshot.get("database") else "FAIL"
    panel_status = "OK" if snapshot.get("panel") else "FAIL"
    lte_api_status = "OK" if snapshot.get("lte_api") else "FAIL"
    traffic_state_status = "OK" if snapshot.get("traffic_state") else "FAIL"
    provider_name = getattr(Config, "PAYMENT_PROVIDER", "payment").upper()
    itpay_status = "OK" if snapshot.get("payment_provider") else "FAIL"
    problems = []
    if snapshot.get("processing_count", 0) > Config.HEALTH_MAX_PROCESSING:
        problems.append(f"слишком много processing: {snapshot['processing_count']}")
    if snapshot.get("old_pending_count", 0) > 0:
        problems.append(f"pending старше {Config.HEALTH_PENDING_AGE_MIN} мин: {snapshot['old_pending_count']}")
    if snapshot.get("payment_error_count", 0) > 0:
        problems.append(f"ошибки активации за 24ч: {snapshot['payment_error_count']}")
    problems_text = "\n".join(f"• {item}" for item in problems) if problems else "• критичных проблем не найдено"
    return (
        "🩺 <b>Состояние системы</b>\n\n"
        f"БД: <b>{db_status}</b>\n"
        f"Panel: <b>{panel_status}</b>\n"
        f"LTE API: <b>{lte_api_status}</b>\n"
        f"Traffic state: <b>{traffic_state_status}</b>\n"
        f"{provider_name}: <b>{itpay_status}</b>\n"
        f"Schema version: <code>{snapshot.get('schema_version', 0)}</code>\n"
        f"Processing payments: <b>{snapshot.get('processing_count', 0)}</b>\n"
        f"Pending > {Config.HEALTH_PENDING_AGE_MIN} мин: <b>{snapshot.get('old_pending_count', 0)}</b>\n"
        f"Ошибки активации за 24ч: <b>{snapshot.get('payment_error_count', 0)}</b>\n\n"
        f"⚠️ <b>Проблемы</b>\n{problems_text}"
    )


async def emit_health_alerts(
    *,
    snapshot: Dict[str, Any],
    state: HealthAlertState,
    bot: Optional[Bot] = None,
) -> None:
    alert_specs = []
    if not snapshot.get("database"):
        alert_specs.append(("database_down", "⚠️ ALERT\n\nБаза данных недоступна"))
    if not snapshot.get("panel"):
        alert_specs.append(("panel_down", "⚠️ ALERT\n\nPanel недоступна"))
    if not snapshot.get("lte_api"):
        alert_specs.append(("lte_api_down", "⚠️ ALERT\n\nLTE report API недоступен"))
    if not snapshot.get("traffic_state"):
        alert_specs.append(("traffic_state_stale", "⚠️ ALERT\n\nОбщий traffic-state отсутствует или устарел"))
    if not snapshot.get("payment_provider"):
        provider_name = getattr(Config, "PAYMENT_PROVIDER", "payment").upper()
        alert_specs.append(("payment_provider_down", f"⚠️ ALERT\n\n{provider_name} session/API недоступна"))
    if snapshot.get("processing_count", 0) > Config.HEALTH_MAX_PROCESSING:
        alert_specs.append((
            "too_many_processing",
            f"⚠️ ALERT\n\nСлишком много processing-платежей: {snapshot['processing_count']} (лимит {Config.HEALTH_MAX_PROCESSING})",
        ))
    if snapshot.get("old_pending_count", 0) > 0:
        alert_specs.append((
            "old_pending",
            f"⚠️ ALERT\n\nЕсть pending-платежи старше {Config.HEALTH_PENDING_AGE_MIN} минут: {snapshot['old_pending_count']}",
        ))
    if snapshot.get("payment_error_count", 0) > 0:
        alert_specs.append((
            "payment_errors",
            f"⚠️ ALERT\n\nОшибки активации за 24 часа: {snapshot['payment_error_count']}",
        ))

    for key, text in alert_specs:
        if state.should_send(key):
            await notify_admins(text, bot=bot)
