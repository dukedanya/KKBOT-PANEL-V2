import logging
from dataclasses import dataclass, field
from typing import Any

from config import Config
from services.health import collect_health_snapshot
from services.migrations import get_pending_migrations
from services.server_bootstrap import ensure_server_bundle
from tariffs.loader import get_all_active

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class StartupCheckReport:
    checks: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not any(isinstance(value, dict) and value.get("status") == "fail" for value in self.checks.values())


async def run_startup_checks(*, container, base_dir: str) -> StartupCheckReport:
    report = StartupCheckReport()

    server_bootstrap = await ensure_server_bundle()
    report.checks["server_bootstrap"] = server_bootstrap
    if server_bootstrap.get("status") == "fail":
        raise RuntimeError(
            "Startup checks failed: server bootstrap failed: "
            + str(server_bootstrap.get("reason") or "unknown error")
        )

    plans = get_all_active()
    report.checks["tariffs"] = {
        "status": "ok" if plans else "fail",
        "active_count": len(plans),
    }
    if not plans:
        raise RuntimeError("Startup checks failed: no active tariffs loaded")

    pending_migrations = await get_pending_migrations(container.db, base_dir)
    report.checks["migrations"] = {
        "status": "warn" if pending_migrations else "ok",
        "pending": [name for _, name in pending_migrations],
    }
    if pending_migrations and Config.effective_startup_fail_on_pending_migrations():
        raise RuntimeError(
            "Startup checks failed: pending migrations detected: "
            + ", ".join(name for _, name in pending_migrations)
        )

    schema_issues = []
    if hasattr(container.db, "get_schema_drift_issues"):
        schema_issues = await container.db.get_schema_drift_issues()
    report.checks["schema"] = {
        "status": "fail" if schema_issues else "ok",
        "issues": schema_issues,
    }
    if schema_issues and Config.effective_startup_fail_on_schema_drift():
        raise RuntimeError(
            "Startup checks failed: schema drift detected: "
            + ", ".join(schema_issues)
        )

    if Config.STARTUP_RECOVER_STALE_PROCESSING:
        recovered = await container.db.reclaim_stale_processing_payments(
            timeout_minutes=Config.STALE_PROCESSING_TIMEOUT_MIN
        )
        report.checks["stale_processing_recovery"] = {
            "status": "ok",
            "recovered": recovered,
        }
        if recovered:
            logger.warning("Startup recovered %s stale processing payments", recovered)
    else:
        report.checks["stale_processing_recovery"] = {
            "status": "skipped",
            "recovered": 0,
        }

    snapshot = await collect_health_snapshot(container.db, container.panel, getattr(container, "payment_gateway", getattr(container, "itpay", None)))
    report.checks["dependencies"] = {
        "status": "ok" if snapshot.get("ok") else "warn",
        "snapshot": snapshot,
    }
    return report
