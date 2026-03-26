from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from kkbot.config import BASE_DIR, load_settings
from kkbot.db.legacy_import import import_legacy_sqlite_to_postgres
from kkbot.db.migrations import apply_postgres_migrations
from kkbot.db.postgres import PostgresDatabase
from kkbot.logging import configure_logging


async def async_main() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    logger = logging.getLogger(__name__)

    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is empty")
    if not settings.legacy_sqlite_path:
        raise RuntimeError("LEGACY_SQLITE_PATH is empty")

    db = PostgresDatabase(
        settings.database_url,
        min_size=settings.database_min_pool,
        max_size=settings.database_max_pool,
    )
    await db.connect()
    try:
        applied = await apply_postgres_migrations(db, BASE_DIR / "migrations" / "postgres")
        logger.info("PostgreSQL migrations applied: %s", ", ".join(applied))
        report = await import_legacy_sqlite_to_postgres(
            Path(settings.legacy_sqlite_path),
            db.pool,  # type: ignore[arg-type]
            batch_size=settings.legacy_import_batch_size,
        )
        await db.set_meta(
            "legacy_sqlite_import",
            {
                "completed": True,
                "source": settings.legacy_sqlite_path,
                "users": report.users,
                "subscriptions": report.subscriptions,
                "withdraw_requests": report.withdraw_requests,
                "payment_intents": report.payment_intents,
                "payment_status_history": report.payment_status_history,
                "support_tickets": report.support_tickets,
                "support_messages": report.support_messages,
                "total_rows": report.total_rows,
            },
        )
        logger.info(
            "Legacy migration finished: users=%s subscriptions=%s withdraw_requests=%s payment_intents=%s payment_status_history=%s support_tickets=%s support_messages=%s total_rows=%s",
            report.users,
            report.subscriptions,
            report.withdraw_requests,
            report.payment_intents,
            report.payment_status_history,
            report.support_tickets,
            report.support_messages,
            report.total_rows,
        )
    finally:
        await db.close()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
