from __future__ import annotations

import logging
from pathlib import Path

from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from kkbot.config import BASE_DIR, load_settings
from kkbot.db.legacy_import import import_legacy_sqlite_to_postgres
from kkbot.db.migrations import apply_postgres_migrations
from kkbot.db.postgres import PostgresDatabase
from kkbot.logging import configure_logging
from kkbot.repositories.meta import MetaRepository
from kkbot.repositories.users import UserRepository

logger = logging.getLogger(__name__)


def _build_router(db: PostgresDatabase, admin_ids: set[int]) -> Router:
    router = Router()
    users = UserRepository(db)
    meta = MetaRepository(db)

    @router.message(CommandStart())
    async def on_start(message: Message) -> None:
        user = message.from_user
        if user is None:
            return
        await db.upsert_bot_user(
            user.id,
            user.username,
            user.first_name,
            user.last_name,
            user.language_code,
            is_admin=user.id in admin_ids,
        )
        await message.answer(
            "KKBOT PANEL V2.0 запущен.\n"
            "База уже работает через PostgreSQL.\n"
            "Старые данные, если были, переносятся только один раз при первом старте."
        )

    @router.message(Command("me"))
    async def on_me(message: Message) -> None:
        user = message.from_user
        if user is None:
            return
        snapshot = await users.get_user_snapshot(user.id)
        if snapshot is None:
            await message.answer("Пользователь пока не найден в PostgreSQL.")
            return
        await message.answer(
            "Профиль в PostgreSQL найден.\n"
            f"user_id: {snapshot['user_id']}\n"
            f"subscription: {snapshot.get('subscription')}"
        )

    @router.message(Command("health"))
    async def on_health(message: Message) -> None:
        user = message.from_user
        if user is None or user.id not in admin_ids:
            return
        import_status = await meta.get_legacy_import_status()
        db_ok = await db.ping()
        await message.answer(
            "KKBOT PANEL V2.0 health\n"
            f"postgres: {'ok' if db_ok else 'fail'}\n"
            f"legacy_import: {import_status or 'not-run'}"
        )

    return router


async def _run_legacy_import_if_needed(db: PostgresDatabase, *, sqlite_path: str, batch_size: int) -> None:
    meta = await db.get_meta("legacy_sqlite_import")
    if meta and meta.get("completed"):
        logger.info("Legacy import already completed")
        return

    source = Path(sqlite_path)
    if not source.exists():
        logger.info("Legacy SQLite not found, skipping import")
        await db.set_meta("legacy_sqlite_import", {"completed": True, "source_missing": True})
        return

    logger.info("Starting legacy SQLite import: %s", source)
    report = await import_legacy_sqlite_to_postgres(source, db.pool, batch_size=batch_size)  # type: ignore[arg-type]
    await db.set_meta(
        "legacy_sqlite_import",
        {
            "completed": True,
            "source": str(source),
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
        "Legacy SQLite import completed: users=%s subscriptions=%s payments=%s total_rows=%s",
        report.users,
        report.subscriptions,
        report.payment_intents,
        report.total_rows,
    )


async def run() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)

    if not settings.bot_token:
        raise RuntimeError("BOT_TOKEN is empty")
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is empty")

    db = PostgresDatabase(
        settings.database_url,
        min_size=settings.database_min_pool,
        max_size=settings.database_max_pool,
    )
    await db.connect()
    applied = await apply_postgres_migrations(db, BASE_DIR / "migrations" / "postgres")
    logger.info("PostgreSQL migrations applied: %s", ", ".join(applied))

    if settings.auto_migrate_legacy and settings.legacy_sqlite_path:
        await _run_legacy_import_if_needed(
            db,
            sqlite_path=settings.legacy_sqlite_path,
            batch_size=settings.legacy_import_batch_size,
        )

    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dispatcher = Dispatcher()
    dispatcher.include_router(_build_router(db, set(settings.admin_user_ids)))

    logger.info("Bot runtime starting on PostgreSQL")
    try:
        await dispatcher.start_polling(bot)
    finally:
        await bot.session.close()
        await db.close()
