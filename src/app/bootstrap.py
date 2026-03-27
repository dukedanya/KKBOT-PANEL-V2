import asyncio
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from app.background import BackgroundContext, cancel_background_tasks, start_background_tasks
from app.container import AppContainer, build_container
from app.dispatcher import build_dispatcher
from app.operational import run_startup_checks
from app.runtime import handle_loop_exception, install_process_exception_hooks, log_startup_summary, run_app, validate_runtime_or_raise
from config import Config
from services.migrations import apply_migrations, get_pending_migrations, latest_migration_version
from tariffs.loader import load_tariffs
from utils.helpers import set_bot

logger = logging.getLogger(__name__)
STARS_MULTIPLIER_SETTING_KEY = "system:telegram_stars_price_multiplier"
RUNTIME_FLOAT_SETTINGS = {
    "system:ref_percent_level1": "REF_PERCENT_LEVEL1",
    "system:ref_percent_level2": "REF_PERCENT_LEVEL2",
    "system:ref_percent_level3": "REF_PERCENT_LEVEL3",
    "system:min_withdraw": "MIN_WITHDRAW",
}
RUNTIME_INT_SETTINGS = {
    "system:ref_bonus_days": "REF_BONUS_DAYS",
    "system:panel_target_inbound_count": "PANEL_TARGET_INBOUND_COUNT",
}
RUNTIME_STR_SETTINGS = {
    "system:panel_target_inbound_ids": "PANEL_TARGET_INBOUND_IDS",
}


@dataclass(slots=True)
class AppRuntimeContext:
    container: AppContainer
    bot: Bot
    dispatcher: object
    background_tasks: list[asyncio.Task]


@asynccontextmanager
async def lifespan() -> AppRuntimeContext:
    container = build_container()
    bot = Bot(
        token=Config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    background_tasks: list[asyncio.Task] = []
    try:
        await container.db.connect()
        if hasattr(container.db, "get_setting"):
            raw_stars_multiplier = await container.db.get_setting(
                STARS_MULTIPLIER_SETTING_KEY,
                str(Config.TELEGRAM_STARS_PRICE_MULTIPLIER),
            )
            try:
                Config.set_stars_price_multiplier(float(raw_stars_multiplier or Config.TELEGRAM_STARS_PRICE_MULTIPLIER))
            except (TypeError, ValueError):
                logger.warning("Invalid stored Telegram Stars multiplier: %s", raw_stars_multiplier)
            for setting_key, attr_name in RUNTIME_FLOAT_SETTINGS.items():
                raw_value = await container.db.get_setting(setting_key, str(getattr(Config, attr_name)))
                try:
                    setattr(Config, attr_name, float(raw_value or getattr(Config, attr_name)))
                except (TypeError, ValueError):
                    logger.warning("Invalid stored float setting %s=%s", setting_key, raw_value)
            for setting_key, attr_name in RUNTIME_INT_SETTINGS.items():
                raw_value = await container.db.get_setting(setting_key, str(getattr(Config, attr_name)))
                try:
                    setattr(Config, attr_name, int(float(raw_value or getattr(Config, attr_name))))
                except (TypeError, ValueError):
                    logger.warning("Invalid stored int setting %s=%s", setting_key, raw_value)
            for setting_key, attr_name in RUNTIME_STR_SETTINGS.items():
                raw_value = await container.db.get_setting(setting_key, str(getattr(Config, attr_name)))
                if raw_value:
                    setattr(Config, attr_name, str(raw_value).strip())
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if Config.MIGRATIONS_AUTO_APPLY:
            applied = await apply_migrations(container.db, base_dir)
            if applied:
                logger.info("Migrations applied: %s", applied)
        pending_after_apply = await get_pending_migrations(container.db, base_dir)
        if pending_after_apply and Config.effective_startup_fail_on_pending_migrations():
            raise RuntimeError(
                "Startup aborted: pending migrations remain: "
                + ", ".join(name for _, name in pending_after_apply)
            )
        if hasattr(container.db, "auto_repair_schema_drift"):
            repaired = await container.db.auto_repair_schema_drift()
            if repaired:
                logger.warning("Schema drift auto-repaired: %s", ", ".join(repaired))
        remaining_drift = []
        if hasattr(container.db, "get_schema_drift_issues"):
            remaining_drift = await container.db.get_schema_drift_issues()
        if remaining_drift and Config.effective_startup_fail_on_schema_drift():
            raise RuntimeError(
                "Startup aborted: schema drift remains after auto-repair: "
                + ", ".join(remaining_drift)
            )
        if hasattr(container.db, "sync_schema_version_with_migrations"):
            synced_version = await container.db.sync_schema_version_with_migrations()
            logger.info("Schema version synced: %s", synced_version)
            expected_version = latest_migration_version(base_dir)
            if synced_version < expected_version:
                raise RuntimeError(
                    f"Startup aborted: schema version {synced_version} is behind latest migration {expected_version}"
                )
        await container.panel.start()
        startup_report = await run_startup_checks(container=container, base_dir=base_dir)
        logger.info("Startup checks ok: %s", startup_report.checks)

        dp = build_dispatcher(bot=bot, db=container.db, panel=container.panel, payment_gateway=container.payment_gateway)
        bg_ctx = BackgroundContext(
            db=container.db,
            panel=container.panel,
            payment_gateway=container.payment_gateway,
            bot=bot,
            health_alert_state=container.health_alert_state,
        )
        background_tasks = start_background_tasks(bg_ctx)
        yield AppRuntimeContext(container=container, bot=bot, dispatcher=dp, background_tasks=background_tasks)
    finally:
        await cancel_background_tasks(background_tasks)
        await container.db.close()
        await container.panel.close()
        await container.payment_gateway.close()
        await bot.session.close()


async def run() -> None:
    added_env_vars = Config.sync_missing_env_variables()
    if added_env_vars:
        logger.info("Added missing .env vars: %s", ", ".join(added_env_vars))
    duplicate_env_vars = Config.detect_duplicate_env_variables()
    if duplicate_env_vars:
        logger.warning("Duplicate .env vars found: %s", ", ".join(duplicate_env_vars))
    validate_runtime_or_raise()
    install_process_exception_hooks()
    log_startup_summary()
    load_tariffs()

    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handle_loop_exception)

    async with lifespan() as ctx:
        me = await ctx.bot.get_me()
        set_bot(ctx.bot, me.username)
        logger.info("Bot started | Бот запущен: @%s", me.username or "unknown")
        await run_app(
            ctx.dispatcher,
            ctx.bot,
            db=ctx.container.db,
            panel=ctx.container.panel,
            payment_gateway=ctx.container.payment_gateway,
        )
