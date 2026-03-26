import asyncio
import logging
import signal
import sys
from contextlib import suppress
from typing import Any

from aiogram import Bot

from config import Config
from services.webhook import start_webhook_server, stop_webhook_server

logger = logging.getLogger(__name__)


class ShutdownSignal:
    def __init__(self) -> None:
        self._event = asyncio.Event()

    def trigger(self) -> None:
        self._event.set()

    async def wait(self) -> None:
        await self._event.wait()

def handle_loop_exception(loop: asyncio.AbstractEventLoop, context: dict) -> None:
    message = context.get("message", "Unhandled asyncio loop exception")
    exc = context.get("exception")
    if exc:
        logger.exception(message, exc_info=exc)
    else:
        logger.error(message)

def log_startup_summary() -> None:
    summary = Config.startup_summary()
    logger.info("Startup config summary: %s", summary)

def validate_runtime_or_raise() -> None:
    errors = Config.validate_startup()
    if errors:
        for err in errors:
            logger.critical("Startup validation failed | Проверь конфиг: %s", err)
        raise RuntimeError("Invalid configuration. Fix .env before starting the bot.")

def install_process_exception_hooks() -> None:
    def _sys_hook(exc_type, exc_value, exc_tb):
        logger.critical("Unhandled top-level exception | Критическая ошибка процесса", exc_info=(exc_type, exc_value, exc_tb))

    sys.excepthook = _sys_hook

def install_signal_handlers(shutdown_signal: ShutdownSignal) -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return

    def _handler() -> None:
        logger.info("Shutdown signal received | Начинаю остановку")
        shutdown_signal.trigger()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _handler)


async def wait_for_shutdown_signal(shutdown_signal: ShutdownSignal) -> None:
    await shutdown_signal.wait()


async def run_polling(dp: Any, bot: Bot) -> None:
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Runtime mode: polling")
    await dp.start_polling(bot)


async def run_webhook(dp: Any, bot: Bot, *, db, panel, payment_gateway=None) -> None:
    shutdown_signal = ShutdownSignal()
    install_signal_handlers(shutdown_signal)
    webhook_runner = await start_webhook_server(
        bot,
        db,
        panel,
        bind_host=Config.WEBHOOK_BIND_HOST,
        port=Config.WEBHOOK_PORT,
        payment_gateway=payment_gateway,
    )
    logger.info("Runtime mode: webhook")
    try:
        await wait_for_shutdown_signal(shutdown_signal)
    finally:
        await stop_webhook_server(webhook_runner)


async def run_app(dp: Any, bot: Bot, *, db, panel, payment_gateway=None) -> None:
    if Config.APP_MODE == "webhook":
        await run_webhook(dp, bot, db=db, panel=panel, payment_gateway=payment_gateway)
        return
    await run_polling(dp, bot)
