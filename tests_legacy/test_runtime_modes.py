import asyncio
import unittest
from unittest.mock import AsyncMock, patch


class RuntimeModeTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_app_uses_polling_mode(self):
        from app import runtime

        dp = AsyncMock()
        bot = AsyncMock()
        with patch.object(runtime.Config, "APP_MODE", "polling"), \
             patch.object(runtime, "run_polling", AsyncMock()) as run_polling, \
             patch.object(runtime, "run_webhook", AsyncMock()) as run_webhook:
            await runtime.run_app(dp, bot, db=object(), panel=object())

        run_polling.assert_awaited_once_with(dp, bot)
        run_webhook.assert_not_awaited()

    async def test_run_app_uses_webhook_mode(self):
        from app import runtime

        dp = AsyncMock()
        bot = AsyncMock()
        db = object()
        panel = object()
        with patch.object(runtime.Config, "APP_MODE", "webhook"), \
             patch.object(runtime, "run_polling", AsyncMock()) as run_polling, \
             patch.object(runtime, "run_webhook", AsyncMock()) as run_webhook:
            await runtime.run_app(dp, bot, db=db, panel=panel)

        run_webhook.assert_awaited_once_with(dp, bot, db=db, panel=panel, payment_gateway=None)
        run_polling.assert_not_awaited()

    async def test_run_webhook_waits_for_shutdown_without_polling(self):
        from app import runtime

        dp = AsyncMock()
        bot = AsyncMock()
        runner = object()
        db = object()
        panel = object()

        with patch.object(runtime, "start_webhook_server", AsyncMock(return_value=runner)) as start_webhook_server, \
             patch.object(runtime, "stop_webhook_server", AsyncMock()) as stop_webhook_server, \
             patch.object(runtime, "wait_for_shutdown_signal", AsyncMock()) as wait_for_shutdown_signal, \
             patch.object(runtime, "install_signal_handlers"):
            await runtime.run_webhook(dp, bot, db=db, panel=panel)

        start_webhook_server.assert_awaited_once()
        wait_for_shutdown_signal.assert_awaited_once()
        stop_webhook_server.assert_awaited_once_with(runner)
        dp.start_polling.assert_not_called()

    async def test_wait_for_shutdown_signal_returns_after_trigger(self):
        from app.runtime import ShutdownSignal, wait_for_shutdown_signal

        signal = ShutdownSignal()
        waiter = asyncio.create_task(wait_for_shutdown_signal(signal))
        await asyncio.sleep(0)
        signal.trigger()
        await asyncio.wait_for(waiter, timeout=1)


if __name__ == "__main__":
    unittest.main()
