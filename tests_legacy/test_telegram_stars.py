import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from services.telegram_stars import TelegramStarsAPI


class TelegramStarsServiceTests(unittest.TestCase):
    def test_payload_roundtrip(self):
        payload = TelegramStarsAPI.build_invoice_payload(payment_id="pay123", user_id=42, plan_id="basic")
        parsed = TelegramStarsAPI.parse_invoice_payload(payload)
        self.assertEqual(parsed["payment_id"], "pay123")
        self.assertEqual(parsed["user_id"], 42)
        self.assertEqual(parsed["plan_id"], "basic")

    def test_resolve_stars_amount_prefers_plan_price(self):
        amount = TelegramStarsAPI.resolve_stars_amount(amount_rub=200, plan={"price_stars": 175})
        self.assertEqual(amount, 175)


class TelegramStarsFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_successful_payment_handler_accepts_payment(self):
        from handlers.buy import successful_payment_handler

        db = SimpleNamespace()
        db.register_payment_event = AsyncMock(return_value=True)
        db.get_pending_payment = AsyncMock(return_value={
            "payment_id": "pay-1",
            "user_id": 77,
            "plan_id": "basic",
            "amount": 200,
            "status": "pending",
            "provider": "telegram_stars",
            "msg_id": None,
        })
        db.set_pending_payment_provider_id = AsyncMock(return_value=True)

        panel = object()
        message = SimpleNamespace(
            successful_payment=SimpleNamespace(
                currency="XTR",
                invoice_payload=TelegramStarsAPI.build_invoice_payload(payment_id="pay-1", user_id=77, plan_id="basic"),
                telegram_payment_charge_id="tg-charge-1",
                provider_payment_charge_id="",
            ),
            bot=None,
        )

        import handlers.buy as buy_module
        original = buy_module.process_successful_payment
        buy_module.process_successful_payment = AsyncMock(return_value={"ok": True})
        try:
            await successful_payment_handler(message, db=db, panel=panel)
        finally:
            buy_module.process_successful_payment = original

        db.register_payment_event.assert_awaited_once()
        db.set_pending_payment_provider_id.assert_awaited_once_with("pay-1", "telegram_stars", "tg-charge-1")

    async def test_pre_checkout_rejects_unknown_payment(self):
        from handlers.buy import pre_checkout_query_handler

        query = SimpleNamespace(
            invoice_payload=TelegramStarsAPI.build_invoice_payload(payment_id="missing", user_id=11, plan_id="basic"),
            answer=AsyncMock(),
        )
        db = SimpleNamespace(get_pending_payment=AsyncMock(return_value=None))
        await pre_checkout_query_handler(query, db=db)
        query.answer.assert_awaited_once()
        args, kwargs = query.answer.await_args
        self.assertFalse(kwargs["ok"])
