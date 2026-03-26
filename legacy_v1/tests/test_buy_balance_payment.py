import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


def _install_test_stubs() -> None:
    if "aiosqlite" not in sys.modules:
        fake_aiosqlite = types.ModuleType("aiosqlite")
        class _Connection: ...
        fake_aiosqlite.Connection = _Connection
        fake_aiosqlite.Row = dict
        async def _connect(*args, **kwargs):
            return None
        fake_aiosqlite.connect = _connect
        sys.modules["aiosqlite"] = fake_aiosqlite

    if "aiogram" not in sys.modules:
        aiogram = types.ModuleType("aiogram")
        types_mod = types.ModuleType("aiogram.types")
        enums_mod = types.ModuleType("aiogram.enums")
        exceptions_mod = types.ModuleType("aiogram.exceptions")

        class _F:
            def __getattr__(self, _name):
                return self
            def startswith(self, *_args, **_kwargs):
                return self
            def in_(self, *_args, **_kwargs):
                return self
            def __eq__(self, _other):
                return self

        class Router:
            def message(self, *_args, **_kwargs):
                return lambda fn: fn
            def callback_query(self, *_args, **_kwargs):
                return lambda fn: fn
            def pre_checkout_query(self, *_args, **_kwargs):
                return lambda fn: fn

        class InlineKeyboardButton:
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs

        class InlineKeyboardMarkup:
            def __init__(self, inline_keyboard=None):
                self.inline_keyboard = inline_keyboard or []

        class LabeledPrice:
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs

        class CallbackQuery:
            pass

        class Message:
            pass

        class PreCheckoutQuery:
            pass

        class Bot:
            pass

        class TelegramBadRequest(Exception):
            pass

        enums_mod.ParseMode = type("ParseMode", (), {"HTML": "HTML"})
        aiogram.Router = Router
        aiogram.F = _F()
        aiogram.Bot = Bot
        types_mod.CallbackQuery = CallbackQuery
        types_mod.Message = Message
        types_mod.PreCheckoutQuery = PreCheckoutQuery
        types_mod.InlineKeyboardButton = InlineKeyboardButton
        types_mod.InlineKeyboardMarkup = InlineKeyboardMarkup
        types_mod.LabeledPrice = LabeledPrice
        exceptions_mod.TelegramBadRequest = TelegramBadRequest

        aiogram.types = types_mod
        aiogram.enums = enums_mod
        aiogram.exceptions = exceptions_mod

        sys.modules["aiogram"] = aiogram
        sys.modules["aiogram.types"] = types_mod
        sys.modules["aiogram.enums"] = enums_mod
        sys.modules["aiogram.exceptions"] = exceptions_mod


_install_test_stubs()

from handlers.buy import _pay_plan_with_balance


class BuyBalancePaymentTests(unittest.IsolatedAsyncioTestCase):
    async def test_balance_payment_success_flow(self):
        callback = SimpleNamespace(
            message=SimpleNamespace(message_id=1001),
            from_user=SimpleNamespace(id=42),
            bot=object(),
            answer=AsyncMock(),
        )
        db = SimpleNamespace(
            get_balance=AsyncMock(side_effect=[500.0, 350.0]),
            get_user_pending_payment=AsyncMock(return_value=None),
            subtract_balance=AsyncMock(return_value=True),
            add_pending_payment=AsyncMock(return_value=True),
            add_balance=AsyncMock(),
            update_payment_status=AsyncMock(),
        )
        panel = object()
        plan = {"id": "basic", "name": "Basic", "price_rub": 150.0, "duration_days": 30}

        with patch("handlers.buy.get_referral_first_payment_offer", new=AsyncMock(return_value={"amount": 150.0})), \
             patch("handlers.buy.process_successful_payment", new=AsyncMock(return_value={"ok": True})):
            await _pay_plan_with_balance(callback, db=db, panel=panel, user_id=42, plan=plan, plan_id="basic")

        db.subtract_balance.assert_awaited_once_with(42, 150.0)
        db.add_pending_payment.assert_awaited_once()
        callback.answer.assert_awaited_once()
        answer_text = callback.answer.await_args.args[0]
        self.assertIn("Оплата с баланса прошла успешно", answer_text)
        db.add_balance.assert_not_awaited()

    async def test_balance_payment_insufficient_funds(self):
        callback = SimpleNamespace(
            message=SimpleNamespace(message_id=1002),
            from_user=SimpleNamespace(id=77),
            bot=object(),
            answer=AsyncMock(),
        )
        db = SimpleNamespace(
            get_balance=AsyncMock(return_value=20.0),
            get_user_pending_payment=AsyncMock(return_value=None),
            subtract_balance=AsyncMock(return_value=False),
            add_pending_payment=AsyncMock(),
            add_balance=AsyncMock(),
            update_payment_status=AsyncMock(),
        )
        panel = object()
        plan = {"id": "pro", "name": "Pro", "price_rub": 100.0, "duration_days": 30}

        with patch("handlers.buy.get_referral_first_payment_offer", new=AsyncMock(return_value={"amount": 100.0})):
            await _pay_plan_with_balance(callback, db=db, panel=panel, user_id=77, plan=plan, plan_id="pro")

        db.subtract_balance.assert_not_awaited()
        db.add_pending_payment.assert_not_awaited()
        callback.answer.assert_awaited_once()
        self.assertIn("Недостаточно средств", callback.answer.await_args.args[0])


if __name__ == "__main__":
    unittest.main()
