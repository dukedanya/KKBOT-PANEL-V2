import sys
import types

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
    enums = types.ModuleType("aiogram.enums")
    types_mod = types.ModuleType("aiogram.types")
    enums.ParseMode = type("ParseMode", (), {"HTML": "HTML"})
    class InlineKeyboardButton:
        def __init__(self, *args, **kwargs):
            pass
    class InlineKeyboardMarkup:
        def __init__(self, inline_keyboard=None):
            self.inline_keyboard = inline_keyboard or []
    types_mod.InlineKeyboardButton = InlineKeyboardButton
    types_mod.InlineKeyboardMarkup = InlineKeyboardMarkup
    exceptions_mod = types.ModuleType("aiogram.exceptions")
    class TelegramBadRequest(Exception):
        pass
    class Bot: ...
    class Message: ...
    class CallbackQuery: ...
    exceptions_mod.TelegramBadRequest = TelegramBadRequest
    types_mod.Message = Message
    types_mod.CallbackQuery = CallbackQuery
    aiogram.Bot = Bot
    aiogram.enums = enums
    aiogram.types = types_mod
    aiogram.exceptions = exceptions_mod
    sys.modules["aiogram"] = aiogram
    sys.modules["aiogram.enums"] = enums
    sys.modules["aiogram.types"] = types_mod
    sys.modules["aiogram.exceptions"] = exceptions_mod

import unittest
from unittest.mock import AsyncMock, patch

from services.subscriptions import reward_referrer_percent


class FakeDB:
    def __init__(self):
        self.users = {
            10: {"user_id": 10, "ref_by": 20},
            20: {"user_id": 20, "ref_by": 30},
            30: {"user_id": 30, "ref_by": 20},
        }
        self.balance_events = []
        self.reward_counts = []

    async def get_user(self, user_id):
        return self.users.get(user_id)

    async def add_balance(self, user_id, amount):
        self.balance_events.append((user_id, round(amount, 2)))
        return True

    async def add_ref_history(self, user_id, ref_user_id, amount=0, bonus_days=0):
        return True

    async def increment_ref_rewarded_count(self, user_id):
        self.reward_counts.append(user_id)
        return True


class ReferralCycleGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_cycle_does_not_double_reward_same_referrer(self):
        db = FakeDB()
        with patch("services.subscriptions.notify_user", new=AsyncMock()):
            await reward_referrer_percent(10, 1000.0, db=db)

        self.assertEqual(db.balance_events, [(20, 250.0), (30, 100.0)])
        self.assertEqual(db.reward_counts, [20])


if __name__ == "__main__":
    unittest.main()
