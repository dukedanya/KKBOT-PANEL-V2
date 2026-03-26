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
    filters_mod = types.ModuleType("aiogram.filters")
    fsm_mod = types.ModuleType("aiogram.fsm")
    fsm_context_mod = types.ModuleType("aiogram.fsm.context")
    fsm_state_mod = types.ModuleType("aiogram.fsm.state")
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
    class InlineKeyboardButton:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
    class InlineKeyboardMarkup:
        def __init__(self, inline_keyboard=None):
            self.inline_keyboard = inline_keyboard or []
    class Bot:
        pass
    class Message:
        pass
    class CallbackQuery:
        pass
    class Command:
        def __init__(self, *_args, **_kwargs):
            pass
    class FSMContext:
        pass
    class State:
        pass
    class StatesGroup:
        pass
    enums.ParseMode = type("ParseMode", (), {"HTML": "HTML"})
    types_mod.InlineKeyboardButton = InlineKeyboardButton
    types_mod.InlineKeyboardMarkup = InlineKeyboardMarkup
    types_mod.Bot = Bot
    types_mod.Message = Message
    types_mod.CallbackQuery = CallbackQuery
    filters_mod.Command = Command
    fsm_context_mod.FSMContext = FSMContext
    fsm_state_mod.State = State
    fsm_state_mod.StatesGroup = StatesGroup
    aiogram.Bot = Bot
    aiogram.F = _F()
    aiogram.Router = Router
    aiogram.enums = enums
    aiogram.types = types_mod
    aiogram.filters = filters_mod
    aiogram.fsm = fsm_mod
    sys.modules["aiogram"] = aiogram
    sys.modules["aiogram.enums"] = enums
    sys.modules["aiogram.types"] = types_mod
    sys.modules["aiogram.filters"] = filters_mod
    sys.modules["aiogram.fsm"] = fsm_mod
    sys.modules["aiogram.fsm.context"] = fsm_context_mod
    sys.modules["aiogram.fsm.state"] = fsm_state_mod

import unittest
from unittest.mock import AsyncMock, patch

from services.traffic_state import TotalTrafficSnapshot
from handlers.payment_diagnostics import _build_user_card_text


class FakeDB:
    async def get_user_card(self, user_id):
        return {
            "user": {
                "join_date": "2026-03-20 12:00:00",
                "has_subscription": 1,
                "vpn_url": "http://example/sub",
                "expiry": "2026-04-20 12:00:00",
                "frozen_until": "",
                "balance": 12.5,
                "trial_used": 1,
                "ref_by": 0,
                "ref_code": "ABC123",
                "banned": 0,
                "ban_reason": "",
            },
            "referral_summary": {"total_refs": 2, "paid_refs": 1, "earned_rub": 50.0},
            "partner_settings": {"status": "standard", "note": ""},
            "support_tickets": [],
            "support_restriction": {"active": 0, "expires_at": "", "reason": ""},
            "payments": [],
            "withdraws": [],
            "adjustments": [],
        }


class AdminUserCardTrafficStateTests(unittest.IsolatedAsyncioTestCase):
    async def test_user_card_shows_total_traffic_and_grace_state(self):
        db = FakeDB()
        snapshot = TotalTrafficSnapshot(
            found=True,
            fresh=True,
            source_path="/tmp/traffic.json",
            total_bytes=50 * 1073741824,
            quota_bytes=100 * 1073741824,
            remaining_bytes=50 * 1073741824,
            mode="grace",
            grace_until="2026-03-29T08:19:02+00:00",
            expired=False,
            over_limit=True,
        )

        with patch("handlers.payment_diagnostics.get_total_traffic_snapshot_for_user", AsyncMock(return_value=snapshot)):
            text = await _build_user_card_text(db, 123456)

        self.assertIn("Общий traffic-state", text)
        self.assertIn("50.0 ГБ", text)
        self.assertIn("100.0 ГБ", text)
        self.assertIn("🐢 Grace", text)
        self.assertIn("29.03.2026 08:19", text)
        self.assertIn("да", text)


if __name__ == "__main__":
    unittest.main()
