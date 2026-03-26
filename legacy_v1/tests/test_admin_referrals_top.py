import unittest

from keyboards.builders import admin_menu_keyboard
from handlers.payment_diagnostics import _build_top_referrers_detail, _admin_dashboard_keyboard


def test_admin_entry_menu_is_compact():
    keyboard = admin_menu_keyboard()
    texts = [button.text for row in keyboard.inline_keyboard for button in row]
    assert "🧭 Сводка (дашборд)" in texts
    assert "💳 Платежи и диагностика" in texts
    assert "📣 Рассылки и массовые действия" in texts


def test_admin_dashboard_contains_top_referrers_button():
    keyboard = _admin_dashboard_keyboard()
    texts = [button.text for row in keyboard.inline_keyboard for button in row]
    assert "🏆 Топ-10 рефералов" in texts


class DummyDB:
    async def get_top_referrers_extended(self, limit=10):
        return [
            {"ref_by": 1001, "paid_count": 7, "earned_rub": 2500.0},
            {"ref_by": 1002, "paid_count": 5, "earned_rub": 1500.0},
        ]


class AdminTopReferrersTests(unittest.IsolatedAsyncioTestCase):
    async def test_build_top_referrers_detail(self):
        text = await _build_top_referrers_detail(DummyDB(), limit=10)
        self.assertIn("Топ-10 рефералов", text)
        self.assertIn("1001", text)
        self.assertIn("7", text)
        self.assertIn("2500.00", text)
