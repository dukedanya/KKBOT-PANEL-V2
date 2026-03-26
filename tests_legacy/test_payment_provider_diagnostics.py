import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from services.telegram_stars import TelegramStarsAPI
from services.yookassa import YooKassaAPI


class ProviderRefundHelpersTests(unittest.IsolatedAsyncioTestCase):
    async def test_telegram_stars_refund_uses_bot_method(self):
        api = TelegramStarsAPI()
        bot = SimpleNamespace(refund_star_payment=AsyncMock(return_value=True))
        ok = await api.refund_payment(bot=bot, user_id=77, telegram_payment_charge_id="charge-1")
        self.assertTrue(ok)
        bot.refund_star_payment.assert_awaited_once_with(user_id=77, telegram_payment_charge_id="charge-1")

    def test_yookassa_build_idempotence_key_uses_prefix(self):
        key = YooKassaAPI.build_idempotence_key("refund")
        self.assertTrue(key.startswith("refund-"))


class PaymentDiagnosticsDbTests(unittest.IsolatedAsyncioTestCase):
    async def test_database_returns_provider_counts_and_recent_events(self):
        from db import Database

        db = Database(":memory:")
        await db.connect()
        try:
            await db.add_pending_payment("pay-1", 1, "basic", 100, provider="yookassa")
            await db.add_pending_payment("pay-2", 2, "basic", 200, provider="telegram_stars")
            await db.register_payment_event("evt-1", payment_id="pay-1", source="test", event_type="created")
            counts = await db.get_payment_provider_counts()
            events = await db.get_recent_payment_events("pay-1")
        finally:
            await db.close()

        providers = {row["provider"]: row for row in counts}
        self.assertEqual(int(providers["yookassa"]["total"]), 1)
        self.assertEqual(int(providers["telegram_stars"]["total"]), 1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_type"], "created")


    async def test_database_returns_daily_and_period_business_reports(self):
        from db import Database

        db = Database(":memory:")
        await db.connect()
        try:
            await db.add_user(1)
            await db.add_user(2)
            await db.update_user(2, ref_by=1)
            await db.mark_trial_used(2)
            await db.add_pending_payment("pay-accepted", 2, "basic", 300, provider="yookassa")
            await db.update_payment_status("pay-accepted", "processing", source="test", reason="ok")
            await db.update_payment_status("pay-accepted", "accepted", allowed_current_statuses=["processing"], source="test", reason="ok")
            await db.add_pending_payment("pay-refunded", 2, "basic", 100, provider="yookassa")
            await db.update_payment_status("pay-refunded", "processing", source="test", reason="ok")
            await db.update_payment_status("pay-refunded", "accepted", allowed_current_statuses=["processing"], source="test", reason="ok")
            await db.update_payment_status("pay-refunded", "refunded", allowed_current_statuses=["accepted"], source="test", reason="refund")
            await db.add_ref_history(user_id=1, ref_user_id=2, amount=25.0, bonus_days=0)

            await db.add_user(3)
            await db.update_user(3, ref_by=1)
            await db.add_pending_payment("pay-old", 3, "basic", 150, provider="yookassa")
            await db.update_payment_status("pay-old", "processing", source="test", reason="ok")
            await db.update_payment_status("pay-old", "accepted", allowed_current_statuses=["processing"], source="test", reason="ok")
            async with db.lock:
                await db.conn.execute("UPDATE users SET join_date = datetime('now', '-3 day') WHERE user_id = 3")
                await db.conn.execute("UPDATE payment_status_history SET created_at = datetime('now', '-3 day') WHERE payment_id = 'pay-old'")
                await db.conn.commit()

            users = await db.get_daily_user_acquisition_report(days_ago=0)
            sales = await db.get_daily_subscription_sales_report(days_ago=0)
            period_users = await db.get_period_user_acquisition_report(days=7)
            period_sales = await db.get_period_subscription_sales_report(days=7)
            total = await db.get_total_revenue_summary()
        finally:
            await db.close()

        self.assertEqual(users["new_users"], 2)
        self.assertEqual(users["referred_new_users"], 1)
        self.assertEqual(users["trial_started_new_users"], 1)
        self.assertEqual(sales["subscriptions_bought"], 2)
        self.assertEqual(sales["gross_revenue"], 400.0)
        self.assertEqual(sales["refunded_revenue"], 100.0)
        self.assertEqual(sales["estimated_profit"], 275.0)
        self.assertEqual(period_users["new_users"], 3)
        self.assertEqual(period_users["referred_new_users"], 2)
        self.assertEqual(period_sales["subscriptions_bought"], 3)
        self.assertEqual(period_sales["gross_revenue"], 550.0)
        self.assertEqual(total["gross_revenue"], 550.0)
        self.assertEqual(total["net_revenue"], 450.0)
