import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from aiohttp.test_utils import TestClient, TestServer

from db import Database
from services.payment_flow import process_successful_payment
from services.webhook import build_webhook_app


class FakePanel:
    async def delete_client(self, base_email):
        return True

    async def create_client(self, **kwargs):
        return {"subId": "sub-123"}


class FakeBot:
    async def send_message(self, *args, **kwargs):
        return None


class FakeItpay:
    pass


class PaymentHardeningTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        await self.db.connect()
        await self.db.add_user(501)
        await self.db.add_pending_payment("p-audit", 501, "plan-basic", 199.0)

    async def asyncTearDown(self):
        await self.db.close()

    async def test_payment_status_history_records_claim_and_accept(self):
        payment = {"payment_id": "p-audit", "user_id": 501, "plan_id": "plan-basic", "amount": 199.0}
        plan = {"id": "plan-basic", "name": "Basic", "traffic_gb": 50, "ip_limit": 2, "duration_days": 30}

        with patch("services.payment_flow.get_by_id", return_value=plan), \
             patch("services.payment_flow.notify_admins", new=AsyncMock()), \
             patch("services.subscriptions.notify_user", new=AsyncMock()), \
             patch("services.subscriptions.notify_admins", new=AsyncMock()):
            result = await process_successful_payment(payment=payment, db=self.db, panel=FakePanel(), bot=None, admin_context="test/audit")

        self.assertTrue(result["ok"])
        history = await self.db.get_payment_status_history("p-audit")
        self.assertEqual([row["to_status"] for row in history], ["processing", "accepted"])
        self.assertEqual(history[0]["source"], "test/audit")
        self.assertIn("subscription activated", history[1]["reason"])

    async def test_register_payment_event_deduplicates(self):
        first = await self.db.register_payment_event("evt-1", payment_id="p-audit", source="test", event_type="payment.completed")
        second = await self.db.register_payment_event("evt-1", payment_id="p-audit", source="test", event_type="payment.completed")
        self.assertTrue(first)
        self.assertFalse(second)

    async def test_reclaim_stale_processing_records_history(self):
        await self.db.claim_pending_payment("p-audit", source="test/reclaim")
        async with self.db.lock:
            await self.db.conn.execute(
                "UPDATE pending_payments SET processing_started_at = datetime('now', '-20 minutes') WHERE payment_id = ?",
                ("p-audit",),
            )
            await self.db.conn.commit()

        released = await self.db.reclaim_stale_processing_payments(timeout_minutes=15, source="test/recovery")
        self.assertEqual(released, 1)
        history = await self.db.get_payment_status_history("p-audit")
        self.assertEqual(history[-1]["source"], "test/recovery")
        self.assertEqual(history[-1]["to_status"], "pending")

    async def test_webhook_duplicate_event_returns_duplicate_marker(self):
        app = build_webhook_app(FakeBot(), self.db, FakePanel(), FakeItpay())
        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            payload = {
                "type": "payment.completed",
                "data": {"id": "itpay-1", "client_payment_id": "p-audit"},
            }
            with patch("services.webhook.process_successful_payment", new=AsyncMock(return_value={"ok": True})):
                first = await client.post("/itpay/webhook", json=payload)
                second = await client.post("/itpay/webhook", json=payload)
            self.assertEqual(first.status, 200)
            self.assertEqual(second.status, 200)
            second_body = await second.json()
            self.assertTrue(second_body.get("duplicate"))
        finally:
            await client.close()
            await server.close()

    async def test_webhook_duplicate_transaction_id_is_deduplicated_even_with_payload_changes(self):
        app = build_webhook_app(FakeBot(), self.db, FakePanel(), FakeItpay())
        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            first_payload = {
                "type": "payment.completed",
                "data": {"id": "itpay-dup-tx", "client_payment_id": "p-audit"},
            }
            second_payload = {
                "type": "payment.completed",
                "data": {"id": "itpay-dup-tx", "client_payment_id": "p-audit", "meta": {"attempt": 2}},
            }
            with patch("services.webhook.process_successful_payment", new=AsyncMock(return_value={"ok": True})):
                first = await client.post("/itpay/webhook", json=first_payload)
                second = await client.post("/itpay/webhook", json=second_payload)
            self.assertEqual(first.status, 200)
            self.assertEqual(second.status, 200)
            second_body = await second.json()
            self.assertTrue(second_body.get("duplicate"))
        finally:
            await client.close()
            await server.close()
