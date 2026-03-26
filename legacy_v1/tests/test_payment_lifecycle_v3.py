import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from db import Database
from services.subscriptions import revoke_subscription


class FakePanel:
    def __init__(self):
        self.deleted = []

    async def delete_client(self, base_email):
        self.deleted.append(base_email)
        return True


class PaymentLifecycleV3Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        await self.db.connect()
        await self.db.add_user(900)
        await self.db.add_pending_payment("pay-v3", 900, "basic", 199.0, provider="yookassa")

    async def asyncTearDown(self):
        await self.db.close()

    async def test_invalid_direct_transition_pending_to_refunded_is_denied(self):
        ok = await self.db.update_payment_status(
            "pay-v3",
            "refunded",
            allowed_current_statuses=["pending"],
            source="test",
        )
        self.assertFalse(ok)
        payment = await self.db.get_pending_payment("pay-v3")
        self.assertEqual(payment["status"], "pending")

    async def test_admin_actions_are_persisted(self):
        action_id = await self.db.add_payment_admin_action(
            "pay-v3", 1, "yookassa_refund", provider="yookassa", result="ok", details="details"
        )
        self.assertGreater(action_id, 0)
        rows = await self.db.get_payment_admin_actions("pay-v3")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["action"], "yookassa_refund")
        self.assertEqual(rows[0]["result"], "ok")

    async def test_revoke_subscription_clears_local_state(self):
        await self.db.set_subscription(900, "Basic", 2, "vpn://test", 50)
        panel = FakePanel()
        with patch("services.subscriptions.notify_user", new=AsyncMock()):
            ok = await revoke_subscription(900, db=self.db, panel=panel, reason="refund")
        self.assertTrue(ok)
        user = await self.db.get_user(900)
        self.assertEqual(user["has_subscription"], 0)
        self.assertEqual(user["vpn_url"], "")
        self.assertTrue(panel.deleted)


class RefundCallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_yookassa_refund_requests_confirmation_and_logs_action(self):
        from handlers.payment_diagnostics import payment_diagnostics_refund_yookassa

        db = SimpleNamespace()
        db.get_pending_payment = AsyncMock(return_value={
            "payment_id": "pay-1",
            "provider": "yookassa",
            "provider_payment_id": "yk-1",
            "status": "accepted",
            "user_id": 111,
            "amount": 200,
        })
        db.update_payment_status = AsyncMock(return_value=True)
        db.record_payment_status_transition = AsyncMock(return_value=1)
        db.add_payment_admin_action = AsyncMock(return_value=1)

        gateway = SimpleNamespace(create_refund=AsyncMock(return_value={"id": "ref-1", "status": "succeeded"}))
        callback = SimpleNamespace(
            from_user=SimpleNamespace(id=1),
            data="paydiag_refund:pay-1",
            answer=AsyncMock(),
            message=SimpleNamespace(edit_text=AsyncMock()),
        )
        bot = SimpleNamespace(send_message=AsyncMock())
        panel = SimpleNamespace(delete_client=AsyncMock(return_value=True))

        with patch("handlers.payment_diagnostics.is_admin", return_value=True), \
             patch("handlers.payment_diagnostics._render_payment_diagnostics", new=AsyncMock(return_value=True)), \
             patch("handlers.payment_diagnostics.notify_admins", new=AsyncMock()), \
             patch("handlers.payment_diagnostics.notify_user", new=AsyncMock()):
            await payment_diagnostics_refund_yookassa(callback, db=db, payment_gateway=gateway, bot=bot, panel=panel)

        db.update_payment_status.assert_not_awaited()
        db.add_payment_admin_action.assert_awaited_once()
        panel.delete_client.assert_not_awaited()

    async def test_yookassa_refund_webhook_confirms_local_refund(self):
        from aiohttp.test_utils import make_mocked_request
        from services.webhook import yookassa_webhook_handler, DB_APP_KEY, PANEL_APP_KEY, PAYMENT_GATEWAY_APP_KEY, BOT_APP_KEY

        db = SimpleNamespace()
        db.register_payment_event = AsyncMock(return_value=True)
        db.get_pending_payment_by_provider_id = AsyncMock(return_value={
            "payment_id": "pay-1",
            "provider": "yookassa",
            "provider_payment_id": "yk-1",
            "status": "accepted",
            "user_id": 111,
            "amount": 200,
        })
        db.update_payment_status = AsyncMock(return_value=True)
        db.record_payment_status_transition = AsyncMock(return_value=1)
        db.add_payment_admin_action = AsyncMock(return_value=1)

        gateway = SimpleNamespace(get_refund=AsyncMock(return_value={"id": "ref-1", "status": "succeeded"}))
        panel = SimpleNamespace(delete_client=AsyncMock(return_value=True))
        request = make_mocked_request(
            'POST',
            '/yookassa/webhook',
            headers={},
            app={DB_APP_KEY: db, PANEL_APP_KEY: panel, PAYMENT_GATEWAY_APP_KEY: gateway, BOT_APP_KEY: object()},
        )
        payload = {"event": "refund.succeeded", "object": {"id": "ref-1", "status": "succeeded", "payment_id": "yk-1"}}
        request._read_bytes = __import__('json').dumps(payload).encode()
        request._cache = {"remote": "127.0.0.1"}

        with patch("services.webhook.Config.YOOKASSA_ENFORCE_IP_CHECK", False), \
             patch("services.webhook.revoke_subscription", new=AsyncMock(return_value=True), create=True):
            response = await yookassa_webhook_handler(request)

        self.assertEqual(response.status, 200)
        db.update_payment_status.assert_awaited_once()
        panel.delete_client.assert_awaited_once()
