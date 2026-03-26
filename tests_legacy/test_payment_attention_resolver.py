import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from services.payment_attention_resolver import auto_resolve_payment_attention


class PaymentAttentionAutoResolverTests(unittest.IsolatedAsyncioTestCase):
    async def test_resolves_mismatch_success_to_accepted(self):
        db = SimpleNamespace()
        db.get_stale_processing_payments = AsyncMock(return_value=[])
        db.get_overdue_payment_operations = AsyncMock(return_value=[])
        db.get_confirmed_payment_status_mismatches = AsyncMock(return_value=[
            {"payment_id": "pay-1", "provider": "yookassa", "event_type": "payment.succeeded"}
        ])
        db.get_pending_payment = AsyncMock(return_value={
            "payment_id": "pay-1", "provider": "yookassa", "status": "pending", "user_id": 10, "plan_id": "basic", "amount": 100,
        })
        db.add_payment_admin_action = AsyncMock(return_value=1)

        gateway = SimpleNamespace(provider_name="yookassa")
        panel = object()

        with patch("services.payment_attention_resolver.process_successful_payment", new=AsyncMock(return_value={"ok": True})):
            summary = await auto_resolve_payment_attention(db=db, panel=panel, payment_gateway=gateway, bot=None)

        self.assertEqual(summary["mismatch"]["resolved"], 1)
        db.add_payment_admin_action.assert_awaited()

    async def test_releases_stale_processing_without_remote_resolution(self):
        db = SimpleNamespace()
        db.get_stale_processing_payments = AsyncMock(return_value=[
            {"payment_id": "pay-2", "provider": "yookassa", "status": "processing", "user_id": 20}
        ])
        db.get_overdue_payment_operations = AsyncMock(return_value=[])
        db.get_confirmed_payment_status_mismatches = AsyncMock(return_value=[])
        db.release_processing_payment = AsyncMock(return_value=True)
        db.add_payment_admin_action = AsyncMock(return_value=1)

        gateway = SimpleNamespace(provider_name="yookassa", get_payment=AsyncMock(return_value=None))
        panel = object()

        summary = await auto_resolve_payment_attention(db=db, panel=panel, payment_gateway=gateway, bot=None)

        self.assertEqual(summary["processing"]["resolved"], 1)
        db.release_processing_payment.assert_awaited_once()

    async def test_confirms_yookassa_refund_request(self):
        db = SimpleNamespace()
        db.get_stale_processing_payments = AsyncMock(return_value=[])
        db.get_overdue_payment_operations = AsyncMock(return_value=[
            {"payment_id": "pay-3", "provider": "yookassa", "requested_status": "refund_requested", "requested_metadata": "provider_refund_id=ref-1"}
        ])
        db.get_confirmed_payment_status_mismatches = AsyncMock(return_value=[])
        db.get_pending_payment = AsyncMock(return_value={
            "payment_id": "pay-3", "provider": "yookassa", "status": "accepted", "user_id": 30, "provider_payment_id": "yk-1"
        })
        db.update_payment_status = AsyncMock(return_value=True)
        db.record_payment_status_transition = AsyncMock(return_value=1)
        db.add_payment_admin_action = AsyncMock(return_value=1)

        gateway = SimpleNamespace(provider_name="yookassa", get_refund=AsyncMock(return_value={"id": "ref-1", "status": "succeeded"}))
        panel = object()

        with patch("services.payment_attention_resolver.revoke_subscription", new=AsyncMock(return_value=True)):
            summary = await auto_resolve_payment_attention(db=db, panel=panel, payment_gateway=gateway, bot=None)

        self.assertEqual(summary["operations"]["resolved"], 1)
        db.update_payment_status.assert_awaited_once()


    async def test_retry_gate_skips_during_cooldown(self):
        db = SimpleNamespace()
        db.get_stale_processing_payments = AsyncMock(return_value=[
            {"payment_id": "pay-4", "provider": "yookassa", "status": "processing", "user_id": 40}
        ])
        db.get_overdue_payment_operations = AsyncMock(return_value=[])
        db.get_confirmed_payment_status_mismatches = AsyncMock(return_value=[])
        db.get_auto_resolve_action_stats = AsyncMock(return_value={"attempts": 1, "last_created_at": "2999-01-01 00:00:00"})
        db.add_payment_admin_action = AsyncMock(return_value=1)
        db.release_processing_payment = AsyncMock(return_value=True)

        gateway = SimpleNamespace(provider_name="yookassa", get_payment=AsyncMock(return_value=None))
        panel = object()

        summary = await auto_resolve_payment_attention(db=db, panel=panel, payment_gateway=gateway, bot=None)

        self.assertEqual(summary["processing"]["resolved"], 0)
        self.assertEqual(summary["processing"]["skipped"], 1)
        db.release_processing_payment.assert_not_awaited()
        db.add_payment_admin_action.assert_awaited()

    async def test_retry_gate_stops_after_max_attempts(self):
        db = SimpleNamespace()
        db.get_stale_processing_payments = AsyncMock(return_value=[])
        db.get_overdue_payment_operations = AsyncMock(return_value=[])
        db.get_confirmed_payment_status_mismatches = AsyncMock(return_value=[
            {"payment_id": "pay-5", "provider": "yookassa", "event_type": "payment.succeeded"}
        ])
        db.get_auto_resolve_action_stats = AsyncMock(return_value={"attempts": 5, "last_created_at": "2026-01-01 00:00:00"})
        db.add_payment_admin_action = AsyncMock(return_value=1)
        db.get_pending_payment = AsyncMock()

        gateway = SimpleNamespace(provider_name="yookassa")
        panel = object()

        summary = await auto_resolve_payment_attention(db=db, panel=panel, payment_gateway=gateway, bot=None)

        self.assertEqual(summary["mismatch"]["resolved"], 0)
        self.assertEqual(summary["mismatch"]["skipped"], 1)
        db.get_pending_payment.assert_not_awaited()
        db.add_payment_admin_action.assert_awaited()


if __name__ == "__main__":
    unittest.main()
