import asyncio
import unittest

from kkbot.services.payment_flow import PaymentFlowService
from kkbot.services.subscriptions import SubscriptionService


class _FakeSubscriptionRepo:
    def __init__(self) -> None:
        self.calls = []

    async def replace_active_with_new(self, **kwargs):
        self.calls.append(("replace", kwargs))
        return 77

    async def revoke_active(self, user_id: int, *, reason: str = ""):
        self.calls.append(("revoke", {"user_id": user_id, "reason": reason}))
        return 1

    async def get_latest_for_user(self, user_id: int):
        self.calls.append(("get_latest", {"user_id": user_id}))
        return {"user_id": user_id, "status": "active", "plan_code": "pro"}


class _FakePaymentRepo:
    def __init__(self) -> None:
        self.calls = []

    async def claim_processing(self, payment_id: str, *, source: str = "", reason: str = ""):
        self.calls.append(("claim", {"payment_id": payment_id, "source": source, "reason": reason}))
        return True

    async def transition_status(self, payment_id: str, **kwargs):
        self.calls.append(("transition", {"payment_id": payment_id, **kwargs}))
        return True

    async def get_intent(self, payment_id: str):
        self.calls.append(("get", {"payment_id": payment_id}))
        return {"payment_id": payment_id, "status": "processing"}


class V2BusinessServiceTests(unittest.TestCase):
    def test_subscription_service_uses_postgres_style_repo(self) -> None:
        service = SubscriptionService(db=None)  # type: ignore[arg-type]
        service.repo = _FakeSubscriptionRepo()

        created_id = asyncio.run(
            service.create_panel_subscription(
                user_id=42,
                plan_code="pro",
                traffic_limit_gb=10,
                vpn_url="https://sub",
                panel_email="42@example.com",
                panel_sub_id="user42",
                panel_client_uuid="uuid-1",
                created_inbounds=[1, 2, 3],
                ip_limit=2,
            )
        )
        self.assertEqual(created_id, 77)

        status = asyncio.run(service.get_subscription_status(42))
        self.assertTrue(status["active"])
        self.assertEqual(status["status"], "active")

        revoked = asyncio.run(service.revoke_subscription(42, reason="manual"))
        self.assertTrue(revoked)

    def test_payment_flow_service_routes_calls_to_repo(self) -> None:
        service = PaymentFlowService(db=None)  # type: ignore[arg-type]
        service.repo = _FakePaymentRepo()

        self.assertTrue(asyncio.run(service.claim_payment("pay-1", source="test", reason="claim")))
        self.assertTrue(
            asyncio.run(service.accept_payment("pay-1", source="test", reason="accept", metadata={"ok": True}))
        )
        self.assertTrue(
            asyncio.run(service.reject_payment("pay-2", source="test", reason="reject", metadata={"ok": False}))
        )

        payment = asyncio.run(service.get_payment("pay-1"))
        self.assertEqual(payment["payment_id"], "pay-1")
