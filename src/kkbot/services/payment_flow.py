from __future__ import annotations

from typing import Any

import services.payment_flow as legacy_payment_flow

from kkbot.db.postgres import PostgresDatabase
from kkbot.repositories.payments import PaymentRepository


class PaymentFlowService:
    def __init__(self, db: PostgresDatabase):
        self.db = db
        self.repo = PaymentRepository(db)

    async def claim_payment(self, payment_id: str, *, source: str = "", reason: str = "") -> bool:
        return await self.repo.claim_processing(payment_id, source=source, reason=reason)

    async def accept_payment(
        self,
        payment_id: str,
        *,
        source: str = "",
        reason: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        return await self.repo.transition_status(
            payment_id,
            expected_from="processing",
            to_status="accepted",
            source=source,
            reason=reason,
            metadata=metadata,
        )

    async def reject_payment(
        self,
        payment_id: str,
        *,
        source: str = "",
        reason: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        return await self.repo.transition_status(
            payment_id,
            expected_from="processing",
            to_status="rejected",
            source=source,
            reason=reason,
            metadata=metadata,
        )

    async def get_payment(self, payment_id: str) -> dict[str, Any] | None:
        return await self.repo.get_intent(payment_id)


def _resolve_postgres_db(db: object) -> PostgresDatabase | None:
    if isinstance(db, PostgresDatabase):
        return db
    postgres = getattr(db, "postgres", None)
    if isinstance(postgres, PostgresDatabase):
        return postgres
    return None


async def process_successful_payment(
    *,
    payment: dict[str, Any],
    db,
    panel,
    bot=None,
    admin_context: str | None = None,
    apply_referral: bool = True,
) -> dict[str, Any]:
    postgres_db = _resolve_postgres_db(db)
    if postgres_db:
        service = PaymentFlowService(postgres_db)
        payment_id = str(payment["payment_id"])
        current = await service.get_payment(payment_id)
        if current and current.get("status") == "accepted":
            return {"ok": True, "already_processed": True, "payment_id": payment_id, "user_id": int(payment["user_id"])}
        claimed = await service.claim_payment(payment_id, source=admin_context or "payment_flow/success", reason="attempt activation")
        if not claimed:
            return {"ok": False, "reason": "claim_failed", "payment_id": payment_id, "user_id": int(payment["user_id"])}
        accepted = await service.accept_payment(
            payment_id,
            source=admin_context or "payment_flow/success",
            reason="payment accepted",
            metadata={"provider": payment.get("provider", ""), "apply_referral": apply_referral},
        )
        return {"ok": bool(accepted), "payment_id": payment_id, "user_id": int(payment["user_id"])}
    return await legacy_payment_flow.process_successful_payment(
        payment=payment,
        db=db,
        panel=panel,
        bot=bot,
        admin_context=admin_context,
        apply_referral=apply_referral,
    )


async def reject_pending_payment(
    *,
    payment: dict[str, Any],
    db,
    bot=None,
    reason_text: str | None = None,
    admin_context: str | None = None,
) -> dict[str, Any]:
    postgres_db = _resolve_postgres_db(db)
    if postgres_db:
        service = PaymentFlowService(postgres_db)
        payment_id = str(payment["payment_id"])
        current = await service.get_payment(payment_id)
        if current and current.get("status") == "rejected":
            return {"ok": True, "already_processed": True, "payment_id": payment_id, "user_id": int(payment["user_id"])}
        claimed = await service.claim_payment(payment_id, source=admin_context or "payment_flow/reject", reason=reason_text or "reject payment")
        if not claimed:
            return {"ok": False, "reason": "claim_failed", "payment_id": payment_id, "user_id": int(payment["user_id"])}
        rejected = await service.reject_payment(
            payment_id,
            source=admin_context or "payment_flow/reject",
            reason=reason_text or "payment rejected",
        )
        return {"ok": bool(rejected), "payment_id": payment_id, "user_id": int(payment["user_id"])}
    return await legacy_payment_flow.reject_pending_payment(
        payment=payment,
        db=db,
        bot=bot,
        reason_text=reason_text,
        admin_context=admin_context,
    )
