from __future__ import annotations

import json
from typing import Any

import services.payment_flow as legacy_payment_flow

from kkbot.db.postgres import PostgresDatabase
from kkbot.repositories.payments import PaymentRepository
from config import Config


def _coerce_payload_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return dict(parsed)
    try:
        return dict(value)
    except Exception:
        return {}


def _default_user_payload(user_id: int) -> dict[str, Any]:
    return {
        "user_id": int(user_id),
        "join_date": "",
        "banned": 0,
        "ban_reason": "",
        "ref_code": "",
        "ref_by": 0,
        "ref_rewarded": 0,
        "bonus_days_pending": 0,
        "trial_used": 0,
        "trial_declined": 0,
        "has_subscription": 0,
        "plan_text": "",
        "ip_limit": 0,
        "traffic_gb": 0,
        "vpn_url": "",
        "balance": 0.0,
        "partner_percent_level1": None,
        "partner_percent_level2": None,
        "partner_percent_level3": None,
        "partner_status": "standard",
        "partner_note": "",
        "ref_suspicious": 0,
        "username": "",
        "first_name": "",
        "last_name": "",
        "language_code": "",
        "notified_3d": 0,
        "notified_1d": 0,
        "notified_1h": 0,
        "frozen_until": None,
    }


async def _get_legacy_user_payload(db: PostgresDatabase, user_id: int) -> dict[str, Any] | None:
    async with db.pool.acquire() as conn:  # type: ignore[union-attr]
        row = await conn.fetchrow(
            """
            SELECT payload
            FROM legacy_users_archive
            WHERE user_id = $1
            """,
            int(user_id),
        )
    if not row:
        return None
    payload = _coerce_payload_dict(row["payload"])
    if not payload:
        return None
    payload.setdefault("user_id", int(user_id))
    return payload


async def _upsert_legacy_user_payload(conn, payload: dict[str, Any]) -> None:
    user_id = int(payload.get("user_id") or 0)
    payload_json = json.dumps(payload, ensure_ascii=False, default=str)
    await conn.execute(
        """
        INSERT INTO legacy_users_archive(user_id, payload, imported_at)
        VALUES($1, $2::jsonb, NOW())
        ON CONFLICT (user_id) DO UPDATE SET
            payload = EXCLUDED.payload,
            imported_at = NOW()
        """,
        user_id,
        payload_json,
    )
    await conn.execute(
        """
        INSERT INTO app_meta(key, value, updated_at)
        VALUES($1, $2::jsonb, NOW())
        ON CONFLICT (key)
        DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """,
        f"legacy_user:{user_id}",
        payload_json,
    )


async def _apply_referral_reward_postgres(
    *,
    db: PostgresDatabase,
    user_id: int,
    amount: float,
) -> list[dict[str, Any]]:
    buyer_payload = await _get_legacy_user_payload(db, user_id)
    if not buyer_payload:
        return []

    ref_by = int(buyer_payload.get("ref_by") or 0)
    ref_rewarded = int(buyer_payload.get("ref_rewarded") or 0)
    if ref_by <= 0 or ref_rewarded:
        return []

    levels = [
        (1, float(Config.REF_PERCENT_LEVEL1 or 0), "за реферала"),
        (2, float(Config.REF_PERCENT_LEVEL2 or 0), "за реферала второго уровня"),
        (3, float(Config.REF_PERCENT_LEVEL3 or 0), "за реферала третьего уровня"),
    ]
    results: list[dict[str, Any]] = []
    visited = {int(user_id)}
    current_referrer_id = ref_by

    async with db.pool.acquire() as conn:  # type: ignore[union-attr]
        async with conn.transaction():
            for level, default_percent, label in levels:
                if current_referrer_id <= 0 or current_referrer_id in visited:
                    break

                row = await conn.fetchrow(
                    """
                    SELECT payload
                    FROM legacy_users_archive
                    WHERE user_id = $1
                    """,
                    int(current_referrer_id),
                )
                if not row:
                    break

                referrer_payload = _coerce_payload_dict(row["payload"]) or _default_user_payload(current_referrer_id)
                referrer_payload["user_id"] = int(current_referrer_id)

                override = referrer_payload.get(f"partner_percent_level{level}")
                percent = float(override if override is not None else default_percent)
                payout = round(float(amount or 0) * percent / 100.0, 2)
                payout = min(payout, float(Config.MAX_DAILY_REF_BONUS_RUB or payout))
                if payout > 0:
                    referrer_payload["balance"] = round(float(referrer_payload.get("balance") or 0.0) + payout, 2)
                    if level == 1:
                        referrer_payload["ref_rewarded_count"] = int(referrer_payload.get("ref_rewarded_count") or 0) + 1
                    await _upsert_legacy_user_payload(conn, referrer_payload)
                    await conn.execute(
                        """
                        INSERT INTO ref_history(user_id, ref_user_id, amount, bonus_days)
                        VALUES($1, $2, $3, 0)
                        """,
                        int(current_referrer_id),
                        int(user_id),
                        float(payout),
                    )
                    results.append(
                        {
                            "user_id": int(current_referrer_id),
                            "amount": float(payout),
                            "label": label,
                            "level": int(level),
                        }
                    )

                visited.add(int(current_referrer_id))
                current_referrer_id = int(referrer_payload.get("ref_by") or 0)

            buyer_payload["ref_rewarded"] = 1
            await _upsert_legacy_user_payload(conn, buyer_payload)

    return results


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
    return db if isinstance(db, PostgresDatabase) else None


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
        user_id = int(payment["user_id"])
        current = await service.get_payment(payment_id)
        if current and current.get("status") == "accepted":
            return {"ok": True, "already_processed": True, "payment_id": payment_id, "user_id": user_id}
        claimed = await service.claim_payment(payment_id, source=admin_context or "payment_flow/success", reason="attempt activation")
        if not claimed:
            return {"ok": False, "reason": "claim_failed", "payment_id": payment_id, "user_id": user_id}
        accepted = await service.accept_payment(
            payment_id,
            source=admin_context or "payment_flow/success",
            reason="payment accepted",
            metadata={"provider": payment.get("provider", ""), "apply_referral": apply_referral},
        )
        if accepted and apply_referral:
            try:
                referral_rewards = await _apply_referral_reward_postgres(
                    db=postgres_db,
                    user_id=user_id,
                    amount=float(payment.get("amount", 0) or 0),
                )
            except Exception:
                referral_rewards = []
        else:
            referral_rewards = []
        return {
            "ok": bool(accepted),
            "payment_id": payment_id,
            "user_id": user_id,
            "referral_rewards": referral_rewards,
        }
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
