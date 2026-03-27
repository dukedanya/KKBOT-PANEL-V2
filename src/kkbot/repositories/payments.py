from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Iterable
from typing import Any

from kkbot.db.postgres import PostgresDatabase


def _dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        normalized = raw.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    return None


class PaymentRepository:
    def __init__(self, db: PostgresDatabase):
        self.db = db

    async def create_intent(self, payload: dict[str, Any]) -> bool:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            result = await conn.execute(
                """
                INSERT INTO payment_intents(
                    payment_id, user_id, plan_id, amount, status, provider, provider_payment_id, msg_id,
                    recipient_user_id, promo_code, promo_discount_percent, gift_label, last_error,
                    activation_attempts, created_at, processed_at, processing_started_at, next_retry_at, updated_at, meta
                )
                VALUES(
                    $1, $2, $3, $4, $5, $6, $7, $8,
                    $9, $10, $11, $12, $13,
                    $14, NOW(), NULL, NULL, NULL, NOW(), $15::jsonb
                )
                ON CONFLICT (payment_id) DO NOTHING
                """,
                str(payload.get("payment_id") or ""),
                int(payload.get("user_id") or 0),
                str(payload.get("plan_id") or ""),
                float(payload.get("amount") or 0),
                str(payload.get("status") or "pending"),
                str(payload.get("provider") or ""),
                str(payload.get("provider_payment_id") or payload.get("itpay_id") or ""),
                payload.get("msg_id"),
                payload.get("recipient_user_id"),
                str(payload.get("promo_code") or ""),
                float(payload.get("promo_discount_percent") or 0),
                str(payload.get("gift_label") or ""),
                str(payload.get("last_error") or ""),
                int(payload.get("activation_attempts") or 0),
                json.dumps({"legacy_payload": payload}, ensure_ascii=False, default=str),
            )
        return int(result.split()[-1]) > 0

    async def get_intent(self, payment_id: str) -> dict[str, Any] | None:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow(
                "SELECT * FROM payment_intents WHERE payment_id = $1",
                payment_id,
            )
        return dict(row) if row else None

    async def upsert_legacy_intent(self, payload: dict[str, Any]) -> None:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                """
                INSERT INTO payment_intents(
                    payment_id, user_id, plan_id, amount, status, provider, provider_payment_id, msg_id,
                    recipient_user_id, promo_code, promo_discount_percent, gift_label, last_error,
                    activation_attempts, created_at, processed_at, processing_started_at, next_retry_at, updated_at, meta
                )
                VALUES(
                    $1, $2, $3, $4, $5, $6, $7, $8,
                    $9, $10, $11, $12, $13,
                    $14, $15::timestamptz, $16::timestamptz, $17::timestamptz, $18::timestamptz, NOW(), $19::jsonb
                )
                ON CONFLICT (payment_id) DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    plan_id = EXCLUDED.plan_id,
                    amount = EXCLUDED.amount,
                    status = EXCLUDED.status,
                    provider = EXCLUDED.provider,
                    provider_payment_id = EXCLUDED.provider_payment_id,
                    msg_id = EXCLUDED.msg_id,
                    recipient_user_id = EXCLUDED.recipient_user_id,
                    promo_code = EXCLUDED.promo_code,
                    promo_discount_percent = EXCLUDED.promo_discount_percent,
                    gift_label = EXCLUDED.gift_label,
                    last_error = EXCLUDED.last_error,
                    activation_attempts = EXCLUDED.activation_attempts,
                    created_at = COALESCE(EXCLUDED.created_at, payment_intents.created_at),
                    processed_at = EXCLUDED.processed_at,
                    processing_started_at = EXCLUDED.processing_started_at,
                    next_retry_at = EXCLUDED.next_retry_at,
                    updated_at = NOW(),
                    meta = EXCLUDED.meta
                """,
                str(payload.get("payment_id") or ""),
                int(payload.get("user_id") or 0),
                str(payload.get("plan_id") or ""),
                float(payload.get("amount") or 0),
                str(payload.get("status") or "pending"),
                str(payload.get("provider") or ""),
                str(payload.get("provider_payment_id") or payload.get("itpay_id") or ""),
                payload.get("msg_id"),
                payload.get("recipient_user_id"),
                str(payload.get("promo_code") or ""),
                float(payload.get("promo_discount_percent") or 0),
                str(payload.get("gift_label") or ""),
                str(payload.get("last_error") or ""),
                int(payload.get("activation_attempts") or 0),
                _dt(payload.get("created_at")),
                _dt(payload.get("processed_at")),
                _dt(payload.get("processing_started_at")),
                _dt(payload.get("next_retry_at")),
                json.dumps({"legacy_payload": payload}, ensure_ascii=False, default=str),
            )

    async def set_provider_payment_id(self, payment_id: str, *, provider: str, provider_payment_id: str) -> bool:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            result = await conn.execute(
                """
                UPDATE payment_intents
                SET provider = $2,
                    provider_payment_id = $3,
                    updated_at = NOW(),
                    meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{legacy_payload,provider_payment_id}', to_jsonb($3::text), true)
                WHERE payment_id = $1
                """,
                payment_id,
                provider,
                provider_payment_id,
            )
        return int(result.split()[-1]) > 0

    async def append_status_history(
        self,
        payment_id: str,
        *,
        from_status: str | None,
        to_status: str,
        source: str = "",
        reason: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> int:
        payload = json.dumps(metadata or {}, ensure_ascii=False)
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow(
                """
                INSERT INTO payment_status_history(payment_id, from_status, to_status, source, reason, metadata_json, created_at)
                VALUES($1, $2, $3, $4, $5, $6::jsonb, NOW())
                RETURNING id
                """,
                payment_id,
                from_status,
                to_status,
                source,
                reason,
                payload,
            )
        return int(row["id"])

    async def mark_error(self, payment_id: str, error_text: str) -> bool:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            result = await conn.execute(
                """
                UPDATE payment_intents
                SET last_error = $2,
                    updated_at = NOW()
                WHERE payment_id = $1
                """,
                payment_id,
                error_text[:500],
            )
        return int(result.split()[-1]) > 0

    async def count_by_status(self, status: str) -> int:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            value = await conn.fetchval("SELECT COUNT(*) FROM payment_intents WHERE status = $1", status)
        return int(value or 0)

    async def list_pending_older_than_minutes(self, minutes: int) -> list[dict[str, Any]]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                """
                SELECT * FROM payment_intents
                WHERE status = 'pending' AND created_at < NOW() - make_interval(mins => $1::int)
                ORDER BY created_at ASC
                """,
                int(minutes),
            )
        return [dict(row) for row in rows]

    async def list_recent_errors(self, hours: int) -> list[dict[str, Any]]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                """
                SELECT * FROM payment_intents
                WHERE COALESCE(last_error, '') != ''
                  AND created_at >= NOW() - make_interval(hours => $1::int)
                ORDER BY created_at DESC
                """,
                int(hours),
            )
        return [dict(row) for row in rows]

    async def list_by_user(self, user_id: int, statuses: Iterable[str] | None = None) -> list[dict[str, Any]]:
        clauses = ["user_id = $1"]
        params: list[Any] = [user_id]
        if statuses:
            params.append(list(statuses))
            clauses.append(f"status = ANY(${len(params)}::text[])")
        query = f"SELECT * FROM payment_intents WHERE {' AND '.join(clauses)} ORDER BY created_at DESC"
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]

    async def get_by_provider_payment_id(self, *, provider: str, provider_payment_id: str) -> dict[str, Any] | None:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow(
                """
                SELECT * FROM payment_intents
                WHERE provider = $1 AND provider_payment_id = $2
                LIMIT 1
                """,
                provider,
                provider_payment_id,
            )
        return dict(row) if row else None

    async def list_by_statuses(self, statuses: Iterable[str]) -> list[dict[str, Any]]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                """
                SELECT * FROM payment_intents
                WHERE status = ANY($1::text[])
                ORDER BY created_at ASC
                """,
                list(statuses),
            )
        return [dict(row) for row in rows]

    async def release_processing(
        self,
        payment_id: str,
        *,
        error_text: str | None = None,
        retry_delay_sec: int = 0,
        source: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        payload = json.dumps(metadata or {}, ensure_ascii=False)
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            async with conn.transaction():
                result = await conn.execute(
                    """
                    UPDATE payment_intents
                    SET status = 'pending',
                        processed_at = NULL,
                        processing_started_at = NULL,
                        next_retry_at = CASE
                            WHEN $3 > 0 THEN NOW() + make_interval(secs => $3::int)
                            ELSE NULL
                        END,
                        last_error = COALESCE($2, last_error),
                        updated_at = NOW()
                    WHERE payment_id = $1 AND status = 'processing'
                    """,
                    payment_id,
                    error_text,
                    int(retry_delay_sec),
                )
                updated = int(result.split()[-1])
                if updated:
                    await conn.execute(
                        """
                        INSERT INTO payment_status_history(payment_id, from_status, to_status, source, reason, metadata_json, created_at)
                        VALUES($1, 'processing', 'pending', $2, $3, $4::jsonb, NOW())
                        """,
                        payment_id,
                        source,
                        error_text or "",
                        payload,
                    )
        return updated > 0

    async def list_stale_processing(self, *, minutes: int = 15, limit: int = 20, provider: str | None = None) -> list[dict[str, Any]]:
        params: list[Any] = [int(minutes)]
        query = """
            SELECT * FROM payment_intents
            WHERE status = 'processing'
              AND processing_started_at IS NOT NULL
              AND processing_started_at < NOW() - make_interval(mins => $1::int)
        """
        if provider and provider != "all":
            params.append(provider)
            query += f" AND provider = ${len(params)}"
        params.append(int(limit))
        query += f" ORDER BY processing_started_at ASC, created_at ASC LIMIT ${len(params)}"
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]

    async def list_confirmed_mismatches(self, *, hours: int = 24, limit: int = 20, provider: str | None = None) -> list[dict[str, Any]]:
        params: list[Any] = [int(hours)]
        query = """
            SELECT
                p.payment_id,
                p.user_id,
                p.plan_id,
                p.amount,
                p.provider,
                p.provider_payment_id,
                p.status,
                p.created_at,
                e.event_type,
                e.source AS event_source,
                e.payload_excerpt,
                e.created_at AS event_created_at
            FROM payment_intents p
            JOIN (
                SELECT DISTINCT ON (payment_id)
                    payment_id, event_type, source, payload_excerpt, created_at
                FROM payment_event_dedup
                WHERE payment_id != ''
                  AND created_at >= NOW() - make_interval(hours => $1::int)
                  AND event_type IN ('payment.succeeded', 'payment.completed', 'payment.pay', 'successful_payment', 'payment.canceled', 'refund.succeeded')
                ORDER BY payment_id, created_at DESC, event_key DESC
            ) e ON e.payment_id = p.payment_id
            WHERE (
                (e.event_type IN ('payment.succeeded', 'payment.completed', 'payment.pay', 'successful_payment') AND p.status NOT IN ('accepted', 'refunded'))
                OR (e.event_type IN ('payment.canceled') AND p.status NOT IN ('rejected', 'cancelled'))
                OR (e.event_type IN ('refund.succeeded') AND p.status != 'refunded')
            )
        """
        if provider and provider != "all":
            params.append(provider)
            query += f" AND p.provider = ${len(params)}"
        params.append(int(limit))
        query += f" ORDER BY e.created_at DESC, p.created_at DESC LIMIT ${len(params)}"
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]

    async def claim_processing(self, payment_id: str, *, source: str = "", reason: str = "") -> bool:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            async with conn.transaction():
                result = await conn.execute(
                    """
                    UPDATE payment_intents
                    SET status = 'processing',
                        processing_started_at = NOW(),
                        activation_attempts = activation_attempts + 1,
                        next_retry_at = NULL
                    WHERE payment_id = $1 AND status = 'pending'
                    """,
                    payment_id,
                )
                updated = int(result.split()[-1])
                if updated:
                    await conn.execute(
                        """
                        INSERT INTO payment_status_history(payment_id, from_status, to_status, source, reason, metadata_json, created_at)
                        VALUES($1, 'pending', 'processing', $2, $3, '{}'::jsonb, NOW())
                        """,
                        payment_id,
                        source,
                        reason,
                    )
        return updated > 0

    async def transition_status(
        self,
        payment_id: str,
        *,
        expected_from: str,
        to_status: str,
        source: str = "",
        reason: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        payload = json.dumps(metadata or {}, ensure_ascii=False)
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            async with conn.transaction():
                result = await conn.execute(
                    """
                    UPDATE payment_intents
                    SET status = $3,
                        processed_at = CASE WHEN $3 IN ('accepted', 'rejected', 'cancelled') THEN NOW() ELSE processed_at END,
                        updated_at = NOW()
                    WHERE payment_id = $1 AND status = $2
                    """,
                    payment_id,
                    expected_from,
                    to_status,
                )
                updated = int(result.split()[-1])
                if updated:
                    await conn.execute(
                        """
                        INSERT INTO payment_status_history(payment_id, from_status, to_status, source, reason, metadata_json, created_at)
                        VALUES($1, $2, $3, $4, $5, $6::jsonb, NOW())
                        """,
                        payment_id,
                        expected_from,
                        to_status,
                        source,
                        reason,
                        payload,
                    )
        return updated > 0
