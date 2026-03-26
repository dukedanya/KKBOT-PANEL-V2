from __future__ import annotations

import json
from datetime import UTC, datetime
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


class OperationsRepository:
    def __init__(self, db: PostgresDatabase):
        self.db = db

    async def insert_antifraud_event(
        self,
        *,
        user_id: int,
        event_type: str,
        severity: str,
        details: str,
    ) -> int:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow(
                """
                INSERT INTO antifraud_events(user_id, event_type, severity, details)
                VALUES($1, $2, $3, $4)
                RETURNING id
                """,
                user_id,
                event_type,
                severity,
                details,
            )
        return int(row["id"])

    async def list_recent_antifraud_events(self, *, limit: int = 20) -> list[dict[str, Any]]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                "SELECT * FROM antifraud_events ORDER BY created_at DESC LIMIT $1",
                int(limit),
            )
        return [dict(row) for row in rows]

    async def insert_admin_user_action(
        self,
        *,
        user_id: int,
        admin_user_id: int,
        action: str,
        details: str,
    ) -> int:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow(
                """
                INSERT INTO admin_user_actions(user_id, admin_user_id, action, details)
                VALUES($1, $2, $3, $4)
                RETURNING id
                """,
                user_id,
                admin_user_id,
                action,
                details,
            )
        return int(row["id"])

    async def insert_payment_admin_action(
        self,
        *,
        payment_id: str,
        admin_user_id: int,
        action: str,
        provider: str,
        result: str,
        details: str,
    ) -> int:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow(
                """
                INSERT INTO payment_admin_actions(payment_id, admin_user_id, action, provider, result, details)
                VALUES($1, $2, $3, $4, $5, $6)
                RETURNING id
                """,
                payment_id,
                admin_user_id,
                action,
                provider,
                result,
                details,
            )
        return int(row["id"])

    async def list_recent_payment_admin_actions(
        self,
        *,
        limit: int = 20,
        provider: str | None = None,
    ) -> list[dict[str, Any]]:
        params: list[Any] = []
        query = "SELECT * FROM payment_admin_actions"
        if provider:
            params.append(provider)
            query += f" WHERE provider = ${len(params)}"
        params.append(int(limit))
        query += f" ORDER BY created_at DESC, id DESC LIMIT ${len(params)}"
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]

    async def register_payment_event(
        self,
        *,
        event_key: str,
        payment_id: str,
        source: str,
        event_type: str,
        payload_excerpt: str,
    ) -> bool:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            result = await conn.execute(
                """
                INSERT INTO payment_event_dedup(event_key, payment_id, source, event_type, payload_excerpt)
                VALUES($1, $2, $3, $4, $5)
                ON CONFLICT (event_key) DO NOTHING
                """,
                event_key,
                payment_id,
                source,
                event_type,
                payload_excerpt,
            )
        return int(result.split()[-1]) > 0

    async def list_recent_payment_events(self, *, payment_id: str, limit: int = 10) -> list[dict[str, Any]]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                """
                SELECT * FROM payment_event_dedup
                WHERE payment_id = $1
                ORDER BY created_at DESC, event_key DESC
                LIMIT $2
                """,
                payment_id,
                int(limit),
            )
        return [dict(row) for row in rows]

    async def upsert_withdraw_request(self, payload: dict[str, Any]) -> None:
        meta_json = json.dumps({"legacy_payload": payload}, ensure_ascii=False, default=str)
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                """
                INSERT INTO withdraw_requests(id, user_id, amount, status, created_at, processed_at, meta)
                VALUES($1, $2, $3, $4, $5::timestamptz, $6::timestamptz, $7::jsonb)
                ON CONFLICT (id) DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    amount = EXCLUDED.amount,
                    status = EXCLUDED.status,
                    created_at = COALESCE(EXCLUDED.created_at, withdraw_requests.created_at),
                    processed_at = EXCLUDED.processed_at,
                    meta = EXCLUDED.meta
                """,
                int(payload.get("id") or 0),
                int(payload.get("user_id") or 0),
                float(payload.get("amount") or 0),
                str(payload.get("status") or "pending"),
                _dt(payload.get("created_at")),
                _dt(payload.get("processed_at")),
                meta_json,
            )

    async def list_pending_withdraw_requests(self) -> list[dict[str, Any]]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                "SELECT * FROM withdraw_requests WHERE status = 'pending' ORDER BY created_at ASC"
            )
        return [dict(row) for row in rows]

    async def get_pending_withdraw_request_for_user(self, user_id: int) -> dict[str, Any] | None:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow(
                """
                SELECT * FROM withdraw_requests
                WHERE user_id = $1 AND status = 'pending'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                user_id,
            )
        return dict(row) if row else None

    async def list_withdraw_requests_by_user(self, user_id: int, *, limit: int = 10) -> list[dict[str, Any]]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                """
                SELECT * FROM withdraw_requests
                WHERE user_id = $1
                ORDER BY created_at DESC
                LIMIT $2
                """,
                user_id,
                int(limit),
            )
        return [dict(row) for row in rows]

    async def upsert_support_ticket(self, payload: dict[str, Any]) -> None:
        meta_json = json.dumps({"legacy_payload": payload}, ensure_ascii=False, default=str)
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                """
                INSERT INTO support_tickets(id, user_id, status, assigned_admin_id, created_at, updated_at, meta)
                VALUES($1, $2, $3, $4, $5::timestamptz, $6::timestamptz, $7::jsonb)
                ON CONFLICT (id) DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    status = EXCLUDED.status,
                    assigned_admin_id = EXCLUDED.assigned_admin_id,
                    created_at = COALESCE(EXCLUDED.created_at, support_tickets.created_at),
                    updated_at = EXCLUDED.updated_at,
                    meta = EXCLUDED.meta
                """,
                int(payload.get("id") or 0),
                int(payload.get("user_id") or 0),
                str(payload.get("status") or "open"),
                payload.get("assigned_admin_id"),
                _dt(payload.get("created_at")),
                _dt(payload.get("updated_at")),
                meta_json,
            )

    async def get_support_ticket(self, ticket_id: int) -> dict[str, Any] | None:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow("SELECT * FROM support_tickets WHERE id = $1", ticket_id)
        return dict(row) if row else None

    async def list_open_support_tickets(self, *, limit: int = 20) -> list[dict[str, Any]]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                """
                SELECT * FROM support_tickets
                WHERE status IN ('open', 'in_progress')
                ORDER BY updated_at DESC NULLS LAST
                LIMIT $1
                """,
                int(limit),
            )
        return [dict(row) for row in rows]

    async def add_support_message(self, payload: dict[str, Any]) -> None:
        meta_json = json.dumps({"legacy_payload": payload}, ensure_ascii=False, default=str)
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                """
                INSERT INTO support_messages(id, ticket_id, sender_role, sender_user_id, text, media_type, media_file_id, created_at, meta)
                VALUES($1, $2, $3, $4, $5, $6, $7, $8::timestamptz, $9::jsonb)
                ON CONFLICT (id) DO UPDATE SET
                    ticket_id = EXCLUDED.ticket_id,
                    sender_role = EXCLUDED.sender_role,
                    sender_user_id = EXCLUDED.sender_user_id,
                    text = EXCLUDED.text,
                    media_type = EXCLUDED.media_type,
                    media_file_id = EXCLUDED.media_file_id,
                    created_at = COALESCE(EXCLUDED.created_at, support_messages.created_at),
                    meta = EXCLUDED.meta
                """,
                int(payload.get("id") or 0),
                int(payload.get("ticket_id") or 0),
                str(payload.get("sender_role") or ""),
                int(payload.get("sender_user_id") or 0),
                str(payload.get("text") or ""),
                str(payload.get("media_type") or ""),
                str(payload.get("media_file_id") or ""),
                _dt(payload.get("created_at")),
                meta_json,
            )

    async def list_support_messages(self, ticket_id: int, *, limit: int = 100) -> list[dict[str, Any]]:
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                """
                SELECT * FROM support_messages
                WHERE ticket_id = $1
                ORDER BY id ASC
                LIMIT $2
                """,
                ticket_id,
                int(limit),
            )
        return [dict(row) for row in rows]

    async def get_last_support_message(self, ticket_id: int, *, sender_role: str | None = None) -> dict[str, Any] | None:
        params: list[Any] = [ticket_id]
        query = "SELECT * FROM support_messages WHERE ticket_id = $1"
        if sender_role:
            params.append(sender_role)
            query += f" AND sender_role = ${len(params)}"
        query += " ORDER BY id DESC LIMIT 1"
        async with self.db.pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow(query, *params)
        return dict(row) if row else None
