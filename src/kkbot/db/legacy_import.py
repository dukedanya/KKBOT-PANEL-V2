from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import asyncpg


@dataclass(frozen=True, slots=True)
class ImportReport:
    users: int = 0
    subscriptions: int = 0
    withdraw_requests: int = 0
    payment_intents: int = 0
    payment_status_history: int = 0
    support_tickets: int = 0
    support_messages: int = 0

    @property
    def total_rows(self) -> int:
        return (
            self.users
            + self.subscriptions
            + self.withdraw_requests
            + self.payment_intents
            + self.payment_status_history
            + self.support_tickets
            + self.support_messages
        )


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _rows(conn: sqlite3.Connection, table_name: str) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    if not _table_exists(conn, table_name):
        return []
    return [dict(row) for row in conn.execute(f'SELECT * FROM "{table_name}"').fetchall()]


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


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


async def _start_import_run(pg_conn: "asyncpg.Connection", source_path: Path) -> int:
    row = await pg_conn.fetchrow(
        """
        INSERT INTO legacy_import_runs(source_path, status, details)
        VALUES($1, 'started', '{}'::jsonb)
        RETURNING id
        """,
        str(source_path),
    )
    return int(row["id"])


async def _finish_import_run(pg_conn: "asyncpg.Connection", run_id: int, status: str, details: dict[str, Any]) -> None:
    await pg_conn.execute(
        """
        UPDATE legacy_import_runs
        SET status = $2,
            details = $3::jsonb,
            finished_at = NOW()
        WHERE id = $1
        """,
        run_id,
        status,
        _json(details),
    )


async def import_legacy_sqlite_to_postgres(sqlite_path: Path, pg_pool: "asyncpg.Pool", *, batch_size: int = 1000) -> ImportReport:
    del batch_size  # targeted import currently uses full-table reads by entity

    report = ImportReport()
    with sqlite3.connect(sqlite_path) as sqlite_conn:
        users = _rows(sqlite_conn, "users")
        withdraw_requests = _rows(sqlite_conn, "withdraw_requests")
        payment_intents = _rows(sqlite_conn, "pending_payments")
        payment_history = _rows(sqlite_conn, "payment_status_history")
        support_tickets = _rows(sqlite_conn, "support_tickets")
        support_messages = _rows(sqlite_conn, "support_messages")

    async with pg_pool.acquire() as pg_conn:
        run_id = await _start_import_run(pg_conn, sqlite_path)
        try:
            async with pg_conn.transaction():
                await pg_conn.execute(
                    """
                    DELETE FROM subscriptions
                    WHERE COALESCE(meta->>'legacy_imported', 'false') = 'true'
                    """
                )
                await pg_conn.execute(
                    """
                    DELETE FROM payment_status_history
                    WHERE COALESCE(metadata_json->>'legacy_imported', 'false') = 'true'
                    """
                )

                if users:
                    for row in users:
                        user_id = int(row["user_id"])
                        await pg_conn.execute(
                            """
                            INSERT INTO legacy_users_archive(user_id, payload)
                            VALUES($1, $2::jsonb)
                            ON CONFLICT (user_id) DO UPDATE SET
                                payload = EXCLUDED.payload,
                                imported_at = NOW()
                            """,
                            user_id,
                            _json(row),
                        )
                        await pg_conn.execute(
                            """
                            INSERT INTO bot_users(user_id, username, first_name, last_name, language_code, is_admin, created_at, updated_at)
                            VALUES($1, NULL, NULL, NULL, NULL, FALSE, COALESCE($2::timestamptz, NOW()), NOW())
                            ON CONFLICT (user_id) DO UPDATE SET updated_at = NOW()
                            """,
                            user_id,
                            _dt(row.get("join_date")),
                        )
                        has_subscription = int(row.get("has_subscription") or 0) == 1
                        banned = int(row.get("banned") or 0) == 1
                        if has_subscription:
                            traffic_gb = int(row.get("traffic_gb") or 0)
                            traffic_limit_bytes = traffic_gb * 1024 * 1024 * 1024
                            await pg_conn.execute(
                                """
                                INSERT INTO subscriptions(user_id, status, plan_code, traffic_limit_bytes, traffic_used_bytes, expires_at, meta)
                                VALUES($1, $2, $3, $4, 0, NULL, $5::jsonb)
                                """,
                                user_id,
                                "disabled" if banned else "active",
                                str(row.get("plan_text") or ""),
                                traffic_limit_bytes,
                                _json(
                                    {
                                        "legacy_imported": True,
                                        "legacy_vpn_url": row.get("vpn_url") or "",
                                        "legacy_ip_limit": row.get("ip_limit") or 0,
                                        "legacy_ref_code": row.get("ref_code") or "",
                                        "legacy_ref_by": row.get("ref_by"),
                                        "legacy_bonus_days_pending": row.get("bonus_days_pending") or 0,
                                        "legacy_trial_used": row.get("trial_used") or 0,
                                        "legacy_trial_declined": row.get("trial_declined") or 0,
                                        "legacy_partner_status": row.get("partner_status") or "",
                                        "legacy_panel_client_key": row.get("panel_client_key") or "",
                                        "legacy_ban_reason": row.get("ban_reason") or "",
                                    }
                                ),
                            )
                            report = ImportReport(
                                users=report.users + 1,
                                subscriptions=report.subscriptions + 1,
                                withdraw_requests=report.withdraw_requests,
                                payment_intents=report.payment_intents,
                                payment_status_history=report.payment_status_history,
                                support_tickets=report.support_tickets,
                                support_messages=report.support_messages,
                            )
                        else:
                            report = ImportReport(
                                users=report.users + 1,
                                subscriptions=report.subscriptions,
                                withdraw_requests=report.withdraw_requests,
                                payment_intents=report.payment_intents,
                                payment_status_history=report.payment_status_history,
                                support_tickets=report.support_tickets,
                                support_messages=report.support_messages,
                            )

                if withdraw_requests:
                    for row in withdraw_requests:
                        req_id = int(row["id"])
                        await pg_conn.execute(
                            """
                            INSERT INTO legacy_withdraw_requests_archive(id, payload)
                            VALUES($1, $2::jsonb)
                            ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, imported_at = NOW()
                            """,
                            req_id,
                            _json(row),
                        )
                        await pg_conn.execute(
                            """
                            INSERT INTO withdraw_requests(id, user_id, amount, status, created_at, processed_at, meta)
                            VALUES($1, $2, $3, $4, $5::timestamptz, $6::timestamptz, $7::jsonb)
                            ON CONFLICT (id) DO UPDATE SET
                                status = EXCLUDED.status,
                                processed_at = EXCLUDED.processed_at,
                                meta = EXCLUDED.meta
                            """,
                            req_id,
                            int(row["user_id"]),
                            float(row.get("amount") or 0),
                            str(row.get("status") or "pending"),
                            _dt(row.get("created_at")),
                            _dt(row.get("processed_at")),
                            _json({"legacy": True}),
                        )
                    report = ImportReport(
                        users=report.users,
                        subscriptions=report.subscriptions,
                        withdraw_requests=report.withdraw_requests + len(withdraw_requests),
                        payment_intents=report.payment_intents,
                        payment_status_history=report.payment_status_history,
                        support_tickets=report.support_tickets,
                        support_messages=report.support_messages,
                    )

                if payment_intents:
                    for row in payment_intents:
                        payment_id = str(row["payment_id"])
                        await pg_conn.execute(
                            """
                            INSERT INTO legacy_payment_intents_archive(payment_id, payload)
                            VALUES($1, $2::jsonb)
                            ON CONFLICT (payment_id) DO UPDATE SET payload = EXCLUDED.payload, imported_at = NOW()
                            """,
                            payment_id,
                            _json(row),
                        )
                        await pg_conn.execute(
                            """
                            INSERT INTO payment_intents(
                                payment_id, user_id, plan_id, amount, status, provider, provider_payment_id, msg_id,
                                recipient_user_id, promo_code, promo_discount_percent, gift_label, last_error,
                                activation_attempts, created_at, processed_at, processing_started_at, next_retry_at, meta
                            )
                            VALUES(
                                $1, $2, $3, $4, $5, $6, $7, $8,
                                $9, $10, $11, $12, $13,
                                $14, $15::timestamptz, $16::timestamptz, $17::timestamptz, $18::timestamptz, $19::jsonb
                            )
                            ON CONFLICT (payment_id) DO UPDATE SET
                                status = EXCLUDED.status,
                                provider = EXCLUDED.provider,
                                provider_payment_id = EXCLUDED.provider_payment_id,
                                processed_at = EXCLUDED.processed_at,
                                processing_started_at = EXCLUDED.processing_started_at,
                                next_retry_at = EXCLUDED.next_retry_at,
                                last_error = EXCLUDED.last_error,
                                activation_attempts = EXCLUDED.activation_attempts,
                                meta = EXCLUDED.meta
                            """,
                            payment_id,
                            int(row["user_id"]),
                            str(row.get("plan_id") or ""),
                            float(row.get("amount") or 0),
                            str(row.get("status") or "pending"),
                            str(row.get("provider") or ""),
                            str(row.get("provider_payment_id") or row.get("itpay_id") or ""),
                            row.get("msg_id"),
                            row.get("recipient_user_id"),
                            str(row.get("promo_code") or ""),
                            float(row.get("promo_discount_percent") or 0),
                            str(row.get("gift_label") or ""),
                            str(row.get("last_error") or ""),
                            int(row.get("activation_attempts") or 0),
                            _dt(row.get("created_at")),
                            _dt(row.get("processed_at")),
                            _dt(row.get("processing_started_at")),
                            _dt(row.get("next_retry_at")),
                            _json({"legacy_imported": True, "legacy_itpay_id": row.get("itpay_id") or ""}),
                        )
                    report = ImportReport(
                        users=report.users,
                        subscriptions=report.subscriptions,
                        withdraw_requests=report.withdraw_requests,
                        payment_intents=report.payment_intents + len(payment_intents),
                        payment_status_history=report.payment_status_history,
                        support_tickets=report.support_tickets,
                        support_messages=report.support_messages,
                    )

                if payment_history:
                    for row in payment_history:
                        await pg_conn.execute(
                            """
                            INSERT INTO payment_status_history(payment_id, from_status, to_status, source, reason, metadata_json, created_at)
                            VALUES($1, $2, $3, $4, $5, $6::jsonb, $7::timestamptz)
                            """,
                            str(row.get("payment_id") or ""),
                            row.get("from_status"),
                            str(row.get("to_status") or ""),
                            str(row.get("source") or ""),
                            str(row.get("reason") or ""),
                            _json({"legacy_imported": True, "legacy_metadata": row.get("metadata") or ""}),
                            _dt(row.get("created_at")),
                        )
                    report = ImportReport(
                        users=report.users,
                        subscriptions=report.subscriptions,
                        withdraw_requests=report.withdraw_requests,
                        payment_intents=report.payment_intents,
                        payment_status_history=report.payment_status_history + len(payment_history),
                        support_tickets=report.support_tickets,
                        support_messages=report.support_messages,
                    )

                if support_tickets:
                    for row in support_tickets:
                        ticket_id = int(row["id"])
                        await pg_conn.execute(
                            """
                            INSERT INTO legacy_support_tickets_archive(id, payload)
                            VALUES($1, $2::jsonb)
                            ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, imported_at = NOW()
                            """,
                            ticket_id,
                            _json(row),
                        )
                        await pg_conn.execute(
                            """
                            INSERT INTO support_tickets(id, user_id, status, assigned_admin_id, created_at, updated_at, meta)
                            VALUES($1, $2, $3, $4, $5::timestamptz, $6::timestamptz, '{}'::jsonb)
                            ON CONFLICT (id) DO UPDATE SET
                                status = EXCLUDED.status,
                                assigned_admin_id = EXCLUDED.assigned_admin_id,
                                updated_at = EXCLUDED.updated_at
                            """,
                            ticket_id,
                            int(row["user_id"]),
                            str(row.get("status") or "open"),
                            row.get("assigned_admin_id"),
                            _dt(row.get("created_at")),
                            _dt(row.get("updated_at")),
                        )
                    report = ImportReport(
                        users=report.users,
                        subscriptions=report.subscriptions,
                        withdraw_requests=report.withdraw_requests,
                        payment_intents=report.payment_intents,
                        payment_status_history=report.payment_status_history,
                        support_tickets=report.support_tickets + len(support_tickets),
                        support_messages=report.support_messages,
                    )

                if support_messages:
                    for row in support_messages:
                        msg_id = int(row["id"])
                        await pg_conn.execute(
                            """
                            INSERT INTO legacy_support_messages_archive(id, payload)
                            VALUES($1, $2::jsonb)
                            ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, imported_at = NOW()
                            """,
                            msg_id,
                            _json(row),
                        )
                        await pg_conn.execute(
                            """
                            INSERT INTO support_messages(id, ticket_id, sender_role, sender_user_id, text, media_type, media_file_id, created_at, meta)
                            VALUES($1, $2, $3, $4, $5, $6, $7, $8::timestamptz, '{}'::jsonb)
                            ON CONFLICT (id) DO UPDATE SET
                                text = EXCLUDED.text,
                                media_type = EXCLUDED.media_type,
                                media_file_id = EXCLUDED.media_file_id
                            """,
                            msg_id,
                            int(row["ticket_id"]),
                            str(row.get("sender_role") or ""),
                            int(row.get("sender_user_id") or 0),
                            str(row.get("text") or ""),
                            str(row.get("media_type") or ""),
                            str(row.get("media_file_id") or ""),
                            _dt(row.get("created_at")),
                        )
                    report = ImportReport(
                        users=report.users,
                        subscriptions=report.subscriptions,
                        withdraw_requests=report.withdraw_requests,
                        payment_intents=report.payment_intents,
                        payment_status_history=report.payment_status_history,
                        support_tickets=report.support_tickets,
                        support_messages=report.support_messages + len(support_messages),
                    )

            await _finish_import_run(
                pg_conn,
                run_id,
                "completed",
                {
                    "users": report.users,
                    "subscriptions": report.subscriptions,
                    "withdraw_requests": report.withdraw_requests,
                    "payment_intents": report.payment_intents,
                    "payment_status_history": report.payment_status_history,
                    "support_tickets": report.support_tickets,
                    "support_messages": report.support_messages,
                    "total_rows": report.total_rows,
                },
            )
            return report
        except Exception as exc:
            await _finish_import_run(
                pg_conn,
                run_id,
                "failed",
                {"error": str(exc)},
            )
            raise
