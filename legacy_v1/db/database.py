import asyncio
import logging
import secrets
import string
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiosqlite
from config import Config
from services.payment_states import can_transition
logger = logging.getLogger(__name__)


def generate_ref_code() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(8))


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None
        self.lock = asyncio.Lock()

    async def connect(self) -> None:
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row
        await self.init_db()

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()

    async def _ensure_table_columns(self, table: str, columns: Dict[str, str]) -> None:
        if not self.conn:
            return
        existing = await self._get_table_columns_unlocked(table)
        for name, ddl in columns.items():
            if name not in existing:
                await self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")

    @staticmethod
    def _normalize_payment_provider(provider: Optional[str]) -> str:
        return (provider or Config.PAYMENT_PROVIDER or "itpay").strip().lower()

    @staticmethod
    def _row_to_dict(row: Any) -> Optional[Dict[str, Any]]:
        return dict(row) if row else None

    @staticmethod
    def _rows_to_dicts(rows: List[Any]) -> List[Dict[str, Any]]:
        return [dict(row) for row in rows]

    async def _ensure_app_settings_table(self) -> None:
        if not self.conn:
            return
        await self.conn.execute(
            "CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    async def _get_table_columns_unlocked(self, table: str) -> set[str]:
        if not self.conn:
            return set()
        cur = await self.conn.execute(f"PRAGMA table_info({table})")
        rows = await cur.fetchall()
        return {row[1] for row in rows}

    async def get_table_columns(self, table: str) -> set[str]:
        if not self.conn:
            return set()
        async with self.lock:
            return await self._get_table_columns_unlocked(table)

    async def get_schema_drift_issues(self) -> list[str]:
        if not self.conn:
            return ["database_not_connected"]

        required_columns: Dict[str, set[str]] = {
            "support_tickets": {"assigned_admin_id"},
            "support_messages": {"media_type", "media_file_id"},
            "pending_payments": {"provider", "provider_payment_id", "processing_started_at", "activation_attempts", "last_error", "next_retry_at"},
        }
        issues: list[str] = []
        for table, required in required_columns.items():
            columns = await self.get_table_columns(table)
            if not columns:
                issues.append(f"missing_table:{table}")
                continue
            missing_columns = sorted(required - columns)
            for column in missing_columns:
                issues.append(f"missing_column:{table}.{column}")
        return issues

    async def auto_repair_schema_drift(self) -> list[str]:
        if not self.conn:
            return []

        before = await self.get_schema_drift_issues()
        if not before:
            return []

        async with self.lock:
            await self._ensure_support_tables()
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_payments (
                    payment_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    plan_id TEXT NOT NULL,
                    amount REAL NOT NULL,
                    status TEXT DEFAULT 'pending',
                    msg_id INTEGER,
                    provider TEXT DEFAULT 'itpay',
                    provider_payment_id TEXT DEFAULT '',
                    itpay_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP,
                    processing_started_at TIMESTAMP,
                    activation_attempts INTEGER DEFAULT 0,
                    last_error TEXT DEFAULT '',
                    next_retry_at TIMESTAMP
                )
                """
            )
            await self._ensure_table_columns(
                "pending_payments",
                {
                    "provider": "TEXT DEFAULT 'itpay'",
                    "provider_payment_id": "TEXT DEFAULT ''",
                    "processing_started_at": "TIMESTAMP",
                    "activation_attempts": "INTEGER DEFAULT 0",
                    "last_error": "TEXT DEFAULT ''",
                    "next_retry_at": "TIMESTAMP",
                },
            )
            await self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pending_payments_provider_payment_id ON pending_payments(provider, provider_payment_id)"
            )
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER NOT NULL
                )
                """
            )
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self.conn.execute("INSERT OR IGNORE INTO schema_version(version) VALUES (0)")
            await self.conn.commit()

        after = await self.get_schema_drift_issues()
        return sorted(set(before) - set(after))

    async def sync_schema_version_with_migrations(self) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            cursor = await self.conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
            row = await cursor.fetchone()
            latest_migration = int((row[0] if row else 0) or 0)
            await self.conn.execute("INSERT OR IGNORE INTO schema_version(version) VALUES (0)")
            await self.conn.execute(
                "UPDATE schema_version SET version = CASE WHEN version < ? THEN ? ELSE version END",
                (latest_migration, latest_migration),
            )
            await self.conn.commit()
            cursor = await self.conn.execute("SELECT version FROM schema_version LIMIT 1")
            current = await cursor.fetchone()
        return int((current[0] if current else 0) or 0)

    async def _ensure_support_tables(self) -> None:
        if not self.conn:
            return
        await self.conn.execute(
            "CREATE TABLE IF NOT EXISTS support_tickets (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'open', assigned_admin_id INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        await self._ensure_table_columns("support_tickets", {"assigned_admin_id": "INTEGER"})
        await self.conn.execute(
            "CREATE TABLE IF NOT EXISTS support_messages (id INTEGER PRIMARY KEY AUTOINCREMENT, ticket_id INTEGER NOT NULL, sender_role TEXT NOT NULL, sender_user_id INTEGER NOT NULL, text TEXT NOT NULL, media_type TEXT DEFAULT '', media_file_id TEXT DEFAULT '', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        await self._ensure_table_columns("support_messages", {"media_type": "TEXT DEFAULT ''", "media_file_id": "TEXT DEFAULT ''"})

    async def init_db(self) -> None:
        if not self.conn:
            return
        await self._ensure_support_tables()
        async with self.lock:
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER NOT NULL
                )
                """
            )
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self.conn.execute(
                "INSERT OR IGNORE INTO schema_version(version) VALUES (0)"
            )
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    banned BOOLEAN DEFAULT FALSE,
                    ban_reason TEXT DEFAULT '',
                    ref_code TEXT,
                    ref_by INTEGER,
                    ref_rewarded INTEGER DEFAULT 0,
                    bonus_days_pending INTEGER DEFAULT 0,
                    trial_used INTEGER DEFAULT 0,
                    trial_declined INTEGER DEFAULT 0,
                    has_subscription INTEGER DEFAULT 0,
                    plan_text TEXT DEFAULT '',
                    ip_limit INTEGER DEFAULT 0,
                    traffic_gb INTEGER DEFAULT 0,
                    vpn_url TEXT DEFAULT ''
                )
                """
            )
            await self.conn.commit()

            # Проверяем и добавляем колонки, если их нет
            columns = list(await self._get_table_columns_unlocked("users"))
            if "ref_system_type" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN ref_system_type INTEGER DEFAULT 1")
            if "ref_rewarded_count" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN ref_rewarded_count INTEGER DEFAULT 0")
            if "frozen_until" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN frozen_until TIMESTAMP DEFAULT NULL")
            if "notified_3d" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN notified_3d INTEGER DEFAULT 0")
            if "notified_1d" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN notified_1d INTEGER DEFAULT 0")
            if "notified_1h" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN notified_1h INTEGER DEFAULT 0")
            if "balance" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN balance REAL DEFAULT 0")
            if "partner_percent_level1" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN partner_percent_level1 REAL")
            if "partner_percent_level2" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN partner_percent_level2 REAL")
            if "partner_percent_level3" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN partner_percent_level3 REAL")
            if "partner_status" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN partner_status TEXT DEFAULT 'standard'")
            if "partner_note" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN partner_note TEXT DEFAULT ''")
            if "ref_suspicious" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN ref_suspicious INTEGER DEFAULT 0")
            if "panel_client_key" not in columns:
                await self.conn.execute("ALTER TABLE users ADD COLUMN panel_client_key TEXT DEFAULT ''")
            await self.conn.commit()

            await self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_ref_code ON users(ref_code)"
            )
            await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_users_ref_by ON users(ref_by)")
            await self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_panel_client_key ON users(panel_client_key) WHERE panel_client_key IS NOT NULL AND panel_client_key != ''"
            )
            await self.conn.commit()

            # Таблица запросов на вывод
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS withdraw_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
                """
            )
            await self.conn.commit()


            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ref_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    ref_user_id INTEGER NOT NULL,
                    amount REAL DEFAULT 0,
                    bonus_days INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self.conn.commit()

            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS antifraud_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    severity TEXT DEFAULT 'warning',
                    details TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self.conn.commit()

            await self.conn.execute("""
                CREATE TABLE IF NOT EXISTS pending_payments (
                    payment_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    plan_id TEXT NOT NULL,
                    amount REAL NOT NULL,
                    status TEXT DEFAULT 'pending',
                    msg_id INTEGER,
                    provider TEXT DEFAULT 'itpay',
                    provider_payment_id TEXT DEFAULT '',
                    itpay_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP,
                    processing_started_at TIMESTAMP,
                    activation_attempts INTEGER DEFAULT 0,
                    last_error TEXT DEFAULT '',
                    next_retry_at TIMESTAMP,
                    recipient_user_id INTEGER,
                    promo_code TEXT DEFAULT '',
                    promo_discount_percent REAL DEFAULT 0,
                    gift_label TEXT DEFAULT ''
                )
            """)
            await self.conn.commit()

            pending_columns = list(await self._get_table_columns_unlocked("pending_payments"))
            if "processing_started_at" not in pending_columns:
                await self.conn.execute("ALTER TABLE pending_payments ADD COLUMN processing_started_at TIMESTAMP")
            if "activation_attempts" not in pending_columns:
                await self.conn.execute("ALTER TABLE pending_payments ADD COLUMN activation_attempts INTEGER DEFAULT 0")
            if "last_error" not in pending_columns:
                await self.conn.execute("ALTER TABLE pending_payments ADD COLUMN last_error TEXT DEFAULT ''")
            if "next_retry_at" not in pending_columns:
                await self.conn.execute("ALTER TABLE pending_payments ADD COLUMN next_retry_at TIMESTAMP")
            if "provider" not in pending_columns:
                await self.conn.execute("ALTER TABLE pending_payments ADD COLUMN provider TEXT DEFAULT 'itpay'")
            if "provider_payment_id" not in pending_columns:
                await self.conn.execute("ALTER TABLE pending_payments ADD COLUMN provider_payment_id TEXT DEFAULT ''")
                await self.conn.execute("UPDATE pending_payments SET provider_payment_id = COALESCE(itpay_id, '') WHERE COALESCE(provider_payment_id, '') = ''")
            if "recipient_user_id" not in pending_columns:
                await self.conn.execute("ALTER TABLE pending_payments ADD COLUMN recipient_user_id INTEGER")
            if "promo_code" not in pending_columns:
                await self.conn.execute("ALTER TABLE pending_payments ADD COLUMN promo_code TEXT DEFAULT ''")
            if "promo_discount_percent" not in pending_columns:
                await self.conn.execute("ALTER TABLE pending_payments ADD COLUMN promo_discount_percent REAL DEFAULT 0")
            if "gift_label" not in pending_columns:
                await self.conn.execute("ALTER TABLE pending_payments ADD COLUMN gift_label TEXT DEFAULT ''")
            await self.conn.commit()
            await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_payments_status_created ON pending_payments(status, created_at)")
            await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_payments_user_status ON pending_payments(user_id, status)")
            await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_payments_provider_payment_id ON pending_payments(provider, provider_payment_id)")
            try:
                await self.conn.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_payments_provider_provider_payment_unique
                    ON pending_payments(provider, provider_payment_id)
                    WHERE provider_payment_id IS NOT NULL AND provider_payment_id != ''
                    """
                )
            except Exception as unique_idx_error:
                logger.warning("pending_payments unique index skipped: %s", unique_idx_error)
            await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_antifraud_events_user_created ON antifraud_events(user_id, created_at)")

            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS payment_status_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payment_id TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    source TEXT DEFAULT '',
                    reason TEXT DEFAULT '',
                    metadata TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_payment_status_history_payment_created ON payment_status_history(payment_id, created_at)"
            )
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS referral_balance_adjustments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    admin_user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    reason TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS referral_partner_settings (
                    user_id INTEGER PRIMARY KEY,
                    custom_percent_level1 REAL,
                    custom_percent_level2 REAL,
                    custom_percent_level3 REAL,
                    status TEXT DEFAULT 'standard',
                    note TEXT DEFAULT '',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_referral_balance_adjustments_user_created ON referral_balance_adjustments(user_id, created_at)")
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS payment_event_dedup (
                    event_key TEXT PRIMARY KEY,
                    payment_id TEXT DEFAULT '',
                    source TEXT DEFAULT '',
                    event_type TEXT DEFAULT '',
                    payload_excerpt TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_payment_event_dedup_payment_created ON payment_event_dedup(payment_id, created_at)"
            )
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS payment_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payment_id TEXT DEFAULT '',
                    status TEXT DEFAULT '',
                    provider TEXT DEFAULT '',
                    details TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_payment_events_status_created ON payment_events(status, created_at)"
            )
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS payment_admin_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payment_id TEXT NOT NULL,
                    admin_user_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    provider TEXT DEFAULT '',
                    result TEXT DEFAULT '',
                    details TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_payment_admin_actions_payment_created ON payment_admin_actions(payment_id, created_at DESC)"
            )
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS admin_user_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    admin_user_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_admin_user_actions_user_created ON admin_user_actions(user_id, created_at DESC)"
            )
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS transient_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    category TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP NOT NULL
                )
                """
            )
            await self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_transient_messages_expires_at ON transient_messages(expires_at)"
            )
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS promo_codes (
                    code TEXT PRIMARY KEY,
                    title TEXT DEFAULT '',
                    description TEXT DEFAULT '',
                    discount_percent REAL DEFAULT 0,
                    discount_type TEXT DEFAULT 'percent',
                    fixed_amount REAL DEFAULT 0,
                    only_new_users INTEGER DEFAULT 0,
                    plan_ids TEXT DEFAULT '',
                    max_uses INTEGER DEFAULT 0,
                    used_count INTEGER DEFAULT 0,
                    active INTEGER DEFAULT 1,
                    expires_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_promo_codes_active_expires ON promo_codes(active, expires_at)"
            )
            promo_columns = list(await self._get_table_columns_unlocked("promo_codes"))
            if "discount_type" not in promo_columns:
                await self.conn.execute("ALTER TABLE promo_codes ADD COLUMN discount_type TEXT DEFAULT 'percent'")
            if "fixed_amount" not in promo_columns:
                await self.conn.execute("ALTER TABLE promo_codes ADD COLUMN fixed_amount REAL DEFAULT 0")
            if "only_new_users" not in promo_columns:
                await self.conn.execute("ALTER TABLE promo_codes ADD COLUMN only_new_users INTEGER DEFAULT 0")
            if "plan_ids" not in promo_columns:
                await self.conn.execute("ALTER TABLE promo_codes ADD COLUMN plan_ids TEXT DEFAULT ''")
            if "user_limit" not in promo_columns:
                await self.conn.execute("ALTER TABLE promo_codes ADD COLUMN user_limit INTEGER DEFAULT 0")
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS promo_code_usages (
                    code TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    used_count INTEGER DEFAULT 0,
                    last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(code, user_id)
                )
                """
            )
            await self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gift_links (
                    token TEXT PRIMARY KEY,
                    buyer_user_id INTEGER NOT NULL,
                    plan_id TEXT NOT NULL,
                    payment_id TEXT DEFAULT '',
                    promo_code TEXT DEFAULT '',
                    promo_discount_percent REAL DEFAULT 0,
                    note TEXT DEFAULT '',
                    reminder_sent_at TIMESTAMP,
                    claimed_by_user_id INTEGER,
                    claimed_at TIMESTAMP,
                    expires_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_gift_links_buyer_created ON gift_links(buyer_user_id, created_at DESC)"
            )
            gift_columns = list(await self._get_table_columns_unlocked("gift_links"))
            if "reminder_sent_at" not in gift_columns:
                await self.conn.execute("ALTER TABLE gift_links ADD COLUMN reminder_sent_at TIMESTAMP")
            await self.conn.commit()

    async def get_schema_version(self) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute("SELECT version FROM schema_version LIMIT 1") as c:
                row = await c.fetchone()
        return int(row[0]) if row else 0

    async def set_schema_version(self, version: int) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            cursor = await self.conn.execute("UPDATE schema_version SET version = ?", (version,))
            await self.conn.commit()
        return cursor.rowcount > 0

    async def get_applied_migration_versions(self) -> list[int]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute("SELECT version FROM schema_migrations ORDER BY version") as c:
                rows = await c.fetchall()
        return [int(r[0]) for r in rows]

    async def record_migration(self, version: int, name: str) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            await self.conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version, name) VALUES (?, ?)",
                (version, name),
            )
            await self.conn.execute("UPDATE schema_version SET version = CASE WHEN version < ? THEN ? ELSE version END", (version, version))
            await self.conn.commit()
        return True

    async def executescript(self, script: str) -> None:
        if not self.conn:
            return
        async with self.lock:
            await self.conn.executescript(script)
            await self.conn.commit()

    async def add_antifraud_event(self, user_id: int, event_type: str, details: str = "", severity: str = "warning") -> int:
        if not self.conn:
            return 0
        async with self.lock:
            cursor = await self.conn.execute(
                "INSERT INTO antifraud_events (user_id, event_type, severity, details) VALUES (?, ?, ?, ?)",
                (user_id, event_type, severity, details[:500]),
            )
            await self.conn.commit()
        return int(cursor.lastrowid or 0)

    async def count_antifraud_events(self, user_id: int, event_type: str, since_hours: int = 24) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT COUNT(*) FROM antifraud_events WHERE user_id = ? AND event_type = ? AND created_at >= datetime('now', '-' || ? || ' hours')",
                (user_id, event_type, since_hours),
            ) as c:
                row = await c.fetchone()
        return int(row[0]) if row else 0

    async def get_recent_antifraud_events(self, limit: int = 20) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute(
                "SELECT * FROM antifraud_events ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ) as c:
                rows = await c.fetchall()
        return [dict(r) for r in rows]

    async def add_user(self, user_id: int) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            try:
                await self.conn.execute(
                    "INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,)
                )
                await self.conn.commit()
                return True
            except Exception as e:
                logger.error("User insert failed: user=%s error=%s", user_id, e)
                return False

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            )
            row = await cursor.fetchone()
        return self._row_to_dict(row)

    async def get_user_by_ref_code(self, ref_code: str) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        async with self.lock:
            async with self.conn.execute(
                "SELECT * FROM users WHERE ref_code = ?", (ref_code,)
            ) as cursor:
                row = await cursor.fetchone()
        return dict(row) if row else None

    async def update_user(self, user_id: int, **kwargs) -> bool:
        if not self.conn or not kwargs:
            return False
        set_clause = ", ".join([f"{key} = ?" for key in kwargs.keys()])
        values = list(kwargs.values())
        values.append(user_id)
        async with self.lock:
            try:
                cursor = await self.conn.execute(
                    f"UPDATE users SET {set_clause} WHERE user_id = ?", values
                )
                await self.conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error("User update failed: user=%s error=%s", user_id, e)
                return False


    async def get_daily_user_acquisition_report(self, *, days_ago: int = 0) -> Dict[str, Any]:
        if not self.conn:
            return {
                "report_date": "",
                "new_users": 0,
                "referred_new_users": 0,
                "trial_started_new_users": 0,
            }
        days_ago = max(0, int(days_ago))
        async with self.lock:
            cursor = await self.conn.execute(
                """
                SELECT
                    date('now', ? || ' day') AS report_date,
                    COUNT(*) AS new_users,
                    SUM(CASE WHEN COALESCE(ref_by, 0) > 0 THEN 1 ELSE 0 END) AS referred_new_users,
                    SUM(CASE WHEN COALESCE(trial_used, 0) = 1 THEN 1 ELSE 0 END) AS trial_started_new_users
                FROM users
                WHERE date(join_date) = date('now', ? || ' day')
                """,
                (f"-{days_ago}", f"-{days_ago}"),
            )
            row = await cursor.fetchone()
        data = dict(row) if row else {}
        return {
            "report_date": str(data.get("report_date") or ""),
            "new_users": int(data.get("new_users") or 0),
            "referred_new_users": int(data.get("referred_new_users") or 0),
            "trial_started_new_users": int(data.get("trial_started_new_users") or 0),
        }


    async def get_period_user_acquisition_report(self, *, days: int, end_days_ago: int = 0) -> Dict[str, Any]:
        if not self.conn:
            return {
                "start_date": "",
                "end_date": "",
                "days": max(1, int(days)),
                "new_users": 0,
                "referred_new_users": 0,
                "trial_started_new_users": 0,
            }
        days = max(1, int(days))
        end_days_ago = max(0, int(end_days_ago))
        start_days_ago = end_days_ago + days - 1
        async with self.lock:
            cursor = await self.conn.execute(
                """
                SELECT
                    date('now', ? || ' day') AS start_date,
                    date('now', ? || ' day') AS end_date,
                    COUNT(*) AS new_users,
                    SUM(CASE WHEN COALESCE(ref_by, 0) > 0 THEN 1 ELSE 0 END) AS referred_new_users,
                    SUM(CASE WHEN COALESCE(trial_used, 0) = 1 THEN 1 ELSE 0 END) AS trial_started_new_users
                FROM users
                WHERE date(join_date) BETWEEN date('now', ? || ' day') AND date('now', ? || ' day')
                """,
                (f"-{start_days_ago}", f"-{end_days_ago}", f"-{start_days_ago}", f"-{end_days_ago}"),
            )
            row = await cursor.fetchone()
        data = dict(row) if row else {}
        return {
            "start_date": str(data.get("start_date") or ""),
            "end_date": str(data.get("end_date") or ""),
            "days": days,
            "new_users": int(data.get("new_users") or 0),
            "referred_new_users": int(data.get("referred_new_users") or 0),
            "trial_started_new_users": int(data.get("trial_started_new_users") or 0),
        }

    async def get_daily_subscription_sales_report(self, *, days_ago: int = 0) -> Dict[str, Any]:
        if not self.conn:
            return {
                "report_date": "",
                "subscriptions_bought": 0,
                "gross_revenue": 0.0,
                "refunded_revenue": 0.0,
                "net_revenue": 0.0,
                "referral_cost": 0.0,
                "estimated_profit": 0.0,
            }
        days_ago = max(0, int(days_ago))
        day_expr = f"-{days_ago}"
        async with self.lock:
            accepted_cursor = await self.conn.execute(
                """
                SELECT
                    COUNT(DISTINCT h.payment_id) AS subscriptions_bought,
                    COALESCE(SUM(p.amount), 0) AS gross_revenue
                FROM payment_status_history h
                JOIN pending_payments p ON p.payment_id = h.payment_id
                WHERE h.to_status = 'accepted'
                  AND date(h.created_at) = date('now', ? || ' day')
                """,
                (day_expr,),
            )
            accepted_row = await accepted_cursor.fetchone()

            refunded_cursor = await self.conn.execute(
                """
                SELECT COALESCE(SUM(p.amount), 0) AS refunded_revenue
                FROM payment_status_history h
                JOIN pending_payments p ON p.payment_id = h.payment_id
                WHERE h.to_status = 'refunded'
                  AND date(h.created_at) = date('now', ? || ' day')
                """,
                (day_expr,),
            )
            refunded_row = await refunded_cursor.fetchone()

            referral_cursor = await self.conn.execute(
                """
                SELECT COALESCE(SUM(amount), 0) AS referral_cost
                FROM ref_history
                WHERE date(created_at) = date('now', ? || ' day')
                """,
                (day_expr,),
            )
            referral_row = await referral_cursor.fetchone()

        subscriptions_bought = int((accepted_row[0] if accepted_row else 0) or 0)
        gross_revenue = float((accepted_row[1] if accepted_row else 0.0) or 0.0)
        refunded_revenue = float((refunded_row[0] if refunded_row else 0.0) or 0.0)
        referral_cost = float((referral_row[0] if referral_row else 0.0) or 0.0)
        net_revenue = gross_revenue - refunded_revenue
        estimated_profit = net_revenue - referral_cost
        return {
            "report_date": (datetime.now(timezone.utc) - timedelta(days=days_ago)).date().isoformat(),
            "subscriptions_bought": subscriptions_bought,
            "gross_revenue": gross_revenue,
            "refunded_revenue": refunded_revenue,
            "net_revenue": net_revenue,
            "referral_cost": referral_cost,
            "estimated_profit": estimated_profit,
        }


    async def get_period_subscription_sales_report(self, *, days: int, end_days_ago: int = 0) -> Dict[str, Any]:
        if not self.conn:
            return {
                "start_date": "",
                "end_date": "",
                "days": max(1, int(days)),
                "subscriptions_bought": 0,
                "gross_revenue": 0.0,
                "refunded_revenue": 0.0,
                "net_revenue": 0.0,
                "referral_cost": 0.0,
                "estimated_profit": 0.0,
            }
        days = max(1, int(days))
        end_days_ago = max(0, int(end_days_ago))
        start_days_ago = end_days_ago + days - 1
        async with self.lock:
            accepted_cursor = await self.conn.execute(
                """
                SELECT
                    COUNT(DISTINCT h.payment_id) AS subscriptions_bought,
                    COALESCE(SUM(p.amount), 0) AS gross_revenue
                FROM payment_status_history h
                JOIN pending_payments p ON p.payment_id = h.payment_id
                WHERE h.to_status = 'accepted'
                  AND date(h.created_at) BETWEEN date('now', ? || ' day') AND date('now', ? || ' day')
                """,
                (f"-{start_days_ago}", f"-{end_days_ago}"),
            )
            accepted_row = await accepted_cursor.fetchone()

            refunded_cursor = await self.conn.execute(
                """
                SELECT COALESCE(SUM(p.amount), 0) AS refunded_revenue
                FROM payment_status_history h
                JOIN pending_payments p ON p.payment_id = h.payment_id
                WHERE h.to_status = 'refunded'
                  AND date(h.created_at) BETWEEN date('now', ? || ' day') AND date('now', ? || ' day')
                """,
                (f"-{start_days_ago}", f"-{end_days_ago}"),
            )
            refunded_row = await refunded_cursor.fetchone()

            referral_cursor = await self.conn.execute(
                """
                SELECT COALESCE(SUM(amount), 0) AS referral_cost
                FROM ref_history
                WHERE date(created_at) BETWEEN date('now', ? || ' day') AND date('now', ? || ' day')
                """,
                (f"-{start_days_ago}", f"-{end_days_ago}"),
            )
            referral_row = await referral_cursor.fetchone()

        subscriptions_bought = int((accepted_row[0] if accepted_row else 0) or 0)
        gross_revenue = float((accepted_row[1] if accepted_row else 0.0) or 0.0)
        refunded_revenue = float((refunded_row[0] if refunded_row else 0.0) or 0.0)
        referral_cost = float((referral_row[0] if referral_row else 0.0) or 0.0)
        net_revenue = gross_revenue - refunded_revenue
        estimated_profit = net_revenue - referral_cost
        end_date = (datetime.now(timezone.utc) - timedelta(days=end_days_ago)).date().isoformat()
        start_date = (datetime.now(timezone.utc) - timedelta(days=start_days_ago)).date().isoformat()
        return {
            "start_date": start_date,
            "end_date": end_date,
            "days": days,
            "subscriptions_bought": subscriptions_bought,
            "gross_revenue": gross_revenue,
            "refunded_revenue": refunded_revenue,
            "net_revenue": net_revenue,
            "referral_cost": referral_cost,
            "estimated_profit": estimated_profit,
        }

    async def get_total_revenue_summary(self) -> Dict[str, Any]:
        if not self.conn:
            return {
                "accepted_payments": 0,
                "gross_revenue": 0.0,
                "refunded_revenue": 0.0,
                "net_revenue": 0.0,
                "referral_cost": 0.0,
                "estimated_profit": 0.0,
            }
        async with self.lock:
            cursor = await self.conn.execute(
                """
                SELECT
                    SUM(CASE WHEN status IN ('accepted', 'refunded') THEN 1 ELSE 0 END) AS accepted_payments,
                    COALESCE(SUM(CASE WHEN status IN ('accepted', 'refunded') THEN amount ELSE 0 END), 0) AS gross_revenue,
                    COALESCE(SUM(CASE WHEN status = 'refunded' THEN amount ELSE 0 END), 0) AS refunded_revenue
                FROM pending_payments
                """
            )
            payment_row = await cursor.fetchone()
            ref_cursor = await self.conn.execute("SELECT COALESCE(SUM(amount), 0) FROM ref_history")
            ref_row = await ref_cursor.fetchone()
        accepted_payments = int((payment_row[0] if payment_row else 0) or 0)
        gross_revenue = float((payment_row[1] if payment_row else 0.0) or 0.0)
        refunded_revenue = float((payment_row[2] if payment_row else 0.0) or 0.0)
        referral_cost = float((ref_row[0] if ref_row else 0.0) or 0.0)
        net_revenue = gross_revenue - refunded_revenue
        estimated_profit = net_revenue - referral_cost
        return {
            "accepted_payments": accepted_payments,
            "gross_revenue": gross_revenue,
            "refunded_revenue": refunded_revenue,
            "net_revenue": net_revenue,
            "referral_cost": referral_cost,
            "estimated_profit": estimated_profit,
        }

    async def get_total_users(self) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute("SELECT COUNT(*) FROM users") as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def get_banned_users_count(self) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT COUNT(*) FROM users WHERE banned = TRUE"
            ) as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def get_banned_user_ids(self) -> List[int]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute(
                "SELECT user_id FROM users WHERE banned = TRUE"
            ) as cursor:
                rows = await cursor.fetchall()
        return [int(row[0]) for row in rows]

    async def get_subscribed_user_ids(self) -> List[int]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute(
                "SELECT user_id FROM users WHERE has_subscription = 1 AND vpn_url != '' AND vpn_url IS NOT NULL"
            ) as cursor:
                rows = await cursor.fetchall()
        return [int(row[0]) for row in rows]

    async def ban_user(self, user_id: int, reason: str = "") -> bool:
        return await self.update_user(user_id, banned=True, ban_reason=reason)

    async def unban_user(self, user_id: int) -> bool:
        return await self.update_user(user_id, banned=False, ban_reason="")

    async def set_subscription(
        self, user_id: int, plan_text: str, ip_limit: int, vpn_url: str, traffic_gb: int
    ) -> bool:
        return await self.update_user(
            user_id=user_id,
            has_subscription=1,
            plan_text=plan_text,
            ip_limit=ip_limit,
            vpn_url=vpn_url,
            traffic_gb=traffic_gb,
            notified_3d=0,
            notified_1d=0,
            notified_1h=0,
        )

    async def remove_subscription(self, user_id: int) -> bool:
        return await self.update_user(
            user_id=user_id,
            has_subscription=0,
            plan_text="",
            ip_limit=0,
            vpn_url="",
            traffic_gb=0,
            frozen_until=None,
            notified_3d=0,
            notified_1d=0,
            notified_1h=0,
        )

    async def set_ref_by(self, user_id: int, ref_by: int) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            cursor = await self.conn.execute(
                """
                UPDATE users
                SET ref_by = ?
                WHERE user_id = ? AND user_id != ? AND (ref_by IS NULL OR ref_by = 0)
                """,
                (ref_by, user_id, user_id),
            )
            await self.conn.commit()
            return cursor.rowcount > 0

    async def mark_ref_rewarded(self, user_id: int) -> bool:
        return await self.update_user(user_id, ref_rewarded=1)

    async def count_referrals(self, ref_by: int) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT COUNT(*) FROM users WHERE ref_by = ?", (ref_by,)
            ) as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def count_referrals_paid(self, ref_by: int) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT COUNT(*) FROM users WHERE ref_by = ? AND ref_rewarded = 1",
                (ref_by,),
            ) as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def get_bonus_days_pending(self, user_id: int) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT bonus_days_pending FROM users WHERE user_id = ?", (user_id,)
            )
            row = await cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    async def clear_bonus_days_pending(self, user_id: int) -> bool:
        return await self.update_user(user_id, bonus_days_pending=0)

    async def add_bonus_days_pending(self, user_id: int, days: int) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            await self.conn.execute(
                """
                UPDATE users
                SET bonus_days_pending = COALESCE(bonus_days_pending, 0) + ?
                WHERE user_id = ?
                """,
                (days, user_id),
            )
            await self.conn.commit()
        return True

    async def mark_trial_used(self, user_id: int) -> bool:
        return await self.update_user(user_id, trial_used=1)

    async def mark_trial_declined(self, user_id: int) -> bool:
        return await self.update_user(user_id, trial_declined=1)

    async def set_has_subscription(self, user_id: int) -> bool:
        return await self.update_user(user_id, has_subscription=1)

    async def clear_has_subscription(self, user_id: int) -> bool:
        return await self.update_user(user_id, has_subscription=0)

    async def add_ref_history(self, user_id: int, ref_user_id: int, amount: float = 0, bonus_days: int = 0) -> None:
        """Записывает начисление в историю."""
        if not self.conn:
            return
        async with self.lock:
            await self.conn.execute(
                "INSERT INTO ref_history (user_id, ref_user_id, amount, bonus_days) VALUES (?, ?, ?, ?)",
                (user_id, ref_user_id, amount, bonus_days),
            )
            await self.conn.commit()

    async def get_ref_history(self, user_id: int, limit: int = 10) -> list:
        """История начислений пользователя."""
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM ref_history WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
                (user_id, limit),
            )
            rows = await cursor.fetchall()
            return self._rows_to_dicts(rows)

    async def get_referrals_list(self, user_id: int) -> list:
        """Список рефералов с флагом оплаты."""
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT user_id, ref_rewarded, join_date FROM users WHERE ref_by = ? ORDER BY join_date DESC",
                (user_id,),
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def count_recent_referrals_by_referrer(self, referrer_id: int, *, since_hours: int = 24) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            cursor = await self.conn.execute(
                """
                SELECT COUNT(*)
                FROM users
                WHERE ref_by = ?
                  AND join_date >= datetime('now', '-' || ? || ' hours')
                """,
                (referrer_id, int(since_hours)),
            )
            row = await cursor.fetchone()
        return int((row[0] if row else 0) or 0)


    async def get_all_users(self) -> list:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute("SELECT * FROM users")
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_all_subscribers(self) -> list:
        """Все пользователи с активной подпиской."""
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM users WHERE has_subscription = 1 AND vpn_url != '' AND vpn_url IS NOT NULL AND banned = 0"
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_users_for_broadcast_segment(self, segment: str) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        normalized = (segment or "all").strip().lower()
        query = "SELECT * FROM users"
        params: tuple[Any, ...] = ()
        if normalized == "active":
            query = (
                "SELECT * FROM users WHERE has_subscription = 1 AND vpn_url != '' "
                "AND vpn_url IS NOT NULL AND banned = 0"
            )
        elif normalized == "inactive":
            query = "SELECT * FROM users WHERE COALESCE(has_subscription, 0) = 0 AND banned = 0"
        elif normalized == "with_balance":
            query = "SELECT * FROM users WHERE COALESCE(balance, 0) > 0 AND banned = 0"
        elif normalized == "referred":
            query = "SELECT * FROM users WHERE COALESCE(ref_by, 0) > 0 AND banned = 0"
        elif normalized == "expired":
            query = (
                "SELECT * FROM users WHERE COALESCE(has_subscription, 0) = 0 "
                "AND expiry IS NOT NULL AND expiry != '' AND banned = 0"
            )
        elif normalized == "trial_only":
            query = (
                "SELECT * FROM users WHERE COALESCE(trial_used, 0) = 1 "
                "AND COALESCE(has_subscription, 0) = 0 AND banned = 0"
            )
        else:
            query = "SELECT * FROM users WHERE banned = 0"
        async with self.lock:
            cursor = await self.conn.execute(query, params)
            rows = await cursor.fetchall()
        return self._rows_to_dicts(rows)

    async def get_broadcast_segment_counts(self) -> Dict[str, int]:
        segments = ("all", "active", "inactive", "with_balance", "referred", "expired", "trial_only")
        counts: Dict[str, int] = {}
        for segment in segments:
            counts[segment] = len(await self.get_users_for_broadcast_segment(segment))
        return counts

    async def get_user_card(self, user_id: int) -> Dict[str, Any]:
        user = await self.get_user(user_id)
        if not user:
            return {}
        referral_summary = await self.get_referral_summary(user_id)
        partner_settings = await self.get_partner_settings(user_id)
        support_tickets = await self.list_user_support_tickets(user_id, limit=5)
        support_restriction = await self.get_support_restriction(user_id)
        payments = await self.get_pending_payments_by_user(user_id)
        withdraws = await self.get_withdraw_requests_by_user(user_id, limit=5)
        adjustments = await self.get_referral_balance_adjustments(user_id, limit=5)
        return {
            "user": user,
            "referral_summary": referral_summary,
            "partner_settings": partner_settings,
            "support_tickets": support_tickets,
            "support_restriction": support_restriction,
            "payments": payments[:5],
            "withdraws": withdraws[:5],
            "adjustments": adjustments[:5],
        }

    async def get_recent_user_ids(self, limit: int = 10) -> List[int]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT user_id FROM users ORDER BY join_date DESC, user_id DESC LIMIT ?",
                (int(limit),),
            )
            rows = await cursor.fetchall()
        return [int(row[0]) for row in rows if row and row[0]]

    async def get_recent_support_blacklist_hits(self, limit: int = 20) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                """
                SELECT * FROM antifraud_events
                WHERE event_type = 'support_blacklist'
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (int(limit),),
            )
            rows = await cursor.fetchall()
        return self._rows_to_dicts(rows)

    async def get_support_restriction(self, user_id: int) -> Dict[str, Any]:
        until_raw = str(await self.get_setting(f"support:blocked_until:{int(user_id)}", "") or "")
        reason = str(await self.get_setting(f"support:block_reason:{int(user_id)}", "") or "")
        active = False
        expires_at = until_raw
        if until_raw:
            try:
                expires_dt = datetime.fromisoformat(until_raw.replace("Z", "+00:00"))
                if expires_dt.tzinfo is not None:
                    expires_dt = expires_dt.astimezone(timezone.utc).replace(tzinfo=None)
                if expires_dt > datetime.utcnow():
                    active = True
                else:
                    await self.clear_support_restriction(user_id)
                    expires_at = ""
                    reason = ""
            except ValueError:
                await self.clear_support_restriction(user_id)
                expires_at = ""
                reason = ""
        return {
            "active": active,
            "expires_at": expires_at,
            "reason": reason,
        }

    async def add_admin_user_action(self, user_id: int, admin_user_id: int, action: str, details: str = "") -> int:
        if not self.conn:
            return 0
        async with self.lock:
            cursor = await self.conn.execute(
                "INSERT INTO admin_user_actions(user_id, admin_user_id, action, details) VALUES (?, ?, ?, ?)",
                (int(user_id), int(admin_user_id), action[:120], details[:1000]),
            )
            await self.conn.commit()
        return int(cursor.lastrowid or 0)

    async def list_admin_user_actions(self, user_id: int, limit: int = 20) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM admin_user_actions WHERE user_id = ? ORDER BY created_at DESC, id DESC LIMIT ?",
                (int(user_id), int(limit)),
            )
            rows = await cursor.fetchall()
        return self._rows_to_dicts(rows)

    async def get_user_timeline(self, user_id: int, limit: int = 25) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for row in await self.list_admin_user_actions(user_id, limit=limit):
            items.append({
                "created_at": row.get("created_at"),
                "kind": "admin_action",
                "title": row.get("action") or "admin_action",
                "details": f"admin={row.get('admin_user_id')} {row.get('details') or ''}".strip(),
            })
        for row in await self.list_user_support_tickets(user_id, limit=10):
            items.append({
                "created_at": row.get("updated_at") or row.get("created_at"),
                "kind": "support_ticket",
                "title": f"ticket#{row.get('id')}:{row.get('status')}",
                "details": "",
            })
        for row in await self.get_pending_payments_by_user(user_id):
            items.append({
                "created_at": row.get("created_at"),
                "kind": "payment",
                "title": f"payment:{row.get('status')}",
                "details": f"{row.get('payment_id')} {float(row.get('amount') or 0):.2f} RUB",
            })
        for row in await self.get_withdraw_requests_by_user(user_id, limit=10):
            items.append({
                "created_at": row.get("created_at"),
                "kind": "withdraw",
                "title": f"withdraw:{row.get('status')}",
                "details": f"#{row.get('id')} {float(row.get('amount') or 0):.2f} RUB",
            })
        for row in await self.get_referral_balance_adjustments(user_id, limit=10):
            items.append({
                "created_at": row.get("created_at"),
                "kind": "balance_adjustment",
                "title": "balance_adjustment",
                "details": f"{float(row.get('amount') or 0):.2f} RUB {row.get('reason') or ''}".strip(),
            })
        items.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
        return items[: max(1, int(limit))]

    async def set_support_restriction(self, user_id: int, *, hours: int, reason: str = "") -> bool:
        expires_at = (datetime.utcnow() + timedelta(hours=max(1, int(hours)))).replace(microsecond=0).isoformat()
        ok_until = await self.set_setting(f"support:blocked_until:{int(user_id)}", expires_at)
        ok_reason = await self.set_setting(f"support:block_reason:{int(user_id)}", reason[:500])
        return bool(ok_until and ok_reason)

    async def clear_support_restriction(self, user_id: int) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            await self.conn.execute("DELETE FROM app_settings WHERE key IN (?, ?)", (
                f"support:blocked_until:{int(user_id)}",
                f"support:block_reason:{int(user_id)}",
            ))
            await self.conn.commit()
        return True

    async def list_support_restricted_users(self, limit: int = 50) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                """
                SELECT key, value
                FROM app_settings
                WHERE key LIKE 'support:blocked_until:%'
                ORDER BY updated_at DESC
                """
            )
            rows = await cursor.fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            key = str(row["key"])
            try:
                user_id = int(key.rsplit(":", 1)[-1])
            except ValueError:
                continue
            restriction = await self.get_support_restriction(user_id)
            if restriction.get("active"):
                result.append({
                    "user_id": user_id,
                    "expires_at": restriction.get("expires_at") or "",
                    "reason": restriction.get("reason") or "",
                })
            if len(result) >= int(limit):
                break
        return result

    async def support_restriction_notifications_enabled(self) -> bool:
        raw = str(await self.get_setting("support:restriction_admin_notifications", "1") or "1").strip()
        return raw != "0"

    async def set_support_restriction_notifications_enabled(self, enabled: bool) -> bool:
        return await self.set_setting("support:restriction_admin_notifications", "1" if enabled else "0")

    async def get_daily_incident_report(self, *, days_ago: int = 0) -> Dict[str, Any]:
        if not self.conn:
            return {}
        day_expr = f"-{max(0, int(days_ago))}"
        async with self.lock:
            payment_errors_cur = await self.conn.execute(
                "SELECT COUNT(*) FROM payment_event_dedup WHERE event_type = 'error' AND date(created_at) = date('now', ? || ' day')",
                (day_expr,),
            )
            payment_errors_row = await payment_errors_cur.fetchone()
            support_hits_cur = await self.conn.execute(
                """
                SELECT COUNT(*)
                FROM antifraud_events
                WHERE event_type = 'support_blacklist'
                  AND date(created_at) = date('now', ? || ' day')
                """,
                (day_expr,),
            )
            support_hits_row = await support_hits_cur.fetchone()
            stale_cur = await self.conn.execute(
                """
                SELECT COUNT(*)
                FROM pending_payments
                WHERE status = 'processing'
                  AND processing_started_at IS NOT NULL
                  AND processing_started_at < datetime('now', '-' || ? || ' minutes')
                """,
                (int(Config.STALE_PROCESSING_TIMEOUT_MIN),),
            )
            stale_row = await stale_cur.fetchone()
            pending_old_cur = await self.conn.execute(
                """
                SELECT COUNT(*)
                FROM pending_payments
                WHERE status = 'pending'
                  AND created_at < datetime('now', '-' || ? || ' minutes')
                """,
                (int(Config.HEALTH_PENDING_AGE_MIN),),
            )
            pending_old_row = await pending_old_cur.fetchone()
        return {
            "report_date": (datetime.now(timezone.utc) - timedelta(days=max(0, int(days_ago)))).date().isoformat(),
            "payment_errors": int((payment_errors_row[0] if payment_errors_row else 0) or 0),
            "support_blacklist_hits": int((support_hits_row[0] if support_hits_row else 0) or 0),
            "stale_processing": int((stale_row[0] if stale_row else 0) or 0),
            "old_pending": int((pending_old_row[0] if pending_old_row else 0) or 0),
        }

    async def set_frozen(self, user_id: int, frozen_until: str) -> bool:
        return await self.update_user(user_id, frozen_until=frozen_until)

    async def clear_frozen(self, user_id: int) -> bool:
        return await self.update_user(user_id, frozen_until=None)

    async def reset_expiry_notifications(self, user_id: int) -> bool:
        return await self.update_user(user_id, notified_3d=0, notified_1d=0, notified_1h=0)

    async def get_top_referrers(self, limit: int = 10) -> list:
        """Топ рефереров по количеству оплативших рефералов."""
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                """
                SELECT ref_by, COUNT(*) as paid_count
                FROM users
                WHERE ref_by IS NOT NULL AND ref_rewarded = 1
                GROUP BY ref_by
                ORDER BY paid_count DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_referral_summary(self, user_id: int) -> dict:
        if not self.conn:
            return {
                "total_refs": 0,
                "paid_refs": 0,
                "earned_rub": 0.0,
                "earned_bonus_days": 0,
                "completed_withdraw_rub": 0.0,
                "pending_withdraw_rub": 0.0,
            }
        async with self.lock:
            refs_cursor = await self.conn.execute(
                """
                SELECT COUNT(*) AS total_refs,
                       COALESCE(SUM(CASE WHEN ref_rewarded = 1 THEN 1 ELSE 0 END), 0) AS paid_refs
                FROM users
                WHERE ref_by = ?
                """,
                (user_id,),
            )
            refs_row = await refs_cursor.fetchone()
            hist_cursor = await self.conn.execute(
                """
                SELECT COALESCE(SUM(amount), 0) AS earned_rub,
                       COALESCE(SUM(bonus_days), 0) AS earned_bonus_days
                FROM ref_history
                WHERE user_id = ?
                """,
                (user_id,),
            )
            hist_row = await hist_cursor.fetchone()
            withdraw_cursor = await self.conn.execute(
                """
                SELECT COALESCE(SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END), 0) AS completed_withdraw_rub,
                       COALESCE(SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END), 0) AS pending_withdraw_rub
                FROM withdraw_requests
                WHERE user_id = ?
                """,
                (user_id,),
            )
            withdraw_row = await withdraw_cursor.fetchone()
        return {
            "total_refs": int((refs_row[0] if refs_row else 0) or 0),
            "paid_refs": int((refs_row[1] if refs_row else 0) or 0),
            "earned_rub": float((hist_row[0] if hist_row else 0.0) or 0.0),
            "earned_bonus_days": int((hist_row[1] if hist_row else 0) or 0),
            "completed_withdraw_rub": float((withdraw_row[0] if withdraw_row else 0.0) or 0.0),
            "pending_withdraw_rub": float((withdraw_row[1] if withdraw_row else 0.0) or 0.0),
        }

    async def get_top_referrers_extended(self, limit: int = 10) -> list:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                """
                SELECT u.ref_by AS ref_by,
                       COUNT(*) AS paid_count,
                       COALESCE(SUM(rh.amount), 0) AS earned_rub,
                       COALESCE(SUM(rh.bonus_days), 0) AS earned_bonus_days
                FROM users u
                LEFT JOIN ref_history rh ON rh.user_id = u.ref_by AND rh.ref_user_id = u.user_id
                WHERE u.ref_by IS NOT NULL AND u.ref_rewarded = 1
                GROUP BY u.ref_by
                ORDER BY paid_count DESC, earned_rub DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def increment_ref_rewarded_count(self, user_id: int) -> None:
        """Увеличивает счётчик успешных рефералов."""
        if not self.conn:
            return
        async with self.lock:
            await self.conn.execute(
                "UPDATE users SET ref_rewarded_count = COALESCE(ref_rewarded_count, 0) + 1 WHERE user_id = ?",
                (user_id,),
            )
            await self.conn.commit()

    async def set_partner_rates(self, user_id: int, level1=None, level2=None, level3=None, status: Optional[str] = None, note: Optional[str] = None) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            cursor = await self.conn.execute("SELECT user_id FROM referral_partner_settings WHERE user_id = ?", (user_id,))
            row = await cursor.fetchone()
            status = status or 'standard'
            note = note or ''
            if row:
                await self.conn.execute(
                    "UPDATE referral_partner_settings SET custom_percent_level1 = ?, custom_percent_level2 = ?, custom_percent_level3 = ?, status = ?, note = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
                    (level1, level2, level3, status, note, user_id),
                )
            else:
                await self.conn.execute(
                    "INSERT INTO referral_partner_settings(user_id, custom_percent_level1, custom_percent_level2, custom_percent_level3, status, note) VALUES (?, ?, ?, ?, ?, ?)",
                    (user_id, level1, level2, level3, status, note),
                )
            await self.conn.execute(
                "UPDATE users SET partner_percent_level1 = ?, partner_percent_level2 = ?, partner_percent_level3 = ?, partner_status = ?, partner_note = ? WHERE user_id = ?",
                (level1, level2, level3, status, note, user_id),
            )
            await self.conn.commit()
            return True

    async def get_partner_settings(self, user_id: int) -> Dict[str, Any]:
        user = await self.get_user(user_id)
        if not user:
            return {'user_id': user_id, 'custom_percent_level1': None, 'custom_percent_level2': None, 'custom_percent_level3': None, 'status': 'standard', 'note': '', 'suspicious': False}
        return {
            'user_id': user_id,
            'custom_percent_level1': user.get('partner_percent_level1'),
            'custom_percent_level2': user.get('partner_percent_level2'),
            'custom_percent_level3': user.get('partner_percent_level3'),
            'status': user.get('partner_status') or 'standard',
            'note': user.get('partner_note') or '',
            'suspicious': bool(user.get('ref_suspicious')),
        }

    async def add_referral_balance_adjustment(self, user_id: int, admin_user_id: int, amount: float, reason: str = '') -> bool:
        if not self.conn:
            return False
        async with self.lock:
            await self.conn.execute("INSERT INTO referral_balance_adjustments(user_id, admin_user_id, amount, reason) VALUES (?, ?, ?, ?)", (user_id, admin_user_id, amount, reason))
            await self.conn.execute("UPDATE users SET balance = COALESCE(balance, 0) + ? WHERE user_id = ?", (amount, user_id))
            await self.conn.commit()
            return True

    async def get_referral_balance_adjustments(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute("SELECT * FROM referral_balance_adjustments WHERE user_id = ? ORDER BY created_at DESC LIMIT ?", (user_id, limit))
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def mark_referral_suspicious(self, user_id: int, flag: bool = True, note: str = '') -> bool:
        if not self.conn:
            return False
        async with self.lock:
            await self.conn.execute(
                "UPDATE users SET ref_suspicious = ?, partner_note = CASE WHEN ? != '' THEN ? ELSE partner_note END WHERE user_id = ?",
                (1 if flag else 0, note, note, user_id),
            )
            await self.conn.commit()
            return True

    async def get_suspicious_referrals(self, limit: int = 20) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute("SELECT user_id, ref_by, join_date, partner_note FROM users WHERE COALESCE(ref_suspicious, 0) = 1 ORDER BY join_date DESC LIMIT ?", (limit,))
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_referral_partner_cabinet(self, user_id: int) -> Dict[str, Any]:
        summary = await self.get_referral_summary(user_id)
        settings = await self.get_partner_settings(user_id)
        referrals = await self.get_referrals_list(user_id)
        total_referrals = len(referrals)
        paid_referrals = sum(1 for r in referrals if r.get('ref_rewarded'))
        trial_refs = 0
        if self.conn:
            async with self.lock:
                cur = await self.conn.execute("SELECT COUNT(*) FROM users WHERE ref_by = ? AND COALESCE(trial_used, 0) = 1", (user_id,))
                row = await cur.fetchone()
                trial_refs = int((row[0] if row else 0) or 0)
        summary.update({
            'status': settings.get('status', 'standard'),
            'custom_percent_level1': settings.get('custom_percent_level1'),
            'custom_percent_level2': settings.get('custom_percent_level2'),
            'custom_percent_level3': settings.get('custom_percent_level3'),
            'note': settings.get('note', ''),
            'suspicious': settings.get('suspicious', False),
            'trial_refs': trial_refs,
            'conversion_pct': round((paid_referrals / total_referrals * 100), 1) if total_referrals else 0.0,
        })
        return summary


    async def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        if not self.conn:
            return default
        async with self.lock:
            await self._ensure_app_settings_table()
            cur = await self.conn.execute("SELECT value FROM app_settings WHERE key = ?", (key,))
            row = await cur.fetchone()
        return row[0] if row else default

    async def set_setting(self, key: str, value: str) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            await self._ensure_app_settings_table()
            await self.conn.execute("INSERT INTO app_settings(key, value, updated_at) VALUES(?, ?, CURRENT_TIMESTAMP) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP", (key, value))
            await self.conn.commit()
            return True

    async def get_active_user_promo_code(self, user_id: int) -> str:
        return str(await self.get_setting(f"promo:active:{int(user_id)}", "") or "").strip().upper()

    async def set_active_user_promo_code(self, user_id: int, code: str) -> bool:
        normalized = (code or "").strip().upper()
        return await self.set_setting(f"promo:active:{int(user_id)}", normalized)

    async def clear_active_user_promo_code(self, user_id: int) -> bool:
        return await self.set_active_user_promo_code(user_id, "")

    async def create_or_update_promo_code(
        self,
        code: str,
        *,
        title: str = "",
        description: str = "",
        discount_percent: float = 0.0,
        discount_type: str = "percent",
        fixed_amount: float = 0.0,
        only_new_users: bool = False,
        plan_ids: str = "",
        user_limit: int = 0,
        max_uses: int = 0,
        active: bool = True,
        expires_at: Optional[str] = None,
    ) -> bool:
        if not self.conn:
            return False
        normalized = (code or "").strip().upper()
        if not normalized:
            return False
        async with self.lock:
            await self.conn.execute(
                """
                INSERT INTO promo_codes(code, title, description, discount_percent, discount_type, fixed_amount, only_new_users, plan_ids, user_limit, max_uses, active, expires_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(code) DO UPDATE SET
                    title=excluded.title,
                    description=excluded.description,
                    discount_percent=excluded.discount_percent,
                    discount_type=excluded.discount_type,
                    fixed_amount=excluded.fixed_amount,
                    only_new_users=excluded.only_new_users,
                    plan_ids=excluded.plan_ids,
                    user_limit=excluded.user_limit,
                    max_uses=excluded.max_uses,
                    active=excluded.active,
                    expires_at=excluded.expires_at,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    normalized,
                    title[:120],
                    description[:500],
                    float(discount_percent or 0.0),
                    (discount_type or "percent").strip().lower(),
                    float(fixed_amount or 0.0),
                    1 if only_new_users else 0,
                    (plan_ids or "").strip(),
                    int(user_limit or 0),
                    int(max_uses or 0),
                    1 if active else 0,
                    expires_at,
                ),
            )
            await self.conn.commit()
        return True

    async def get_promo_code(self, code: str) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        normalized = (code or "").strip().upper()
        if not normalized:
            return None
        async with self.lock:
            cursor = await self.conn.execute("SELECT * FROM promo_codes WHERE code = ?", (normalized,))
            row = await cursor.fetchone()
        return self._row_to_dict(row)

    async def list_promo_codes(self, *, active_only: bool = False, limit: int = 20) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        query = "SELECT * FROM promo_codes"
        params: List[Any] = []
        if active_only:
            query += " WHERE active = 1"
        query += " ORDER BY updated_at DESC, code ASC LIMIT ?"
        params.append(int(limit))
        async with self.lock:
            cursor = await self.conn.execute(query, tuple(params))
            rows = await cursor.fetchall()
        return self._rows_to_dicts(rows)

    async def validate_promo_code(self, code: str, *, user_id: Optional[int] = None, plan_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        promo = await self.get_promo_code(code)
        if not promo:
            return None
        if int(promo.get("active", 0) or 0) != 1:
            return None
        expires_at = str(promo.get("expires_at") or "").strip()
        if expires_at:
            try:
                expires_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
                if expires_dt.tzinfo is None:
                    expires_dt = expires_dt.replace(tzinfo=timezone.utc)
                if expires_dt <= datetime.now(timezone.utc):
                    return None
            except ValueError:
                pass
        max_uses = int(promo.get("max_uses") or 0)
        used_count = int(promo.get("used_count") or 0)
        if max_uses > 0 and used_count >= max_uses:
            return None
        user_limit = int(promo.get("user_limit") or 0)
        if user_limit > 0 and user_id is not None:
            async with self.lock:
                cursor = await self.conn.execute(
                    "SELECT used_count FROM promo_code_usages WHERE code = ? AND user_id = ?",
                    (str(promo.get("code") or "").strip().upper(), int(user_id)),
                )
                usage_row = await cursor.fetchone()
            used_by_user = int((usage_row[0] if usage_row else 0) or 0)
            if used_by_user >= user_limit:
                return None
        if int(promo.get("only_new_users") or 0) == 1 and user_id is not None:
            user = await self.get_user(int(user_id))
            if user and (int(user.get("trial_used") or 0) == 1 or int(user.get("ref_rewarded") or 0) == 1 or int(user.get("has_subscription") or 0) == 1):
                return None
        raw_plan_ids = str(promo.get("plan_ids") or "").strip()
        if raw_plan_ids and plan_id:
            allowed = {item.strip() for item in raw_plan_ids.split(",") if item.strip()}
            if allowed and plan_id not in allowed:
                return None
        return promo

    async def create_gift_link(
        self,
        *,
        token: str,
        buyer_user_id: int,
        plan_id: str,
        payment_id: str = "",
        promo_code: str = "",
        promo_discount_percent: float = 0.0,
        note: str = "",
        expires_at: Optional[str] = None,
    ) -> bool:
        if not self.conn or not token:
            return False
        async with self.lock:
            cursor = await self.conn.execute(
                """
                INSERT OR REPLACE INTO gift_links
                (token, buyer_user_id, plan_id, payment_id, promo_code, promo_discount_percent, note, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    token[:80],
                    int(buyer_user_id),
                    plan_id[:80],
                    payment_id[:120],
                    (promo_code or "").strip().upper(),
                    float(promo_discount_percent or 0.0),
                    note[:240],
                    expires_at,
                ),
            )
            await self.conn.commit()
        return bool(cursor.rowcount)

    async def get_gift_link(self, token: str) -> Optional[Dict[str, Any]]:
        if not self.conn or not token:
            return None
        async with self.lock:
            cursor = await self.conn.execute("SELECT * FROM gift_links WHERE token = ?", (token[:80],))
            row = await cursor.fetchone()
        return self._row_to_dict(row)

    async def claim_gift_link(self, token: str, user_id: int) -> bool:
        if not self.conn or not token:
            return False
        async with self.lock:
            cursor = await self.conn.execute(
                """
                UPDATE gift_links
                SET claimed_by_user_id = ?, claimed_at = CURRENT_TIMESTAMP
                WHERE token = ?
                  AND claimed_by_user_id IS NULL
                  AND (
                        expires_at IS NULL
                        OR expires_at = ''
                        OR expires_at > CURRENT_TIMESTAMP
                  )
                """,
                (int(user_id), token[:80]),
            )
            await self.conn.commit()
        return bool(cursor.rowcount)

    async def get_gift_links_by_buyer(self, buyer_user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM gift_links WHERE buyer_user_id = ? ORDER BY created_at DESC LIMIT ?",
                (int(buyer_user_id), int(limit)),
            )
            rows = await cursor.fetchall()
        return self._rows_to_dicts(rows)

    async def get_user_gift_history(self, user_id: int, limit: int = 20) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                """
                SELECT * FROM gift_links
                WHERE buyer_user_id = ? OR claimed_by_user_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (int(user_id), int(user_id), int(limit)),
            )
            rows = await cursor.fetchall()
        return self._rows_to_dicts(rows)

    async def list_unclaimed_gift_links_for_reminder(self, *, hours: int, limit: int = 20) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                """
                SELECT *
                FROM gift_links
                WHERE claimed_by_user_id IS NULL
                  AND created_at < datetime('now', '-' || ? || ' hours')
                  AND (
                        reminder_sent_at IS NULL
                        OR reminder_sent_at = ''
                        OR reminder_sent_at < datetime('now', '-' || ? || ' hours')
                  )
                  AND (
                        expires_at IS NULL
                        OR expires_at = ''
                        OR expires_at > CURRENT_TIMESTAMP
                  )
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (int(hours), int(hours), int(limit)),
            )
            rows = await cursor.fetchall()
        return self._rows_to_dicts(rows)

    async def touch_gift_link_reminder(self, token: str) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            cursor = await self.conn.execute(
                "UPDATE gift_links SET reminder_sent_at = CURRENT_TIMESTAMP WHERE token = ?",
                (token[:80],),
            )
            await self.conn.commit()
        return bool(cursor.rowcount)

    async def get_gift_links_stats(self) -> Dict[str, Any]:
        if not self.conn:
            return {"total": 0, "claimed": 0, "unclaimed": 0}
        async with self.lock:
            cursor = await self.conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN claimed_by_user_id IS NOT NULL THEN 1 ELSE 0 END) AS claimed,
                    SUM(CASE WHEN claimed_by_user_id IS NULL THEN 1 ELSE 0 END) AS unclaimed
                FROM gift_links
                """
            )
            row = await cursor.fetchone()
        return {
            "total": int((row[0] if row else 0) or 0),
            "claimed": int((row[1] if row else 0) or 0),
            "unclaimed": int((row[2] if row else 0) or 0),
        }

    async def get_promo_code_stats(self) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                """
                SELECT
                    p.code,
                    p.discount_type,
                    p.discount_percent,
                    p.fixed_amount,
                    p.used_count,
                    COUNT(pp.payment_id) AS payments_count,
                    COALESCE(SUM(pp.amount), 0) AS total_amount
                FROM promo_codes p
                LEFT JOIN pending_payments pp ON pp.promo_code = p.code AND pp.status IN ('accepted', 'refunded')
                GROUP BY p.code, p.discount_type, p.discount_percent, p.fixed_amount, p.used_count
                ORDER BY payments_count DESC, p.code ASC
                """
            )
            rows = await cursor.fetchall()
        return self._rows_to_dicts(rows)

    async def get_promo_code_usage_details(self, code: str, limit: int = 20) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        normalized = (code or "").strip().upper()
        if not normalized:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                """
                SELECT user_id, used_count, last_used_at
                FROM promo_code_usages
                WHERE code = ?
                ORDER BY used_count DESC, last_used_at DESC
                LIMIT ?
                """,
                (normalized, int(limit)),
            )
            rows = await cursor.fetchall()
        return self._rows_to_dicts(rows)

    async def mark_promo_code_used(self, code: str, *, user_id: Optional[int] = None) -> bool:
        if not self.conn:
            return False
        normalized = (code or "").strip().upper()
        if not normalized:
            return False
        async with self.lock:
            cursor = await self.conn.execute(
                """
                UPDATE promo_codes
                SET used_count = COALESCE(used_count, 0) + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE code = ?
                  AND active = 1
                  AND (
                        COALESCE(max_uses, 0) = 0
                        OR COALESCE(used_count, 0) < max_uses
                  )
                """,
                (normalized,),
            )
            if cursor.rowcount and user_id is not None:
                await self.conn.execute(
                    """
                    INSERT INTO promo_code_usages(code, user_id, used_count, last_used_at)
                    VALUES (?, ?, 1, CURRENT_TIMESTAMP)
                    ON CONFLICT(code, user_id) DO UPDATE SET
                        used_count = promo_code_usages.used_count + 1,
                        last_used_at = CURRENT_TIMESTAMP
                    """,
                    (normalized, int(user_id)),
                )
            await self.conn.commit()
        return bool(cursor.rowcount)

    async def register_transient_message(self, chat_id: int, message_id: int, *, category: str = "", ttl_hours: int = 24) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            cursor = await self.conn.execute(
                """
                INSERT INTO transient_messages(chat_id, message_id, category, expires_at)
                VALUES (?, ?, ?, datetime('now', '+' || ? || ' hours'))
                """,
                (int(chat_id), int(message_id), category[:80], int(ttl_hours)),
            )
            await self.conn.commit()
        return int(cursor.lastrowid or 0)

    async def get_expired_transient_messages(self, limit: int = 100) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                """
                SELECT * FROM transient_messages
                WHERE expires_at <= CURRENT_TIMESTAMP
                ORDER BY expires_at ASC, id ASC
                LIMIT ?
                """,
                (int(limit),),
            )
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def delete_transient_message_record(self, record_id: int) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            cursor = await self.conn.execute("DELETE FROM transient_messages WHERE id = ?", (int(record_id),))
            await self.conn.commit()
        return bool(cursor.rowcount)

    async def get_or_create_support_ticket(self, user_id: int) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            await self._ensure_support_tables()
            cur = await self.conn.execute("SELECT id FROM support_tickets WHERE user_id = ? AND status = 'open' ORDER BY id DESC LIMIT 1", (user_id,))
            row = await cur.fetchone()
            if row:
                return int(row[0])
            cur = await self.conn.execute("INSERT INTO support_tickets(user_id) VALUES(?)", (user_id,))
            await self.conn.commit()
            return int(cur.lastrowid)

    async def add_support_message(self, ticket_id: int, sender_role: str, sender_user_id: int, text: str, media_type: str = "", media_file_id: str = "") -> int:
        if not self.conn:
            return 0
        async with self.lock:
            await self._ensure_support_tables()
            cur = await self.conn.execute("INSERT INTO support_messages(ticket_id, sender_role, sender_user_id, text, media_type, media_file_id) VALUES(?, ?, ?, ?, ?, ?)", (ticket_id, sender_role, sender_user_id, text, media_type, media_file_id))
            await self.conn.execute("UPDATE support_tickets SET updated_at = CURRENT_TIMESTAMP, status = CASE WHEN status='closed' THEN 'open' ELSE status END WHERE id = ?", (ticket_id,))
            await self.conn.commit()
            return int(cur.lastrowid)

    async def get_last_support_message(self, ticket_id: int, sender_role: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        async with self.lock:
            if sender_role:
                cur = await self.conn.execute("SELECT * FROM support_messages WHERE ticket_id = ? AND sender_role = ? ORDER BY id DESC LIMIT 1", (ticket_id, sender_role))
            else:
                cur = await self.conn.execute("SELECT * FROM support_messages WHERE ticket_id = ? ORDER BY id DESC LIMIT 1", (ticket_id,))
            row = await cur.fetchone()
        return dict(row) if row else None

    async def get_support_ticket(self, ticket_id: int) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        async with self.lock:
            await self._ensure_support_tables()
            cur = await self.conn.execute("SELECT * FROM support_tickets WHERE id = ?", (ticket_id,))
            row = await cur.fetchone()
        return dict(row) if row else None

    async def close_support_ticket(self, ticket_id: int) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            await self._ensure_support_tables()
            await self.conn.execute("UPDATE support_tickets SET status='closed', updated_at=CURRENT_TIMESTAMP WHERE id = ?", (ticket_id,))
            await self.conn.commit()
            return True


    async def list_open_support_tickets(self, limit: int = 20) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            await self._ensure_support_tables()
            cur = await self.conn.execute("SELECT * FROM support_tickets WHERE status IN ('open','in_progress') ORDER BY updated_at DESC LIMIT ?", (limit,))
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def list_stale_support_tickets(self, *, minutes: int = 45, limit: int = 20) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            await self._ensure_support_tables()
            cur = await self.conn.execute(
                """
                SELECT *
                FROM support_tickets
                WHERE status IN ('open', 'in_progress')
                  AND updated_at <= datetime('now', '-' || ? || ' minutes')
                ORDER BY updated_at ASC, id ASC
                LIMIT ?
                """,
                (int(minutes), int(limit)),
            )
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def list_user_support_tickets(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            await self._ensure_support_tables()
            cur = await self.conn.execute(
                "SELECT * FROM support_tickets WHERE user_id = ? ORDER BY updated_at DESC, id DESC LIMIT ?",
                (user_id, limit),
            )
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def archive_closed_support_tickets(self, days: int = 14) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            await self._ensure_support_tables()
            cursor = await self.conn.execute(
                """
                UPDATE support_tickets
                SET status='archived', updated_at=CURRENT_TIMESTAMP
                WHERE status='closed'
                  AND updated_at < datetime('now', '-' || ? || ' days')
                """,
                (int(days),),
            )
            await self.conn.commit()
        return int(cursor.rowcount or 0)

    async def get_support_daily_report(self, *, days_ago: int = 0) -> Dict[str, Any]:
        if not self.conn:
            return {
                "report_date": "",
                "opened_tickets": 0,
                "closed_tickets": 0,
                "messages_from_users": 0,
                "messages_from_admins": 0,
                "open_tickets": 0,
            }
        day_expr = f"-{max(0, int(days_ago))}"
        async with self.lock:
            await self._ensure_support_tables()
            opened_cur = await self.conn.execute(
                "SELECT COUNT(*) FROM support_tickets WHERE date(created_at) = date('now', ? || ' day')",
                (day_expr,),
            )
            opened_row = await opened_cur.fetchone()
            closed_cur = await self.conn.execute(
                """
                SELECT COUNT(*)
                FROM support_tickets
                WHERE status IN ('closed', 'archived')
                  AND date(updated_at) = date('now', ? || ' day')
                """,
                (day_expr,),
            )
            closed_row = await closed_cur.fetchone()
            user_msg_cur = await self.conn.execute(
                "SELECT COUNT(*) FROM support_messages WHERE sender_role = 'user' AND date(created_at) = date('now', ? || ' day')",
                (day_expr,),
            )
            user_msg_row = await user_msg_cur.fetchone()
            admin_msg_cur = await self.conn.execute(
                "SELECT COUNT(*) FROM support_messages WHERE sender_role = 'admin' AND date(created_at) = date('now', ? || ' day')",
                (day_expr,),
            )
            admin_msg_row = await admin_msg_cur.fetchone()
            open_cur = await self.conn.execute(
                "SELECT COUNT(*) FROM support_tickets WHERE status IN ('open', 'in_progress')"
            )
            open_row = await open_cur.fetchone()
        return {
            "report_date": (datetime.now(timezone.utc) - timedelta(days=max(0, int(days_ago)))).date().isoformat(),
            "opened_tickets": int((opened_row[0] if opened_row else 0) or 0),
            "closed_tickets": int((closed_row[0] if closed_row else 0) or 0),
            "messages_from_users": int((user_msg_row[0] if user_msg_row else 0) or 0),
            "messages_from_admins": int((admin_msg_row[0] if admin_msg_row else 0) or 0),
            "open_tickets": int((open_row[0] if open_row else 0) or 0),
        }

    async def set_support_ticket_status(self, ticket_id: int, status: str, assigned_admin_id: Optional[int] = None) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            await self._ensure_support_tables()
            if assigned_admin_id is None:
                await self.conn.execute("UPDATE support_tickets SET status=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", (status, ticket_id))
            else:
                await self.conn.execute("UPDATE support_tickets SET status=?, assigned_admin_id=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", (status, assigned_admin_id, ticket_id))
            await self.conn.commit()
            return True

    async def get_support_ticket_reminder_state(self, ticket_id: int) -> str:
        return str(await self.get_setting(f"support:ticket_reminder_sent:{int(ticket_id)}", "") or "")

    async def set_support_ticket_reminder_state(self, ticket_id: int, value: str) -> bool:
        return await self.set_setting(f"support:ticket_reminder_sent:{int(ticket_id)}", value[:50])

    async def get_support_messages(self, ticket_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cur = await self.conn.execute("SELECT * FROM support_messages WHERE ticket_id = ? ORDER BY id ASC LIMIT ?", (ticket_id, limit))
            rows = await cur.fetchall()
        return [dict(row) for row in rows]

    async def ensure_ref_code(self, user_id: int) -> Optional[str]:
        user = await self.get_user(user_id)
        if not user:
            await self.add_user(user_id)
            user = await self.get_user(user_id)

        if not user:
            return None

        if user.get("ref_code"):
            return user.get("ref_code")

        for _ in range(20):
            code = generate_ref_code()
            existing = await self.get_user_by_ref_code(code)
            if existing:
                continue
            updated = await self.update_user(user_id, ref_code=code)
            if updated:
                return code

        return None

    async def ensure_panel_client_key(self, user_id: int) -> Optional[str]:
        user = await self.get_user(user_id)
        if not user:
            await self.add_user(user_id)
            user = await self.get_user(user_id)

        if not user:
            return None

        current = str(user.get("panel_client_key") or "").strip()
        if current:
            return current

        try:
            candidate = str(int(user_id))
        except (TypeError, ValueError):
            candidate = ""

        if candidate:
            existing = await self.get_user_by_panel_client_key(candidate)
            if not existing or int(existing.get("user_id") or 0) == int(user_id):
                updated = await self.update_user(user_id, panel_client_key=candidate)
                if updated:
                    return candidate

        for _ in range(50):
            fallback = f"p{secrets.token_hex(6)}"
            existing = await self.get_user_by_panel_client_key(fallback)
            if existing:
                continue
            updated = await self.update_user(user_id, panel_client_key=fallback)
            if updated:
                return fallback

        return None

    async def get_user_by_panel_client_key(self, panel_client_key: str) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        normalized = str(panel_client_key or "").strip()
        if not normalized:
            return None
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM users WHERE panel_client_key = ?",
                (normalized,),
            )
            row = await cursor.fetchone()
        return self._row_to_dict(row)

    # --- Работа с балансом ---
    async def get_balance(self, user_id: int) -> float:
        user = await self.get_user(user_id)
        return user.get("balance", 0.0) if user else 0.0

    async def add_balance(self, user_id: int, amount: float) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            cursor = await self.conn.execute(
                "UPDATE users SET balance = COALESCE(balance, 0) + ? WHERE user_id = ?",
                (amount, user_id),
            )
            await self.conn.commit()
            return cursor.rowcount > 0

    async def subtract_balance(self, user_id: int, amount: float) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            cursor = await self.conn.execute(
                "UPDATE users SET balance = COALESCE(balance, 0) - ? WHERE user_id = ? AND balance >= ?",
                (amount, user_id, amount),
            )
            await self.conn.commit()
            return cursor.rowcount > 0

    # --- Работа с запросами на вывод ---
    async def create_withdraw_request(self, user_id: int, amount: float) -> int:
        if not self.conn or amount <= 0:
            return 0
        async with self.lock:
            existing = await self.conn.execute(
                "SELECT id FROM withdraw_requests WHERE user_id = ? AND status = 'pending' ORDER BY created_at DESC LIMIT 1",
                (user_id,),
            )
            existing_row = await existing.fetchone()
            if existing_row:
                return int(existing_row[0])

            cursor = await self.conn.execute(
                "INSERT INTO withdraw_requests (user_id, amount) VALUES (?, ?)",
                (user_id, amount),
            )
            await self.conn.commit()
            return cursor.lastrowid

    async def get_pending_withdraw_requests(self) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM withdraw_requests WHERE status = 'pending' ORDER BY created_at ASC"
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_withdraw_request(self, request_id: int) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM withdraw_requests WHERE id = ?",
                (request_id,),
            )
            row = await cursor.fetchone()
            return self._row_to_dict(row)

    async def get_user_pending_withdraw_request(self, user_id: int) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM withdraw_requests WHERE user_id = ? AND status = 'pending' ORDER BY created_at DESC LIMIT 1",
                (user_id,),
            )
            row = await cursor.fetchone()
            return self._row_to_dict(row)

    async def get_withdraw_requests_by_user(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM withdraw_requests WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
                (user_id, limit),
            )
            rows = await cursor.fetchall()
            return self._rows_to_dicts(rows)

    async def process_withdraw_request(self, request_id: int, accept: bool) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT user_id, amount FROM withdraw_requests WHERE id = ? AND status = 'pending'",
                (request_id,),
            )
            row = await cursor.fetchone()
            if not row:
                return False

            if accept:
                user_id = row["user_id"]
                amount = row["amount"]
                debit_cursor = await self.conn.execute(
                    "UPDATE users SET balance = COALESCE(balance, 0) - ? WHERE user_id = ? AND balance >= ?",
                    (amount, user_id, amount),
                )
                if debit_cursor.rowcount <= 0:
                    logger.warning(
                        "Withdraw request denied: insufficient balance user=%s request=%s",
                        user_id,
                        request_id,
                    )
                    await self.conn.commit()
                    return False

                status_cursor = await self.conn.execute(
                    "UPDATE withdraw_requests SET status = 'completed', processed_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'pending'",
                    (request_id,),
                )
            else:
                status_cursor = await self.conn.execute(
                    "UPDATE withdraw_requests SET status = 'rejected', processed_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'pending'",
                    (request_id,),
                )
            await self.conn.commit()
            return status_cursor.rowcount > 0


    async def add_pending_payment(
        self,
        payment_id,
        user_id,
        plan_id,
        amount,
        msg_id=None,
        provider=None,
        *,
        recipient_user_id: Optional[int] = None,
        promo_code: str = "",
        promo_discount_percent: float = 0.0,
        gift_label: str = "",
    ) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            try:
                cursor = await self.conn.execute(
                    """
                    INSERT OR IGNORE INTO pending_payments
                    (payment_id, user_id, plan_id, amount, msg_id, provider, recipient_user_id, promo_code, promo_discount_percent, gift_label)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payment_id,
                        user_id,
                        plan_id,
                        amount,
                        msg_id,
                        provider or Config.PAYMENT_PROVIDER,
                        recipient_user_id,
                        (promo_code or "").strip().upper(),
                        float(promo_discount_percent or 0.0),
                        gift_label[:120],
                    ),
                )
                await self.conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error("Pending payment insert failed: %s", e)
                return False

    async def get_pending_payment(self, payment_id) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        async with self.lock:
            cursor = await self.conn.execute("SELECT * FROM pending_payments WHERE payment_id = ?", (payment_id,))
            row = await cursor.fetchone()
        return self._row_to_dict(row)

    async def get_pending_payment_by_itpay_id(self, itpay_id) -> Optional[Dict[str, Any]]:
        return await self.get_pending_payment_by_provider_id("itpay", itpay_id)

    async def set_pending_payment_itpay_id(self, payment_id, itpay_id) -> bool:
        return await self.set_pending_payment_provider_id(payment_id, "itpay", itpay_id)

    async def get_pending_payment_by_provider_id(self, provider: str, provider_payment_id: str) -> Optional[Dict[str, Any]]:
        if not self.conn or not provider_payment_id:
            return None
        normalized_provider = self._normalize_payment_provider(provider)
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM pending_payments WHERE provider = ? AND (provider_payment_id = ? OR (provider = 'itpay' AND itpay_id = ?))",
                (normalized_provider, provider_payment_id, provider_payment_id),
            )
            row = await cursor.fetchone()
        return self._row_to_dict(row)

    async def set_pending_payment_provider_id(self, payment_id, provider: str, provider_payment_id: str) -> bool:
        if not self.conn or not provider_payment_id:
            return False
        async with self.lock:
            try:
                normalized_provider = self._normalize_payment_provider(provider)
                cursor = await self.conn.execute(
                    "SELECT payment_id FROM pending_payments WHERE provider = ? AND provider_payment_id = ? LIMIT 1",
                    (normalized_provider, provider_payment_id),
                )
                existing = await cursor.fetchone()
                if existing and str(existing[0]) != str(payment_id):
                    logger.warning(
                        "Provider payment id conflict: provider=%s provider_payment_id=%s existing=%s requested=%s",
                        normalized_provider,
                        provider_payment_id,
                        existing[0],
                        payment_id,
                    )
                    return False
                if existing and str(existing[0]) == str(payment_id):
                    return True

                if normalized_provider == "itpay":
                    cursor = await self.conn.execute(
                        "UPDATE pending_payments SET provider = ?, provider_payment_id = ?, itpay_id = ? WHERE payment_id = ?",
                        (normalized_provider, provider_payment_id, provider_payment_id, payment_id),
                    )
                else:
                    cursor = await self.conn.execute(
                        "UPDATE pending_payments SET provider = ?, provider_payment_id = ? WHERE payment_id = ?",
                        (normalized_provider, provider_payment_id, payment_id),
                    )
                await self.conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error("Set provider payment id failed: %s", e)
                return False

    async def claim_pending_payment(self, payment_id: str, *, source: str = "", reason: str = "", metadata: str = "") -> bool:
        if not self.conn:
            return False
        async with self.lock:
            try:
                cursor = await self.conn.execute(
                    """
                    UPDATE pending_payments
                    SET status = 'processing',
                        processed_at = NULL,
                        processing_started_at = CURRENT_TIMESTAMP,
                        next_retry_at = NULL,
                        activation_attempts = COALESCE(activation_attempts, 0) + 1
                    WHERE payment_id = ? AND status = 'pending'
                    """,
                    (payment_id,),
                )
                if cursor.rowcount > 0:
                    await self.conn.execute(
                        """
                        INSERT INTO payment_status_history (payment_id, from_status, to_status, source, reason, metadata)
                        VALUES (?, 'pending', 'processing', ?, ?, ?)
                        """,
                        (payment_id, source[:120], reason[:500], metadata[:2000]),
                    )
                await self.conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error("Claim pending payment failed: %s", e)
                return False

    async def release_processing_payment(self, payment_id: str, error_text: Optional[str] = None, *, source: str = "", metadata: str = "", retry_delay_sec: int = 0) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            try:
                safe_delay_sec = max(0, int(retry_delay_sec or 0))
                cursor = await self.conn.execute(
                    """
                    UPDATE pending_payments
                    SET status = 'pending',
                        processed_at = NULL,
                        processing_started_at = NULL,
                        next_retry_at = CASE
                            WHEN ? > 0 THEN datetime('now', '+' || ? || ' seconds')
                            ELSE NULL
                        END,
                        last_error = COALESCE(?, last_error)
                    WHERE payment_id = ? AND status = 'processing'
                    """,
                    (safe_delay_sec, safe_delay_sec, error_text, payment_id),
                )
                if cursor.rowcount > 0:
                    await self.conn.execute(
                        """
                        INSERT INTO payment_status_history (payment_id, from_status, to_status, source, reason, metadata)
                        VALUES (?, 'processing', 'pending', ?, ?, ?)
                        """,
                        (payment_id, source[:120], (error_text or '')[:500], metadata[:2000]),
                    )
                await self.conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error("Release processing payment failed: %s", e)
                return False

    async def mark_payment_error(self, payment_id: str, error_text: str) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            try:
                cursor = await self.conn.execute(
                    "UPDATE pending_payments SET last_error = ? WHERE payment_id = ?",
                    (error_text[:500], payment_id),
                )
                await self.conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error("Mark payment error failed: %s", e)
                return False

    async def reclaim_stale_processing_payments(self, timeout_minutes: int = 15, *, source: str = "system/recovery") -> int:
        if not self.conn:
            return 0
        async with self.lock:
            try:
                cursor = await self.conn.execute(
                    """
                    SELECT payment_id FROM pending_payments
                    WHERE status = 'processing'
                      AND processing_started_at IS NOT NULL
                      AND processing_started_at < datetime('now', '-' || ? || ' minutes')
                    """,
                    (timeout_minutes,),
                )
                stale_rows = await cursor.fetchall()
                stale_ids = [str(row[0]) for row in stale_rows]

                cursor = await self.conn.execute(
                    """
                    UPDATE pending_payments
                    SET status = 'pending',
                        processing_started_at = NULL,
                        last_error = CASE
                            WHEN COALESCE(last_error, '') = '' THEN 'auto-released stale processing lock'
                            ELSE last_error
                        END
                    WHERE status = 'processing'
                      AND processing_started_at IS NOT NULL
                      AND processing_started_at < datetime('now', '-' || ? || ' minutes')
                    """,
                    (timeout_minutes,),
                )
                if stale_ids:
                    for payment_id in stale_ids:
                        await self.conn.execute(
                            """
                            INSERT INTO payment_status_history (payment_id, from_status, to_status, source, reason, metadata)
                            VALUES (?, 'processing', 'pending', ?, 'auto-released stale processing lock', '')
                            """,
                            (payment_id, source[:120]),
                        )
                await self.conn.commit()
                return cursor.rowcount
            except Exception as e:
                logger.error("Reclaim stale processing payments failed: %s", e)
                return 0

    async def get_all_pending_payments(self, statuses: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        effective_statuses = statuses or ["pending"]
        placeholders = ",".join(["?"] * len(effective_statuses))
        query = f"SELECT * FROM pending_payments WHERE status IN ({placeholders}) ORDER BY created_at ASC"
        async with self.lock:
            async with self.conn.execute(query, tuple(effective_statuses)) as c:
                rows = await c.fetchall()
        return [dict(r) for r in rows]

    async def get_pending_payments_by_user(self, user_id: int) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute(
                "SELECT * FROM pending_payments WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,),
            ) as c:
                rows = await c.fetchall()
        return [dict(r) for r in rows]

    async def get_user_pending_payment(self, user_id: int, *, plan_id: Optional[str] = None, statuses: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        if not self.conn:
            return None
        effective_statuses = statuses or ["pending", "processing"]
        placeholders = ",".join(["?"] * len(effective_statuses))
        params: List[Any] = [user_id, *effective_statuses]
        query = f"SELECT * FROM pending_payments WHERE user_id = ? AND status IN ({placeholders})"
        if plan_id is not None:
            query += " AND plan_id = ?"
            params.append(plan_id)
        query += " ORDER BY created_at DESC LIMIT 1"
        async with self.lock:
            async with self.conn.execute(query, tuple(params)) as c:
                row = await c.fetchone()
        return dict(row) if row else None

    async def update_payment_status(self, payment_id, status, allowed_current_statuses=None, *, source: str = "", reason: str = "", metadata: str = "") -> bool:
        if not self.conn:
            return False
        current_statuses = allowed_current_statuses or ["pending"]
        async with self.lock:
            try:
                cursor = await self.conn.execute("SELECT status FROM pending_payments WHERE payment_id = ?", (payment_id,))
                row = await cursor.fetchone()
                if not row:
                    return False
                current_status = str(row[0] or "")
                if current_status not in current_statuses:
                    return False
                if not can_transition(current_status, status):
                    logger.warning("Payment status transition denied: %s -> %s for %s", current_status, status, payment_id)
                    return False
                cursor = await self.conn.execute(
                    "UPDATE pending_payments SET status = ?, processed_at = CURRENT_TIMESTAMP, processing_started_at = NULL, next_retry_at = NULL, last_error = '' WHERE payment_id = ? AND status = ?",
                    (status, payment_id, current_status),
                )
                if cursor.rowcount > 0:
                    await self.conn.execute(
                        """
                        INSERT INTO payment_status_history (payment_id, from_status, to_status, source, reason, metadata)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (payment_id, current_status[:120], status, source[:120], reason[:500], metadata[:2000]),
                    )
                await self.conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error("Update payment status failed: %s", e)
                return False

    async def record_payment_status_transition(
        self,
        payment_id: str,
        *,
        from_status: Optional[str],
        to_status: str,
        source: str = "",
        reason: str = "",
        metadata: str = "",
    ) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            cursor = await self.conn.execute(
                """
                INSERT INTO payment_status_history (payment_id, from_status, to_status, source, reason, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (payment_id, from_status, to_status, source[:120], reason[:500], metadata[:2000]),
            )
            await self.conn.commit()
        return int(cursor.lastrowid or 0)

    async def get_payment_status_history(self, payment_id: str) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM payment_status_history WHERE payment_id = ? ORDER BY id ASC",
                (payment_id,),
            )
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_recent_payment_events(self, payment_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM payment_event_dedup WHERE payment_id = ? ORDER BY created_at DESC, event_key DESC LIMIT ?",
                (payment_id, int(limit)),
            )
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_payment_provider_counts(self) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT COALESCE(provider, '') AS provider, COUNT(*) AS total, SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending, SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) AS processing, SUM(CASE WHEN status = 'accepted' THEN 1 ELSE 0 END) AS accepted, SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) AS rejected FROM pending_payments GROUP BY COALESCE(provider, '') ORDER BY provider ASC"
            )
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def register_payment_event(
        self,
        event_key: str,
        *,
        payment_id: str = "",
        source: str = "",
        event_type: str = "",
        payload_excerpt: str = "",
    ) -> bool:
        if not self.conn:
            return False
        async with self.lock:
            try:
                cursor = await self.conn.execute(
                    """
                    INSERT OR IGNORE INTO payment_event_dedup (event_key, payment_id, source, event_type, payload_excerpt)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (event_key[:255], payment_id, source[:120], event_type[:120], payload_excerpt[:1000]),
                )
                await self.conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error("Register payment event failed: %s", e)
                return False


    async def add_payment_admin_action(self, payment_id: str, admin_user_id: int, action: str, *, provider: str = "", result: str = "", details: str = "") -> int:
        if not self.conn:
            return 0
        async with self.lock:
            cursor = await self.conn.execute(
                """
                INSERT INTO payment_admin_actions (payment_id, admin_user_id, action, provider, result, details)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (payment_id, int(admin_user_id), action[:120], provider[:80], result[:120], details[:2000]),
            )
            await self.conn.commit()
        return int(cursor.lastrowid or 0)


    async def get_recent_payment_admin_actions(self, *, limit: int = 20, provider: Optional[str] = None) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        query = "SELECT * FROM payment_admin_actions"
        params: List[Any] = []
        if provider:
            query += " WHERE provider = ?"
            params.append(provider)
        query += " ORDER BY created_at DESC, id DESC LIMIT ?"
        params.append(int(limit))
        async with self.lock:
            cursor = await self.conn.execute(query, tuple(params))
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_payment_admin_actions(self, payment_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            cursor = await self.conn.execute(
                "SELECT * FROM payment_admin_actions WHERE payment_id = ? ORDER BY created_at DESC, id DESC LIMIT ?",
                (payment_id, int(limit)),
            )
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]


    async def get_auto_resolve_action_stats(self, payment_id: str, action: str) -> Dict[str, Any]:
        if not self.conn:
            return {"attempts": 0, "last_created_at": None}
        async with self.lock:
            cursor = await self.conn.execute(
                """
                SELECT COUNT(*) AS attempts, MAX(created_at) AS last_created_at
                FROM payment_admin_actions
                WHERE payment_id = ? AND action = ?
                """,
                (payment_id, action[:120]),
            )
            row = await cursor.fetchone()
        if not row:
            return {"attempts": 0, "last_created_at": None}
        return {"attempts": int(row[0] or 0), "last_created_at": row[1]}

    async def get_pending_payment_operations(self, *, limit: int = 20, provider: Optional[str] = None, operation: Optional[str] = None) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
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
                h.to_status AS requested_status,
                h.source AS requested_source,
                h.reason AS requested_reason,
                h.metadata AS requested_metadata,
                h.created_at AS requested_at
            FROM pending_payments p
            JOIN (
                SELECT h1.*
                FROM payment_status_history h1
                JOIN (
                    SELECT payment_id, MAX(id) AS max_id
                    FROM payment_status_history
                    WHERE to_status IN ('refund_requested', 'cancel_requested')
                    GROUP BY payment_id
                ) latest ON latest.max_id = h1.id
            ) h ON h.payment_id = p.payment_id
            WHERE p.status NOT IN ('refunded', 'cancelled', 'rejected')
        """
        params: List[Any] = []
        if provider and provider != 'all':
            query += " AND p.provider = ?"
            params.append(provider)
        if operation == 'refund':
            query += " AND h.to_status = 'refund_requested'"
        elif operation == 'cancel':
            query += " AND h.to_status = 'cancel_requested'"
        query += " ORDER BY h.created_at DESC, p.created_at DESC LIMIT ?"
        params.append(int(limit))
        async with self.lock:
            cursor = await self.conn.execute(query, tuple(params))
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_stale_processing_payments(self, *, minutes: int = 15, limit: int = 20, provider: Optional[str] = None) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        query = """
            SELECT *
            FROM pending_payments
            WHERE status = 'processing'
              AND processing_started_at IS NOT NULL
              AND processing_started_at < datetime('now', '-' || ? || ' minutes')
        """
        params: List[Any] = [int(minutes)]
        if provider and provider != 'all':
            query += " AND provider = ?"
            params.append(provider)
        query += " ORDER BY processing_started_at ASC, created_at ASC LIMIT ?"
        params.append(int(limit))
        async with self.lock:
            cursor = await self.conn.execute(query, tuple(params))
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_overdue_payment_operations(self, *, minutes: int = 20, limit: int = 20, provider: Optional[str] = None) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
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
                h.to_status AS requested_status,
                h.source AS requested_source,
                h.reason AS requested_reason,
                h.metadata AS requested_metadata,
                h.created_at AS requested_at
            FROM pending_payments p
            JOIN (
                SELECT h1.*
                FROM payment_status_history h1
                JOIN (
                    SELECT payment_id, MAX(id) AS max_id
                    FROM payment_status_history
                    WHERE to_status IN ('refund_requested', 'cancel_requested')
                    GROUP BY payment_id
                ) latest ON latest.max_id = h1.id
            ) h ON h.payment_id = p.payment_id
            WHERE p.status NOT IN ('refunded', 'cancelled', 'rejected')
              AND h.created_at < datetime('now', '-' || ? || ' minutes')
        """
        params: List[Any] = [int(minutes)]
        if provider and provider != 'all':
            query += " AND p.provider = ?"
            params.append(provider)
        query += " ORDER BY h.created_at ASC, p.created_at ASC LIMIT ?"
        params.append(int(limit))
        async with self.lock:
            cursor = await self.conn.execute(query, tuple(params))
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_confirmed_payment_status_mismatches(self, *, hours: int = 24, limit: int = 20, provider: Optional[str] = None) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
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
            FROM pending_payments p
            JOIN (
                SELECT e1.*
                FROM payment_event_dedup e1
                JOIN (
                    SELECT payment_id, MAX(created_at || '|' || event_key) AS max_marker
                    FROM payment_event_dedup
                    WHERE payment_id != ''
                      AND created_at >= datetime('now', '-' || ? || ' hours')
                      AND event_type IN ('payment.succeeded', 'payment.completed', 'payment.pay', 'successful_payment', 'payment.canceled', 'refund.succeeded')
                    GROUP BY payment_id
                ) latest ON (e1.created_at || '|' || e1.event_key) = latest.max_marker
            ) e ON e.payment_id = p.payment_id
            WHERE (
                (e.event_type IN ('payment.succeeded', 'payment.completed', 'payment.pay', 'successful_payment') AND p.status NOT IN ('accepted', 'refunded'))
                OR (e.event_type IN ('payment.canceled') AND p.status NOT IN ('rejected', 'cancelled'))
                OR (e.event_type IN ('refund.succeeded') AND p.status != 'refunded')
            )
        """
        params: List[Any] = [int(hours)]
        if provider and provider != 'all':
            query += " AND p.provider = ?"
            params.append(provider)
        query += " ORDER BY e.created_at DESC, p.created_at DESC LIMIT ?"
        params.append(int(limit))
        async with self.lock:
            cursor = await self.conn.execute(query, tuple(params))
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]


    async def cleanup_old_payment_events(self, days: int = 30) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            try:
                cursor = await self.conn.execute(
                    "DELETE FROM payment_event_dedup WHERE created_at < datetime('now', '-' || ? || ' days')",
                    (days,),
                )
                await self.conn.commit()
                return cursor.rowcount
            except Exception as e:
                logger.error("Cleanup old payment events failed: %s", e)
                return 0

    async def get_processing_payments_count(self) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute("SELECT COUNT(*) FROM pending_payments WHERE status = 'processing'") as c:
                row = await c.fetchone()
        return int(row[0]) if row else 0

    async def get_old_pending_payments(self, minutes: int = 10) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute(
                "SELECT * FROM pending_payments WHERE status = 'pending' AND created_at < datetime('now', '-' || ? || ' minutes') ORDER BY created_at ASC",
                (minutes,),
            ) as c:
                rows = await c.fetchall()
        return [dict(r) for r in rows]

    async def get_recent_payment_errors(self, hours: int = 24) -> List[Dict[str, Any]]:
        if not self.conn:
            return []
        async with self.lock:
            async with self.conn.execute(
                "SELECT * FROM pending_payments WHERE COALESCE(last_error, '') != '' AND created_at >= datetime('now', '-' || ? || ' hours') ORDER BY created_at DESC",
                (hours,),
            ) as c:
                rows = await c.fetchall()
        return [dict(r) for r in rows]

    async def count_user_payments_created_since(self, user_id: int, seconds: int) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT COUNT(*) FROM pending_payments WHERE user_id = ? AND created_at >= datetime('now', '-' || ? || ' seconds')",
                (user_id, seconds),
            ) as c:
                row = await c.fetchone()
        return int(row[0]) if row else 0

    async def count_user_pending_payments(self, user_id: int) -> int:
        if not self.conn:
            return 0
        async with self.lock:
            async with self.conn.execute(
                "SELECT COUNT(*) FROM pending_payments WHERE user_id = ? AND status IN ('pending', 'processing')",
                (user_id,),
            ) as c:
                row = await c.fetchone()
        return int(row[0]) if row else 0

    async def cleanup_old_pending_payments(self, days=30) -> int:
        if not self.conn: return 0
        async with self.lock:
            cursor = await self.conn.execute("DELETE FROM pending_payments WHERE status IN ('accepted','rejected') AND processed_at < datetime('now', '-' || ? || ' days')", (days,))
            await self.conn.commit()
        return cursor.rowcount
