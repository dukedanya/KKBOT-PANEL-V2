#!/usr/bin/env python3
import argparse
import gzip
import sqlite3
import tempfile
from pathlib import Path


def _materialize_backup(path: Path) -> Path:
    if path.suffix != ".gz":
        return path
    tmp_fd, tmp_name = tempfile.mkstemp(prefix="restore-check-", suffix=".sqlite3")
    Path(tmp_name).unlink(missing_ok=True)
    out_path = Path(tmp_name)
    with gzip.open(path, "rb") as src, out_path.open("wb") as dst:
        dst.write(src.read())
    return out_path


def _check_integrity(db_path: Path) -> tuple[bool, str]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("PRAGMA integrity_check").fetchone()
        result = str(row[0] if row else "unknown")
        if result.lower() != "ok":
            return False, result
        required_tables = {
            "users",
            "pending_payments",
            "schema_version",
            "schema_migrations",
            "support_tickets",
            "support_messages",
        }
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        existing = {str(r[0]) for r in rows}
        missing = sorted(required_tables - existing)
        if missing:
            return False, "missing_tables:" + ",".join(missing)
    return True, "ok"


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate that a SQLite backup can be restored.")
    parser.add_argument("backup_file", help="Path to backup .sqlite3 or .sqlite3.gz")
    args = parser.parse_args()

    backup = Path(args.backup_file).expanduser().resolve()
    if not backup.exists():
        raise SystemExit(f"Backup file not found: {backup}")

    materialized = _materialize_backup(backup)
    remove_materialized = materialized != backup
    try:
        ok, message = _check_integrity(materialized)
        if not ok:
            raise SystemExit(f"RESTORE_CHECK_FAILED: {message}")
        print("RESTORE_CHECK_OK")
    finally:
        if remove_materialized:
            materialized.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
