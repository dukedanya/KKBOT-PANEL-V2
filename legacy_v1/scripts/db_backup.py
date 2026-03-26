#!/usr/bin/env python3
import argparse
import gzip
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from config import Config


def build_backup_path(output_dir: Path, source_db: Path, *, gzip_enabled: bool) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = ".sqlite3.gz" if gzip_enabled else ".sqlite3"
    return output_dir / f"{source_db.stem}-backup-{stamp}{suffix}"


def backup_sqlite(source_db: Path, backup_path: Path, *, gzip_enabled: bool) -> Path:
    output_tmp = backup_path.with_suffix(".tmp")
    if output_tmp.exists():
        output_tmp.unlink()
    with sqlite3.connect(source_db) as src_conn:
        with sqlite3.connect(output_tmp) as dst_conn:
            src_conn.backup(dst_conn)

    if gzip_enabled:
        with output_tmp.open("rb") as src, gzip.open(backup_path, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst)
        output_tmp.unlink(missing_ok=True)
    else:
        output_tmp.replace(backup_path)
    return backup_path


def cleanup_old_backups(output_dir: Path, keep: int) -> None:
    if keep <= 0:
        return
    backups = sorted(
        [p for p in output_dir.glob("*-backup-*.sqlite3*") if p.is_file()],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for stale in backups[keep:]:
        stale.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Create SQLite backup snapshot for production restore.")
    parser.add_argument("--db-path", default=Config.DATA_FILE, help="Path to source SQLite DB")
    parser.add_argument("--output-dir", default=Config.BACKUP_DIR, help="Directory for backup files")
    parser.add_argument("--keep", type=int, default=Config.BACKUP_KEEP, help="How many latest backups to keep")
    parser.add_argument("--gzip", action="store_true", help="Compress backup with gzip")
    args = parser.parse_args()

    source_db = Path(args.db_path).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if not source_db.exists():
        raise SystemExit(f"Source DB not found: {source_db}")

    backup_path = build_backup_path(output_dir, source_db, gzip_enabled=bool(args.gzip))
    created = backup_sqlite(source_db, backup_path, gzip_enabled=bool(args.gzip))
    cleanup_old_backups(output_dir, keep=max(0, int(args.keep)))
    print(str(created))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
