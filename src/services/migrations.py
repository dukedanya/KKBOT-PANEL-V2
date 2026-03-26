import logging
import re
from pathlib import Path

from db import Database

logger = logging.getLogger(__name__)

MIGRATION_RE = re.compile(r"^(\d+)_.*\.sql$")


def list_migration_files(base_dir: str) -> list[tuple[int, str]]:
    migrations_dir = Path(base_dir) / "migrations"
    if not migrations_dir.exists():
        return []

    items: list[tuple[int, str]] = []
    for path in sorted(migrations_dir.glob("*.sql")):
        match = MIGRATION_RE.match(path.name)
        if not match:
            continue
        items.append((int(match.group(1)), path.name))
    return items


def latest_migration_version(base_dir: str) -> int:
    files = list_migration_files(base_dir)
    if not files:
        return 0
    return max(version for version, _ in files)


async def get_pending_migrations(db: Database, base_dir: str) -> list[tuple[int, str]]:
    files = list_migration_files(base_dir)
    if not files:
        return []
    applied = set(await db.get_applied_migration_versions())
    return [(version, name) for version, name in files if version not in applied]


async def apply_migrations(db: Database, base_dir: str) -> int:
    pending = await get_pending_migrations(db, base_dir)
    applied_now = 0
    migrations_dir = Path(base_dir) / "migrations"
    for version, name in pending:
        path = migrations_dir / name
        sql = path.read_text(encoding="utf-8")
        await db.executescript(sql)
        await db.record_migration(version, name)
        logger.info("Applied migration %s", name)
        applied_now += 1
    return applied_now
