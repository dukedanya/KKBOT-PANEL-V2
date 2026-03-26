from __future__ import annotations

from pathlib import Path

from kkbot.db.postgres import PostgresDatabase


async def apply_postgres_migrations(db: PostgresDatabase, migrations_dir: Path) -> list[str]:
    applied: list[str] = []
    for path in sorted(migrations_dir.glob("*.sql")):
        await db.execute_script(path)
        applied.append(path.name)
    return applied
