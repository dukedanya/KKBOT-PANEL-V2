from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")


@dataclass(frozen=True, slots=True)
class Settings:
    bot_token: str
    admin_user_ids: tuple[int, ...]
    app_env: str
    log_level: str
    database_url: str
    database_min_pool: int
    database_max_pool: int
    legacy_sqlite_path: str
    auto_migrate_legacy: bool
    legacy_import_batch_size: int
    panel_base: str
    panel_login: str
    panel_password: str
    verify_ssl: bool
    webhook_enabled: bool
    webhook_host: str
    webhook_port: int


def load_settings() -> Settings:
    admin_ids = tuple(
        int(raw.strip())
        for raw in os.getenv("ADMIN_USER_IDS", "").split(",")
        if raw.strip()
    )
    return Settings(
        bot_token=os.getenv("BOT_TOKEN", "").strip(),
        admin_user_ids=admin_ids,
        app_env=os.getenv("APP_ENV", "production").strip() or "production",
        log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO",
        database_url=os.getenv("DATABASE_URL", "").strip(),
        database_min_pool=int(os.getenv("DATABASE_MIN_POOL", "1")),
        database_max_pool=int(os.getenv("DATABASE_MAX_POOL", "10")),
        legacy_sqlite_path=os.getenv("LEGACY_SQLITE_PATH", "").strip(),
        auto_migrate_legacy=_as_bool(os.getenv("AUTO_MIGRATE_LEGACY"), True),
        legacy_import_batch_size=int(os.getenv("LEGACY_IMPORT_BATCH_SIZE", "1000")),
        panel_base=os.getenv("PANEL_BASE", "").rstrip("/"),
        panel_login=os.getenv("PANEL_LOGIN", "").strip(),
        panel_password=os.getenv("PANEL_PASSWORD", "").strip(),
        verify_ssl=_as_bool(os.getenv("VERIFY_SSL"), True),
        webhook_enabled=_as_bool(os.getenv("WEBHOOK_ENABLED"), False),
        webhook_host=os.getenv("WEBHOOK_HOST", "").strip(),
        webhook_port=int(os.getenv("WEBHOOK_PORT", "8080")),
    )
