import json
import logging
import os
import time
import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.error import URLError
from urllib.request import urlopen

import aiohttp

from config import Config
from db import Database

logger = logging.getLogger(__name__)

_BYTES_IN_GB = 1073741824


@dataclass(slots=True)
class TotalTrafficSnapshot:
    found: bool
    fresh: bool
    source_path: str
    total_bytes: int = 0
    quota_bytes: int = 0
    remaining_bytes: int = 0
    mode: str = "unknown"
    grace_until: str = ""
    expired: bool = False
    over_limit: bool = False
    raw: Optional[Dict[str, Any]] = None

    @property
    def total_gb(self) -> float:
        return self.total_bytes / _BYTES_IN_GB

    @property
    def quota_gb(self) -> float:
        return self.quota_bytes / _BYTES_IN_GB

    @property
    def remaining_gb(self) -> float:
        return self.remaining_bytes / _BYTES_IN_GB


def _load_json_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)
    return data if isinstance(data, dict) else {}


def _load_json_url(url: str) -> Dict[str, Any]:
    with urlopen(url, timeout=5) as response:
        data = json.load(response)
    return data if isinstance(data, dict) else {}


def _is_state_fresh(path: str) -> bool:
    try:
        stat = os.stat(path)
    except OSError:
        return False
    age_sec = time.time() - stat.st_mtime
    return age_sec <= max(30, int(Config.TOTAL_TRAFFIC_STATE_MAX_AGE_SEC or 1800))


async def load_total_traffic_state() -> tuple[Optional[Dict[str, Any]], str, bool]:
    url = (Config.TOTAL_TRAFFIC_STATE_URL or "").strip()
    if url:
        try:
            payload = await asyncio.to_thread(_load_json_url, url)
            return payload, url, True
        except (OSError, URLError, TimeoutError, ValueError) as exc:
            logger.warning("load_total_traffic_state url failed: %s", exc)

    path = (Config.TOTAL_TRAFFIC_STATE_PATH or "").strip()
    if path and os.path.exists(path):
        try:
            payload = _load_json_file(path)
            return payload, path, _is_state_fresh(path)
        except Exception as exc:
            logger.error("load_total_traffic_state file failed: %s", exc)
    return None, (url or path), False


async def get_total_traffic_snapshot_for_user(user_id: int, db: Database) -> Optional[TotalTrafficSnapshot]:
    state, source, fresh = await load_total_traffic_state()
    if not state:
        return None

    try:
        from kkbot.services.subscriptions import panel_base_email

        base_email = await panel_base_email(user_id, db)
        users = state.get("users") or []
        if not isinstance(users, list):
            return None

        matched: Optional[Dict[str, Any]] = None
        for item in users:
            if not isinstance(item, dict):
                continue
            if str(item.get("email") or "").strip().lower() == base_email.strip().lower():
                matched = item
                break

        if not matched:
            return None

        total_bytes = int(matched.get("totalBytes") or 0)
        quota_bytes = max(0, int(matched.get("quotaBytes") or 0))
        remaining_bytes = max(0, quota_bytes - total_bytes) if quota_bytes > 0 else 0
        return TotalTrafficSnapshot(
            found=True,
            fresh=fresh,
            source_path=source,
            total_bytes=total_bytes,
            quota_bytes=quota_bytes,
            remaining_bytes=remaining_bytes,
            mode=str(matched.get("mode") or "unknown"),
            grace_until=str(matched.get("graceUntil") or ""),
            expired=bool(matched.get("expired")),
            over_limit=bool(matched.get("overLimit")),
            raw=matched,
        )
    except Exception as exc:
        logger.error("get_total_traffic_snapshot_for_user(%s): %s", user_id, exc)
        return None


async def check_lte_report_api_health() -> bool:
    url = (Config.LTE_REPORT_API_HEALTH_URL or "").strip()
    if not url:
        return False
    timeout = aiohttp.ClientTimeout(total=5)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                return response.status == 200
    except Exception as exc:
        logger.warning("check_lte_report_api_health failed: %s", exc)
        return False


def format_grace_until(value: str) -> str:
    clean = (value or "").strip()
    if not clean:
        return ""
    try:
        dt = datetime.fromisoformat(clean.replace("Z", "+00:00"))
        return dt.strftime("%d.%m.%Y %H:%M")
    except ValueError:
        return clean
