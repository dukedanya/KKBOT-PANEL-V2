import asyncio
import base64
import binascii
import logging
from pathlib import Path
from typing import Iterable

import aiohttp
import boto3

from config import Config

logger = logging.getLogger(__name__)


def _source_urls() -> list[str]:
    return [
        item.strip()
        for item in (Config.CIDR_OBJECT_STORAGE_SOURCE_URLS or "").split(",")
        if item.strip()
    ]


def _looks_like_base64_payload(text: str) -> bool:
    compact = "".join((text or "").split())
    if not compact or "://" in compact or len(compact) % 4 != 0:
        return False
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=")
    return all(ch in allowed for ch in compact)


def _decode_payload_if_needed(text: str) -> str:
    if not _looks_like_base64_payload(text):
        return text
    compact = "".join(text.split())
    try:
        decoded = base64.b64decode(compact, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError):
        return text
    return decoded if "vless://" in decoded else text


def _extract_config_lines(text: str) -> list[str]:
    payload = _decode_payload_if_needed(text or "")
    lines: list[str] = []
    for raw_line in payload.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith(("vless://", "vmess://", "trojan://", "ss://", "ssr://")):
            lines.append(line)
    return lines


def merge_config_payloads(payloads: Iterable[str]) -> str:
    merged: list[str] = []
    seen: set[str] = set()
    for payload in payloads:
        for line in _extract_config_lines(payload):
            if line in seen:
                continue
            seen.add(line)
            merged.append(line)
    return ("\n".join(merged) + "\n") if merged else ""


async def _download_source(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.text()


def _upload_to_object_storage(payload: str) -> str:
    client = boto3.client(
        "s3",
        endpoint_url=Config.LTE_OBJECT_STORAGE_ENDPOINT,
        aws_access_key_id=Config.LTE_OBJECT_STORAGE_ACCESS_KEY_ID,
        aws_secret_access_key=Config.LTE_OBJECT_STORAGE_SECRET_ACCESS_KEY,
    )
    extra_args = {"ContentType": "text/plain; charset=utf-8"}
    if Config.LTE_OBJECT_STORAGE_PUBLIC_READ:
        extra_args["ACL"] = "public-read"
    client.put_object(
        Bucket=Config.LTE_OBJECT_STORAGE_BUCKET,
        Key=Config.LTE_OBJECT_STORAGE_OBJECT_NAME,
        Body=payload.encode("utf-8"),
        **extra_args,
    )
    return (
        f"{Config.LTE_OBJECT_STORAGE_ENDPOINT.rstrip('/')}/"
        f"{Config.LTE_OBJECT_STORAGE_BUCKET}/{Config.LTE_OBJECT_STORAGE_OBJECT_NAME}"
    )


async def sync_cidr_config_to_object_storage() -> dict:
    urls = _source_urls()
    if not urls:
        raise RuntimeError("CIDR_OBJECT_STORAGE_SOURCE_URLS is empty")
    required = {
        "LTE_OBJECT_STORAGE_BUCKET": Config.LTE_OBJECT_STORAGE_BUCKET,
        "LTE_OBJECT_STORAGE_ACCESS_KEY_ID": Config.LTE_OBJECT_STORAGE_ACCESS_KEY_ID,
        "LTE_OBJECT_STORAGE_SECRET_ACCESS_KEY": Config.LTE_OBJECT_STORAGE_SECRET_ACCESS_KEY,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError("Missing object storage settings: " + ", ".join(missing))

    timeout = aiohttp.ClientTimeout(total=60, connect=15, sock_read=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        payloads = await asyncio.gather(*[_download_source(session, url) for url in urls])

    merged_payload = merge_config_payloads(payloads)
    if not merged_payload.strip():
        raise RuntimeError("Merged CIDR payload is empty")

    local_path = Path(Config.CIDR_OBJECT_STORAGE_LOCAL_PATH)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(merged_payload, encoding="utf-8")

    object_url = await asyncio.to_thread(_upload_to_object_storage, merged_payload)
    line_count = len([line for line in merged_payload.splitlines() if line.strip()])
    logger.info(
        "CIDR config uploaded to object storage: lines=%s sources=%s url=%s",
        line_count,
        len(urls),
        object_url,
    )
    return {
        "ok": True,
        "lines": line_count,
        "sources": len(urls),
        "object_url": object_url,
        "local_path": str(local_path),
    }
