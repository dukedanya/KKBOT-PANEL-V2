import base64
import hashlib
import hmac
import json
import logging
from typing import Any, Dict, Optional
from urllib.parse import urlsplit

import aiohttp

from config import Config

logger = logging.getLogger(__name__)
ITPAY_API_BASE = "https://api.gw.itpay.ru"
SUCCESS_STATUSES = {"completed", "paid", "success", "succeeded", "payment.completed", "payment.pay"}
FAILED_STATUSES = {"canceled", "cancelled", "failed", "expired", "rejected", "declined"}


class ItpayAPI:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if not self.session or self.session.closed:
            creds = base64.b64encode(
                f"{Config.ITPAY_PUBLIC_ID}:{Config.ITPAY_API_SECRET}".encode()
            ).decode()
            timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
            self.session = aiohttp.ClientSession(
                headers={"Authorization": f"Basic {creds}", "Content-Type": "application/json"},
                connector=aiohttp.TCPConnector(ssl=Config.VERIFY_SSL),
                timeout=timeout,
            )
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    @staticmethod
    async def _read_json_response(resp: aiohttp.ClientResponse) -> Optional[Dict[str, Any]]:
        try:
            return await resp.json(content_type=None)
        except (aiohttp.ContentTypeError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            try:
                raw_text = await resp.text()
            except (aiohttp.ClientError, UnicodeDecodeError) as text_exc:
                raw_text = f"<unavailable: {text_exc}>"
            logger.error("ITPAY non-json response status=%s body=%s error=%s", resp.status, raw_text[:1000], exc)
            return None


    @staticmethod
    def _normalize_public_base(url: str) -> str:
        url = (url or '').strip()
        if not url:
            return ''
        parts = urlsplit(url)
        if not parts.scheme or not parts.netloc:
            return ''
        return f"{parts.scheme}://{parts.netloc}"

    @classmethod
    def _resolve_public_base_url(cls) -> str:
        site = cls._normalize_public_base(Config.SITE_URL)
        webhook_host = cls._normalize_public_base(Config.WEBHOOK_HOST)
        if site and 't.me' not in site:
            return site
        if webhook_host:
            return webhook_host
        if site:
            return site
        return (Config.ITPAY_PUBLIC_BASE_FALLBACK or "http://127.0.0.1").strip() or "http://127.0.0.1"

    async def create_payment(
        self,
        amount: float,
        client_payment_id: str,
        user_id: int,
        plan_id: str,
        description: str = "Оплата подписки",
        success_url: Optional[str] = None,
        **kwargs,
    ) -> Optional[Dict[str, Any]]:
        session = await self._get_session()
        webhook_base = self._resolve_public_base_url()
        payload: Dict[str, Any] = {
            "amount": f"{amount:.2f}",
            "client_payment_id": client_payment_id,
            "description": description,
            "method": "sbp",
            "webhook_url": f"{webhook_base}{Config.ITPAY_WEBHOOK_PATH}",
            "metadata": {
                "user_id": str(user_id),
                "plan_id": plan_id,
            },
        }
        if success_url:
            payload["success_url"] = success_url
        try:
            async with session.post(f"{ITPAY_API_BASE}/v1/payments", json=payload) as resp:
                data = await self._read_json_response(resp)
                if resp.status == 200 and data and data.get("data"):
                    return data["data"]
                logger.error("ITPAY create_payment status=%s response=%s", resp.status, data)
        except aiohttp.ClientError as e:
            logger.error("ITPAY create_payment network error: %s", e)
        except Exception as e:
            logger.error("ITPAY create_payment: %s", e)
        return None

    async def get_payment(self, payment_id: str) -> Optional[Dict[str, Any]]:
        session = await self._get_session()
        try:
            async with session.get(f"{ITPAY_API_BASE}/v1/payments/{payment_id}") as resp:
                data = await self._read_json_response(resp)
                if resp.status == 200 and data and data.get("data"):
                    return data["data"]
                logger.warning("ITPAY get_payment status=%s payment_id=%s response=%s", resp.status, payment_id, data)
        except aiohttp.ClientError as e:
            logger.error("ITPAY get_payment network error payment_id=%s: %s", payment_id, e)
        except Exception as e:
            logger.error("ITPAY get_payment payment_id=%s: %s", payment_id, e)
        return None

    @staticmethod
    def extract_status(payment_data: Optional[Dict[str, Any]]) -> str:
        if not payment_data:
            return ""
        status = payment_data.get("status") or payment_data.get("state") or payment_data.get("payment_status") or ""
        return str(status).strip().lower()

    @classmethod
    def is_success_status(cls, payment_data: Optional[Dict[str, Any]]) -> bool:
        return cls.extract_status(payment_data) in SUCCESS_STATUSES

    @classmethod
    def is_failed_status(cls, payment_data: Optional[Dict[str, Any]]) -> bool:
        return cls.extract_status(payment_data) in FAILED_STATUSES

    @staticmethod
    def verify_webhook_signature(api_secret: str, raw_body: bytes, signature_header: str) -> bool:
        try:
            parts = dict(p.split("=", 1) for p in signature_header.split(",") if "=" in p)
            timestamp, v1 = parts.get("t", ""), parts.get("v1", "")
            if not timestamp or not v1:
                logger.warning("ITPAY signature verify: missing timestamp or v1 header")
                return False
            body_json = json.loads(raw_body.decode("utf-8"))
            data_str = json.dumps(body_json.get("data", {}), separators=(",", ":"), ensure_ascii=False)
            signed_payload = f"{timestamp}.{data_str}"
            expected = hmac.new(api_secret.encode(), signed_payload.encode(), hashlib.sha256).hexdigest()
            return hmac.compare_digest(expected, v1)
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.error("ITPAY signature verify parse error: %s", exc)
            return False
