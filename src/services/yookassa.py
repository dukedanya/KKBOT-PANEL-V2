import base64
import ipaddress
import json
import logging
import uuid
from typing import Any, Dict, Optional

import aiohttp

from config import Config

logger = logging.getLogger(__name__)
YOOKASSA_API_BASE = "https://api.yookassa.ru/v3"
SUCCESS_STATUSES = {"succeeded"}
FAILED_STATUSES = {"canceled", "cancelled"}
WAITING_STATUSES = {"pending", "waiting_for_capture"}
_ALLOWED_NETWORKS = [
    ipaddress.ip_network("185.71.76.0/27"),
    ipaddress.ip_network("185.71.77.0/27"),
    ipaddress.ip_network("77.75.153.0/25"),
    ipaddress.ip_network("77.75.156.11/32"),
    ipaddress.ip_network("77.75.156.35/32"),
    ipaddress.ip_network("77.75.154.128/25"),
    ipaddress.ip_network("2a02:5180::/32"),
]


class YooKassaAPI:
    provider_name = "yookassa"

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_error_message: str = ""

    async def _get_session(self) -> aiohttp.ClientSession:
        if not self.session or self.session.closed:
            creds = base64.b64encode(
                f"{Config.YOOKASSA_SHOP_ID}:{Config.YOOKASSA_SECRET_KEY}".encode()
            ).decode()
            timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
            self.session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Basic {creds}",
                    "Content-Type": "application/json",
                },
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
            logger.error("YooKassa non-json response status=%s body=%s error=%s", resp.status, raw_text[:1000], exc)
            return None

    @staticmethod
    def _normalize_payment(payment: Dict[str, Any]) -> Dict[str, Any]:
        confirmation = payment.get("confirmation") or {}
        pay_url = confirmation.get("confirmation_url") or ""
        normalized = dict(payment)
        normalized.setdefault("payment_qr_urls", {})
        normalized["payment_qr_urls"] = {
            "desktop": pay_url,
            "android": pay_url,
            "ios": pay_url,
        }
        return normalized

    @staticmethod
    def _receipt_email(user_id: int) -> str:
        domain = (Config.PANEL_EMAIL_DOMAIN or "").strip().lower()
        if "." not in domain:
            domain = "example.com"
        return f"user_{user_id}@{domain}"

    @classmethod
    def _build_receipt(
        cls,
        *,
        amount: float,
        user_id: int,
        description: str,
    ) -> Dict[str, Any]:
        item_description = (description or "Оплата подписки").strip()[:128]
        value = f"{amount:.2f}"
        return {
            "customer": {
                "email": cls._receipt_email(user_id),
            },
            "items": [
                {
                    "description": item_description,
                    "quantity": "1.00",
                    "amount": {"value": value, "currency": "RUB"},
                    "vat_code": 1,
                    "payment_mode": "full_payment",
                    "payment_subject": "service",
                }
            ],
        }

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
        self.last_error_message = ""
        payload: Dict[str, Any] = {
            "amount": {"value": f"{amount:.2f}", "currency": "RUB"},
            "capture": True,
            "confirmation": {
                "type": "redirect",
                "return_url": success_url or Config.YOOKASSA_RETURN_URL or Config.TG_CHANNEL,
            },
            "description": description,
            "metadata": {
                "user_id": str(user_id),
                "plan_id": plan_id,
                "client_payment_id": client_payment_id,
            },
            "receipt": self._build_receipt(amount=amount, user_id=user_id, description=description),
        }
        headers = {"Idempotence-Key": client_payment_id}
        try:
            async with session.post(f"{YOOKASSA_API_BASE}/payments", json=payload, headers=headers) as resp:
                data = await self._read_json_response(resp)
                if resp.status in (200, 201) and data and data.get("id"):
                    return self._normalize_payment(data)
                if isinstance(data, dict):
                    self.last_error_message = str(
                        data.get("description") or data.get("code") or data.get("type") or ""
                    ).strip()
                logger.error("YooKassa create_payment status=%s response=%s", resp.status, data)
        except aiohttp.ClientError as e:
            self.last_error_message = f"Ошибка сети YooKassa: {e}"
            logger.error("YooKassa create_payment network error: %s", e)
        except Exception as e:
            self.last_error_message = f"Ошибка YooKassa: {e}"
            logger.error("YooKassa create_payment: %s", e)
        return None

    async def get_payment(self, payment_id: str) -> Optional[Dict[str, Any]]:
        session = await self._get_session()
        try:
            async with session.get(f"{YOOKASSA_API_BASE}/payments/{payment_id}") as resp:
                data = await self._read_json_response(resp)
                if resp.status == 200 and data and data.get("id"):
                    return self._normalize_payment(data)
                logger.warning("YooKassa get_payment status=%s payment_id=%s response=%s", resp.status, payment_id, data)
        except aiohttp.ClientError as e:
            logger.error("YooKassa get_payment network error payment_id=%s: %s", payment_id, e)
        except Exception as e:
            logger.error("YooKassa get_payment payment_id=%s: %s", payment_id, e)
        return None

    @staticmethod
    def extract_status(payment_data: Optional[Dict[str, Any]]) -> str:
        if not payment_data:
            return ""
        status = payment_data.get("status") or payment_data.get("state") or ""
        return str(status).strip().lower()

    @classmethod
    def is_success_status(cls, payment_data: Optional[Dict[str, Any]]) -> bool:
        return cls.extract_status(payment_data) in SUCCESS_STATUSES

    @classmethod
    def is_failed_status(cls, payment_data: Optional[Dict[str, Any]]) -> bool:
        return cls.extract_status(payment_data) in FAILED_STATUSES

    @classmethod
    def is_waiting_status(cls, payment_data: Optional[Dict[str, Any]]) -> bool:
        return cls.extract_status(payment_data) in WAITING_STATUSES

    @staticmethod
    def get_checkout_url(payment_data: Optional[Dict[str, Any]]) -> str:
        if not payment_data:
            return ""
        qr_urls = payment_data.get("payment_qr_urls") or {}
        return qr_urls.get("desktop") or qr_urls.get("android") or qr_urls.get("ios") or ""

    @staticmethod
    def is_allowed_notification_ip(ip_value: str) -> bool:
        if not ip_value:
            return False
        try:
            ip_obj = ipaddress.ip_address(ip_value)
        except ValueError:
            return False
        return any(ip_obj in network for network in _ALLOWED_NETWORKS)

    @classmethod
    async def verify_notification(cls, request_remote: str, object_id: str) -> bool:
        if Config.YOOKASSA_ENFORCE_IP_CHECK and not cls.is_allowed_notification_ip(request_remote):
            logger.warning("YooKassa notification denied by IP: %s", request_remote)
            return False
        if not object_id:
            return False
        api = cls()
        try:
            payment = await api.get_payment(object_id)
            return bool(payment and payment.get("id") == object_id)
        finally:
            await api.close()

    @staticmethod
    def build_idempotence_key(prefix: str = "yookassa") -> str:
        return f"{prefix}-{uuid.uuid4()}"

    async def create_refund(
        self,
        *,
        payment_id: str,
        amount: Optional[float] = None,
        reason: str = "",
        idempotence_key: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        session = await self._get_session()
        payload: Dict[str, Any] = {"payment_id": payment_id}
        if amount is not None:
            payload["amount"] = {"value": f"{float(amount):.2f}", "currency": "RUB"}
        if reason:
            payload["description"] = reason[:128]
        headers = {"Idempotence-Key": idempotence_key or self.build_idempotence_key("refund")}
        try:
            async with session.post(f"{YOOKASSA_API_BASE}/refunds", json=payload, headers=headers) as resp:
                data = await self._read_json_response(resp)
                if resp.status in (200, 201) and data and data.get("id"):
                    return data
                logger.error("YooKassa create_refund status=%s payment_id=%s response=%s", resp.status, payment_id, data)
        except aiohttp.ClientError as e:
            logger.error("YooKassa create_refund network error payment_id=%s: %s", payment_id, e)
        except Exception as e:
            logger.error("YooKassa create_refund payment_id=%s: %s", payment_id, e)
        return None

    async def get_refund(self, refund_id: str) -> Optional[Dict[str, Any]]:
        session = await self._get_session()
        try:
            async with session.get(f"{YOOKASSA_API_BASE}/refunds/{refund_id}") as resp:
                data = await self._read_json_response(resp)
                if resp.status == 200 and data and data.get("id"):
                    return data
                logger.warning("YooKassa get_refund status=%s refund_id=%s response=%s", resp.status, refund_id, data)
        except aiohttp.ClientError as e:
            logger.error("YooKassa get_refund network error refund_id=%s: %s", refund_id, e)
        except Exception as e:
            logger.error("YooKassa get_refund refund_id=%s: %s", refund_id, e)
        return None

    async def cancel_payment(self, payment_id: str, *, idempotence_key: Optional[str] = None) -> Optional[Dict[str, Any]]:
        session = await self._get_session()
        headers = {"Idempotence-Key": idempotence_key or self.build_idempotence_key("cancel")}
        try:
            async with session.post(f"{YOOKASSA_API_BASE}/payments/{payment_id}/cancel", json={}, headers=headers) as resp:
                data = await self._read_json_response(resp)
                if resp.status == 200 and data and data.get("id"):
                    return self._normalize_payment(data)
                logger.error("YooKassa cancel_payment status=%s payment_id=%s response=%s", resp.status, payment_id, data)
        except aiohttp.ClientError as e:
            logger.error("YooKassa cancel_payment network error payment_id=%s: %s", payment_id, e)
        except Exception as e:
            logger.error("YooKassa cancel_payment payment_id=%s: %s", payment_id, e)
        return None
