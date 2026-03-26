from aiogram import Bot
import logging
from typing import Any, Dict, Optional

from config import Config

logger = logging.getLogger(__name__)


class TelegramStarsAPI:
    provider_name = "telegram_stars"

    async def close(self):
        return None

    @staticmethod
    def _normalize_stars_amount(raw_amount: Any) -> int:
        try:
            amount = int(raw_amount)
        except (TypeError, ValueError):
            return 0
        return max(amount, 1)

    @classmethod
    def resolve_stars_amount(cls, *, amount_rub: float, plan: Optional[Dict[str, Any]] = None) -> int:
        if plan and plan.get("price_stars") is not None:
            return cls._normalize_stars_amount(plan.get("price_stars"))
        multiplier = float(getattr(Config, "TELEGRAM_STARS_PRICE_MULTIPLIER", 1.0) or 1.0)
        computed = round(float(amount_rub or 0) * multiplier)
        return cls._normalize_stars_amount(computed)

    @staticmethod
    def build_invoice_payload(*, payment_id: str, user_id: int, plan_id: str) -> str:
        return f"tgstars:{payment_id}:{user_id}:{plan_id}"

    @staticmethod
    def parse_invoice_payload(payload: str) -> Optional[Dict[str, Any]]:
        if not payload or not payload.startswith("tgstars:"):
            return None
        parts = payload.split(":", 3)
        if len(parts) != 4:
            return None
        _prefix, payment_id, user_id_raw, plan_id = parts
        try:
            user_id = int(user_id_raw)
        except ValueError:
            return None
        return {
            "payment_id": payment_id,
            "user_id": user_id,
            "plan_id": plan_id,
        }

    async def create_payment(
        self,
        amount: float,
        client_payment_id: str,
        user_id: int,
        plan_id: str,
        description: str = "Оплата подписки",
        success_url: Optional[str] = None,
        *,
        plan: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        stars_amount = self.resolve_stars_amount(amount_rub=amount, plan=plan)
        return {
            "id": client_payment_id,
            "invoice_payload": self.build_invoice_payload(payment_id=client_payment_id, user_id=user_id, plan_id=plan_id),
            "stars_amount": stars_amount,
            "title": (plan or {}).get("name") or description or "Подписка VPN",
            "description": description,
            "payment_qr_urls": {},
            "currency": "XTR",
        }

    async def get_payment(self, payment_id: str) -> Optional[Dict[str, Any]]:
        logger.info("Telegram Stars does not support remote get_payment for payment_id=%s", payment_id)
        return None

    @staticmethod
    def extract_status(payment_data: Optional[Dict[str, Any]]) -> str:
        if not payment_data:
            return ""
        return str(payment_data.get("status") or "").strip().lower()

    @classmethod
    def is_success_status(cls, payment_data: Optional[Dict[str, Any]]) -> bool:
        return cls.extract_status(payment_data) in {"paid", "succeeded", "successful", "success"}

    @classmethod
    def is_failed_status(cls, payment_data: Optional[Dict[str, Any]]) -> bool:
        return cls.extract_status(payment_data) in {"failed", "cancelled", "canceled", "rejected", "expired"}

    @classmethod
    def is_waiting_status(cls, payment_data: Optional[Dict[str, Any]]) -> bool:
        return cls.extract_status(payment_data) in {"pending", "processing", "invoice_sent"}

    @staticmethod
    def get_checkout_url(payment_data: Optional[Dict[str, Any]]) -> str:
        return ""

    async def refund_payment(self, *, bot: Bot, user_id: int, telegram_payment_charge_id: str) -> bool:
        try:
            return bool(await bot.refund_star_payment(user_id=user_id, telegram_payment_charge_id=telegram_payment_charge_id))
        except Exception as e:
            logger.error("Telegram Stars refund_payment user_id=%s charge_id=%s: %s", user_id, telegram_payment_charge_id, e)
            return False
