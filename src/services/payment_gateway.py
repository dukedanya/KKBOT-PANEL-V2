from __future__ import annotations

from typing import Protocol, runtime_checkable, Optional, Dict, Any, List

from config import Config
from services.itpay import ItpayAPI
from services.telegram_stars import TelegramStarsAPI
from services.yookassa import YooKassaAPI


@runtime_checkable
class PaymentGateway(Protocol):
    provider_name: str

    async def close(self) -> None: ...
    async def create_payment(self, amount: float, client_payment_id: str, user_id: int, plan_id: str, description: str = "", success_url: Optional[str] = None, *, plan: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]: ...
    async def get_payment(self, payment_id: str) -> Optional[Dict[str, Any]]: ...
    def extract_status(self, payment_data: Optional[Dict[str, Any]]) -> str: ...
    def is_success_status(self, payment_data: Optional[Dict[str, Any]]) -> bool: ...
    def is_failed_status(self, payment_data: Optional[Dict[str, Any]]) -> bool: ...
    def is_waiting_status(self, payment_data: Optional[Dict[str, Any]]) -> bool: ...
    def get_checkout_url(self, payment_data: Optional[Dict[str, Any]]) -> str: ...


PROVIDER_META = {
    "itpay": {
        "label": "ITPAY",
        "button_label": "💳 ITPAY",
        "description": "💳 ITPAY",
    },
    "yookassa": {
        "label": "ЮKassa",
        "button_label": "💳 ЮKassa",
        "description": "💳 ЮKassa",
    },
    "telegram_stars": {
        "label": "Telegram Stars",
        "button_label": "⭐ Telegram Stars",
        "description": "⭐ Telegram Stars",
    },
}

PROVIDER_LABELS = {key: value["label"] for key, value in PROVIDER_META.items()}



def is_provider_configured(provider: str) -> bool:
    provider = (provider or '').strip().lower()
    if provider == 'itpay':
        return bool(Config.ITPAY_PUBLIC_ID and Config.ITPAY_API_SECRET)
    if provider == 'yookassa':
        return bool(Config.YOOKASSA_SHOP_ID and Config.YOOKASSA_SECRET_KEY)
    if provider == 'telegram_stars':
        return bool(Config.BOT_TOKEN)
    return False


def get_provider_label(provider: str) -> str:
    return PROVIDER_LABELS.get((provider or '').strip().lower(), provider or 'payment')


def get_provider_button_label(provider: str) -> str:
    meta = PROVIDER_META.get((provider or '').strip().lower(), {})
    return meta.get("button_label") or get_provider_label(provider)


def get_provider_description(provider: str) -> str:
    meta = PROVIDER_META.get((provider or '').strip().lower(), {})
    return meta.get("description") or get_provider_label(provider)


def get_enabled_payment_providers() -> List[str]:
    raw = (getattr(Config, "PAYMENT_PROVIDERS", "") or "").strip()
    if raw:
        providers = []
        for item in raw.split(","):
            name = (item or "").strip().lower()
            if name in PROVIDER_META and name not in providers:
                providers.append(name)
        providers = [provider for provider in providers if is_provider_configured(provider)]
        if providers:
            return providers

    provider = (Config.PAYMENT_PROVIDER or "itpay").strip().lower()
    if provider in PROVIDER_META and is_provider_configured(provider):
        return [provider]

    fallback = [name for name in ("itpay", "yookassa", "telegram_stars") if is_provider_configured(name)]
    return fallback


def build_payment_gateway(provider: Optional[str] = None):
    provider_name = (provider or Config.PAYMENT_PROVIDER or 'itpay').strip().lower()
    if provider_name == 'yookassa':
        return YooKassaAPI()
    if provider_name == 'telegram_stars':
        return TelegramStarsAPI()
    return ItpayAPI()
