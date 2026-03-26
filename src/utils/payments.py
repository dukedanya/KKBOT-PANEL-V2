from typing import Any


def get_provider_payment_id(payment: dict[str, Any] | None) -> str:
    if not payment:
        return ""
    return str(payment.get("provider_payment_id") or payment.get("itpay_id") or "")
