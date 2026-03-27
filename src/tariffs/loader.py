import json
import os
from typing import Any, Dict, List, Optional

from services.telegram_stars import TelegramStarsAPI
from services.payment_gateway import get_enabled_payment_providers

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARIFFS_PATH = os.path.join(BASE_DIR, "data", "tarifs.json")
TARIFFS_ALL: List[Dict[str, Any]] = []
TARIFFS_ACTIVE: List[Dict[str, Any]] = []
TARIFFS_BY_ID: Dict[str, Dict[str, Any]] = {}


def load_tariffs() -> None:
    global TARIFFS_ALL, TARIFFS_ACTIVE, TARIFFS_BY_ID

    if not os.path.exists(TARIFFS_PATH):
        raise FileNotFoundError(f"Файл тарифов не найден: {TARIFFS_PATH}")

    with open(TARIFFS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    plans = data.get("plans") or []
    if not isinstance(plans, list):
        raise ValueError("tarifs.json должен содержать список plans")

    for plan in plans:
        if "active" not in plan:
            plan["active"] = True
        if "price_stars" not in plan:
            plan["price_stars"] = None

    TARIFFS_ALL = plans
    TARIFFS_ACTIVE = [p for p in plans if p.get("active", True)]
    TARIFFS_ACTIVE.sort(key=lambda p: (p.get("sort", 9999), p.get("price_rub", 0)))
    TARIFFS_BY_ID = {p.get("id"): p for p in plans if p.get("id")}


def get_all_active() -> List[Dict[str, Any]]:
    return list(TARIFFS_ACTIVE)


def get_by_id(plan_id: str) -> Optional[Dict[str, Any]]:
    return TARIFFS_BY_ID.get(plan_id)


def is_trial_plan(plan: Optional[Dict[str, Any]]) -> bool:
    if not plan:
        return False
    return plan.get("id") == "trial" or plan.get("price_rub", 0) == 0


def get_minimal_by_price() -> Optional[Dict[str, Any]]:
    if not TARIFFS_ACTIVE:
        return None
    eligible = [p for p in TARIFFS_ACTIVE if not is_trial_plan(p)]
    if not eligible:
        return None
    return min(eligible, key=lambda p: (p.get("price_rub", 0), p.get("ip_limit", 0)))


def format_traffic(traffic_gb: Any) -> str:
    return "Безлимит"


def format_duration(days: int) -> str:
    value = int(days)
    mod10 = value % 10
    mod100 = value % 100
    if mod10 == 1 and mod100 != 11:
        unit = "день"
    elif 2 <= mod10 <= 4 and not 12 <= mod100 <= 14:
        unit = "дня"
    else:
        unit = "дней"
    return f"{value} {unit}"


def format_price(plan: Dict[str, Any]) -> str:
    price = float(plan.get("price_rub", 0) or 0)
    old_price = float(plan.get("old_price_rub", 0) or 0)
    duration = int(plan.get("duration_days", 30) or 30)
    if price == 0:
        return f"Бесплатно на {duration} дн."
    if old_price > price > 0:
        discount_percent = int(round((old_price - price) * 100 / old_price))
        old_value = int(old_price) if old_price.is_integer() else old_price
        new_value = int(price) if price.is_integer() else price
        return f"<s>{old_value} ₽</s> {new_value} ₽ (-{discount_percent}%)"
    value = int(price) if price.is_integer() else price
    return f"{value} ₽"


def format_stars_price(plan: Dict[str, Any]) -> str:
    stars_amount = TelegramStarsAPI.resolve_stars_amount(amount_rub=plan.get("price_rub", 0), plan=plan)
    return f"{stars_amount} ⭐"


def has_stars_provider_enabled() -> bool:
    return "telegram_stars" in get_enabled_payment_providers()


def _format_plan_description(plan: Dict[str, Any]) -> str:
    description = str(plan.get("description") or "").strip()
    if not description:
        return ""
    return f"<blockquote>{description}</blockquote>"


def _format_plan_quote(plan: Dict[str, Any], *, include_duration: bool = True) -> str:
    parts = [
        f"💰 {format_price(plan)}",
        f"📱 до {plan.get('ip_limit', 0)} устройств",
        "∞ Безлимитный трафик",
    ]
    if include_duration:
        parts.append(f"⏱ {format_duration(int(plan.get('duration_days', 30) or 30))}")
    description = str(plan.get("description") or "").strip()
    if description:
        parts.append(description)
    return "<blockquote>" + "\n".join(parts) + "</blockquote>"


def build_tariffs_text(plans: Optional[List[Dict[str, Any]]] = None) -> str:
    plans = plans if plans is not None else get_all_active()
    if not plans:
        return "🔒 <b>Тарифы VPN</b>\n\nТарифы временно недоступны."

    lines = ["🔒 <b>Тарифы VPN</b>", ""]
    for idx, plan in enumerate(plans, 1):
        lines.append(f"{idx}. <b>{plan.get('name', plan.get('id'))}</b>")
        lines.append(f"   {_format_plan_quote(plan, include_duration=True)}")
        lines.append("")
    return "\n".join(lines).strip()


def build_buy_text(plans: Optional[List[Dict[str, Any]]] = None) -> str:
    plans = plans if plans is not None else get_all_active()
    if not plans:
        return "💳 <b>Купить подписку VPN</b>\n\nТарифы временно недоступны."

    lines = ["💳 <b>Выберите тариф</b>", ""]
    for idx, plan in enumerate(plans, 1):
        lines.append(f"{idx}. <b>{plan.get('name', plan.get('id'))}</b>")
        lines.append(f"   {_format_plan_quote(plan, include_duration=True)}")
        lines.append("")
    return "\n".join(lines).strip()
