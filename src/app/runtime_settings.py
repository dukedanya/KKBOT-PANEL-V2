from __future__ import annotations

import logging
from typing import Any

from config import Config

logger = logging.getLogger(__name__)

STARS_MULTIPLIER_SETTING_KEY = "system:telegram_stars_price_multiplier"
RUNTIME_FLOAT_SETTINGS = {
    "system:ref_percent_level1": "REF_PERCENT_LEVEL1",
    "system:ref_percent_level2": "REF_PERCENT_LEVEL2",
    "system:ref_percent_level3": "REF_PERCENT_LEVEL3",
    "system:min_withdraw": "MIN_WITHDRAW",
    "system:ref_first_payment_discount_percent": "REF_FIRST_PAYMENT_DISCOUNT_PERCENT",
}
RUNTIME_INT_SETTINGS = {
    "system:ref_bonus_days": "REF_BONUS_DAYS",
    "system:referred_bonus_days": "REFERRED_BONUS_DAYS",
    "system:panel_target_inbound_count": "PANEL_TARGET_INBOUND_COUNT",
}
RUNTIME_STR_SETTINGS = {
    "system:panel_target_inbound_ids": "PANEL_TARGET_INBOUND_IDS",
}

REF_SETTING_KEYS = {
    "REF_BONUS_DAYS": "system:ref_bonus_days",
    "REF_PERCENT_LEVEL1": "system:ref_percent_level1",
    "REF_PERCENT_LEVEL2": "system:ref_percent_level2",
    "REF_PERCENT_LEVEL3": "system:ref_percent_level3",
    "MIN_WITHDRAW": "system:min_withdraw",
    "REF_FIRST_PAYMENT_DISCOUNT_PERCENT": "system:ref_first_payment_discount_percent",
    "REFERRED_BONUS_DAYS": "system:referred_bonus_days",
}


async def apply_runtime_settings(db: Any) -> None:
    if not hasattr(db, "get_setting"):
        return

    raw_stars_multiplier = await db.get_setting(
        STARS_MULTIPLIER_SETTING_KEY,
        str(Config.TELEGRAM_STARS_PRICE_MULTIPLIER),
    )
    try:
        Config.set_stars_price_multiplier(float(raw_stars_multiplier or Config.TELEGRAM_STARS_PRICE_MULTIPLIER))
    except (TypeError, ValueError):
        logger.warning("Invalid stored Telegram Stars multiplier: %s", raw_stars_multiplier)

    for setting_key, attr_name in RUNTIME_FLOAT_SETTINGS.items():
        raw_value = await db.get_setting(setting_key, str(getattr(Config, attr_name)))
        try:
            setattr(Config, attr_name, float(raw_value or getattr(Config, attr_name)))
        except (TypeError, ValueError):
            logger.warning("Invalid stored float setting %s=%s", setting_key, raw_value)

    for setting_key, attr_name in RUNTIME_INT_SETTINGS.items():
        raw_value = await db.get_setting(setting_key, str(getattr(Config, attr_name)))
        try:
            setattr(Config, attr_name, int(float(raw_value or getattr(Config, attr_name))))
        except (TypeError, ValueError):
            logger.warning("Invalid stored int setting %s=%s", setting_key, raw_value)

    for setting_key, attr_name in RUNTIME_STR_SETTINGS.items():
        raw_value = await db.get_setting(setting_key, str(getattr(Config, attr_name)))
        if raw_value:
            setattr(Config, attr_name, str(raw_value).strip())
