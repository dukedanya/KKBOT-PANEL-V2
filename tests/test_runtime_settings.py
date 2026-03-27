import unittest
from unittest.mock import patch

from app.runtime_settings import apply_runtime_settings
from config import Config


class _FakeDb:
    def __init__(self, values):
        self.values = values

    async def get_setting(self, key: str, default: str | None = None):
        return self.values.get(key, default)


class RuntimeSettingsTests(unittest.IsolatedAsyncioTestCase):
    async def test_apply_runtime_settings_overrides_mutable_values_from_db(self) -> None:
        fake_db = _FakeDb(
            {
                "system:telegram_stars_price_multiplier": "1.75",
                "system:ref_bonus_days": "11",
                "system:referred_bonus_days": "9",
                "system:ref_percent_level1": "31",
                "system:ref_percent_level2": "12",
                "system:ref_percent_level3": "6",
                "system:min_withdraw": "555",
                "system:ref_first_payment_discount_percent": "18",
                "system:panel_target_inbound_count": "4",
                "system:panel_target_inbound_ids": "1,2,3,4",
            }
        )

        with (
            patch.object(Config, "TELEGRAM_STARS_PRICE_MULTIPLIER", 1.0),
            patch.object(Config, "REF_BONUS_DAYS", 7),
            patch.object(Config, "REFERRED_BONUS_DAYS", 5),
            patch.object(Config, "REF_PERCENT_LEVEL1", 25.0),
            patch.object(Config, "REF_PERCENT_LEVEL2", 10.0),
            patch.object(Config, "REF_PERCENT_LEVEL3", 5.0),
            patch.object(Config, "MIN_WITHDRAW", 300.0),
            patch.object(Config, "REF_FIRST_PAYMENT_DISCOUNT_PERCENT", 15.0),
            patch.object(Config, "PANEL_TARGET_INBOUND_COUNT", 0),
            patch.object(Config, "PANEL_TARGET_INBOUND_IDS", "1,2,3"),
        ):
            await apply_runtime_settings(fake_db)
            self.assertEqual(Config.TELEGRAM_STARS_PRICE_MULTIPLIER, 1.75)
            self.assertEqual(Config.REF_BONUS_DAYS, 11)
            self.assertEqual(Config.REFERRED_BONUS_DAYS, 9)
            self.assertEqual(Config.REF_PERCENT_LEVEL1, 31.0)
            self.assertEqual(Config.REF_PERCENT_LEVEL2, 12.0)
            self.assertEqual(Config.REF_PERCENT_LEVEL3, 6.0)
            self.assertEqual(Config.MIN_WITHDRAW, 555.0)
            self.assertEqual(Config.REF_FIRST_PAYMENT_DISCOUNT_PERCENT, 18.0)
            self.assertEqual(Config.PANEL_TARGET_INBOUND_COUNT, 4)
            self.assertEqual(Config.PANEL_TARGET_INBOUND_IDS, "1,2,3,4")
