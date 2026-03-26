import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


class ConfigValidationTests(unittest.TestCase):
    def test_str_to_bool_truthy_values(self):
        from config import str_to_bool
        self.assertTrue(str_to_bool("1"))
        self.assertTrue(str_to_bool("true"))
        self.assertTrue(str_to_bool("YES"))

    def test_validate_startup_reports_missing_required_values(self):
        from config import Config
        with patch.object(Config, "BOT_TOKEN", ""), \
             patch.object(Config, "ADMIN_USER_IDS", []), \
             patch.object(Config, "PANEL_BASE", ""), \
             patch.object(Config, "PANEL_LOGIN", ""), \
             patch.object(Config, "PANEL_PASSWORD", ""), \
             patch.object(Config, "ITPAY_PUBLIC_ID", ""), \
             patch.object(Config, "ITPAY_API_SECRET", ""):
            errors = Config.validate_startup()
        self.assertGreaterEqual(len(errors), 6)
        self.assertIn("BOT_TOKEN is required", errors)
        self.assertIn("ADMIN_USER_IDS must contain at least one Telegram user id", errors)

    def test_startup_summary_hides_secrets_but_has_runtime_flags(self):
        from config import Config
        summary = Config.startup_summary()
        self.assertIn("environment", summary)
        self.assertIn("jobs", summary)
        self.assertIn("limits", summary)
        self.assertNotIn("BOT_TOKEN", summary)
        self.assertNotIn("ITPAY_API_SECRET", summary)

    def test_settings_sections_return_typed_values(self):
        from config import Config, JobsSettings, LoggingSettings, RuntimeSettings

        runtime = Config.runtime_settings()
        logging_settings = Config.logging_settings()
        jobs = Config.jobs_settings()

        self.assertIsInstance(runtime, RuntimeSettings)
        self.assertIsInstance(logging_settings, LoggingSettings)
        self.assertIsInstance(jobs, JobsSettings)
        self.assertIn(runtime.app_mode, {"polling", "webhook"})
        self.assertIsInstance(jobs.enable_health_monitor_job, bool)


    def test_validate_startup_rejects_nonpositive_stars_multiplier(self):
        from config import Config
        with patch.object(Config, "PAYMENT_PROVIDER", "telegram_stars"), \
             patch.object(Config, "TELEGRAM_STARS_PRICE_MULTIPLIER", 0), \
             patch.object(Config, "BOT_TOKEN", "token"), \
             patch.object(Config, "ADMIN_USER_IDS", [1]), \
             patch.object(Config, "PANEL_BASE", "https://panel.example"), \
             patch.object(Config, "PANEL_LOGIN", "admin"), \
             patch.object(Config, "PANEL_PASSWORD", "secret"):
            errors = Config.validate_startup()
        self.assertIn("TELEGRAM_STARS_PRICE_MULTIPLIER must be greater than 0 for PAYMENT_PROVIDER=telegram_stars", errors)

    def test_sync_missing_env_variables_appends_only_missing_keys(self):
        from config import Config

        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            env_path = tmp_path / ".env"
            env_example = tmp_path / ".env.example"
            env_release = tmp_path / ".env.release.example"
            env_polling = tmp_path / ".env.polling.example"
            env_webhook = tmp_path / ".env.webhook.example"

            env_path.write_text("BOT_TOKEN=real_token\nADMIN_USER_IDS=1\n", encoding="utf-8")
            env_example.write_text("BOT_TOKEN=placeholder\nPANEL_BASE=https://panel.example\n", encoding="utf-8")
            env_release.write_text("LOG_LEVEL=INFO\n", encoding="utf-8")
            env_polling.write_text("APP_MODE=polling\n", encoding="utf-8")
            env_webhook.write_text("", encoding="utf-8")

            with patch("config.ENV_FILE_PATH", str(env_path)), patch("config.BASE_DIR", str(tmp_path)):
                added = Config.sync_missing_env_variables()

            updated = env_path.read_text(encoding="utf-8")
            self.assertEqual(added, ["PANEL_BASE", "LOG_LEVEL", "APP_MODE"])
            self.assertIn("BOT_TOKEN=real_token", updated)
            self.assertIn("PANEL_BASE=https://panel.example", updated)
            self.assertIn("LOG_LEVEL=INFO", updated)
            self.assertIn("APP_MODE=polling", updated)
            self.assertNotIn("BOT_TOKEN=placeholder", updated)

    def test_detect_duplicate_env_variables_returns_repeated_keys(self):
        from config import Config

        with TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text(
                "BOT_TOKEN=one\nADMIN_USER_IDS=1\nBOT_TOKEN=two\nLOG_LEVEL=INFO\nLOG_LEVEL=DEBUG\n",
                encoding="utf-8",
            )

            with patch("config.ENV_FILE_PATH", str(env_path)):
                duplicates = Config.detect_duplicate_env_variables()

        self.assertEqual(duplicates, ["BOT_TOKEN", "LOG_LEVEL"])


if __name__ == "__main__":
    unittest.main()
