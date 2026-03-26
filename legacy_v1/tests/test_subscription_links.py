import unittest
from unittest.mock import patch

from utils.subscription_links import (
    build_merged_subscription_url,
    build_primary_subscription_url,
    build_sidr_subscription_url,
    render_connection_info,
)


class SubscriptionLinksTests(unittest.TestCase):
    def test_build_sidr_subscription_url_uses_template_placeholders(self):
        with patch("utils.subscription_links.Config.SIDR_SUBSCRIPTION_TEMPLATE", "sidr://import/{url_quoted}#{name_quoted}"), \
             patch("utils.subscription_links.Config.SIDR_SUBSCRIPTION_NAME", "Kakoito VPN"):
            result = build_sidr_subscription_url(
                "https://panel.example/sub/abc?x=1&y=2",
                user_id=42,
                plan_name="Premium",
            )

        self.assertEqual(
            result,
            "sidr://import/https%3A%2F%2Fpanel.example%2Fsub%2Fabc%3Fx%3D1%26y%3D2#Kakoito%20VPN%20-%20Premium",
        )

    def test_render_connection_info_contains_base_url_even_without_sidr_template(self):
        with patch("utils.subscription_links.Config.SIDR_SUBSCRIPTION_TEMPLATE", ""):
            result = render_connection_info("https://panel.example/sub/abc", user_id=7)

        self.assertIn("https://panel.example/sub/abc", result)
        self.assertNotIn("Sidr:", result)

    def test_build_merged_subscription_url_includes_base_url_when_enabled(self):
        with patch("utils.subscription_links.Config.MERGED_SUBSCRIPTION_API_BASE", "http://77.239.115.146:8787"), \
             patch("utils.subscription_links.Config.MERGED_SUBSCRIPTION_INCLUDE_BASE_URL", True), \
             patch("utils.subscription_links.Config.MERGED_SUBSCRIPTION_FORMAT", "base64"):
            result = build_merged_subscription_url(
                "uuid-123",
                base_subscription_url="https://panel.example/sub/user7",
            )

        self.assertEqual(
            result,
            "http://77.239.115.146:8787/sub/uuid-123?base_url=https%3A%2F%2Fpanel.example%2Fsub%2Fuser7",
        )

    def test_build_primary_subscription_url_prefers_merged_url(self):
        with patch("utils.subscription_links.Config.MERGED_SUBSCRIPTION_API_BASE", "http://77.239.115.146:8787"), \
             patch("utils.subscription_links.Config.SUB_PANEL_BASE", "https://panel.example/sub/"), \
             patch("utils.subscription_links.Config.MERGED_SUBSCRIPTION_INCLUDE_BASE_URL", True):
            result = build_primary_subscription_url(client_uuid="uuid-123", sub_id="user7")

        self.assertEqual(
            result,
            "http://77.239.115.146:8787/sub/uuid-123?base_url=https%3A%2F%2Fpanel.example%2Fsub%2Fuser7",
        )
