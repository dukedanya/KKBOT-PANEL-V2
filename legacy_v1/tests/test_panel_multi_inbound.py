import unittest
from unittest.mock import AsyncMock, patch

from services.panel import PanelAPI


class PanelMultiInboundTests(unittest.IsolatedAsyncioTestCase):
    async def test_target_inbounds_include_configured_disabled_slots(self):
        panel = PanelAPI()
        panel.get_inbounds = AsyncMock(return_value={
            "success": True,
            "obj": [
                {"id": 1, "enable": True, "protocol": "vless"},
                {"id": 2, "enable": False, "protocol": "vless"},
                {"id": 3, "enable": False, "protocol": "vless"},
            ],
        })

        with patch("services.panel.Config.PANEL_TARGET_INBOUND_IDS", "2,3"):
            result = await panel._get_target_inbounds()

        self.assertEqual([item["id"] for item in result], [2, 3])

    async def test_create_client_uses_same_uuid_for_selected_inbounds(self):
        panel = PanelAPI()
        panel.ensure_auth = AsyncMock()
        panel.get_inbounds = AsyncMock(return_value={
            "success": True,
            "obj": [
                {"id": 2, "enable": True, "protocol": "vless"},
                {"id": 3, "enable": True, "protocol": "vless"},
                {"id": 4, "enable": True, "protocol": "vless"},
                {"id": 7, "enable": True, "protocol": "vless"},
            ],
        })
        seen_payloads = []

        async def fake_request(method, url, **kwargs):
            seen_payloads.append(kwargs["json"])
            return 200, {"success": True}, ""

        panel._request_json_with_reauth = AsyncMock(side_effect=fake_request)

        with patch("services.panel.Config.PANEL_TARGET_INBOUND_IDS", "2,3,4"):
            client = await panel.create_client(
                email="1@vpnbot",
                limit_ip=2,
                total_gb=50,
                days=30,
            )

        self.assertIsNotNone(client)
        self.assertEqual(len(seen_payloads), 3)
        inbound_ids = [payload["id"] for payload in seen_payloads]
        self.assertEqual(inbound_ids, [2, 3, 4])
        self.assertIn('"email": "i2_1@vpnbot"', seen_payloads[0]["settings"])
        self.assertIn('"email": "i3_1@vpnbot"', seen_payloads[1]["settings"])
        self.assertIn('"email": "i4_1@vpnbot"', seen_payloads[2]["settings"])
        client_ids = []
        sub_ids = []
        for payload in seen_payloads:
            settings = payload["settings"]
            self.assertIn('"clients"', settings)
            client_fragment = settings.split('"id": "', 1)[1].split('"', 1)[0]
            sub_fragment = settings.split('"subId": "', 1)[1].split('"', 1)[0]
            client_ids.append(client_fragment)
            sub_ids.append(sub_fragment)
        self.assertEqual(len(set(client_ids)), 1)
        self.assertEqual(len(set(sub_ids)), 1)
        self.assertTrue(all("/panel/api/inbounds/update/" in call.args[1] for call in panel._request_json_with_reauth.await_args_list))

    async def test_upsert_client_updates_existing_inbound_without_new_identity(self):
        panel = PanelAPI()
        panel.ensure_auth = AsyncMock()
        panel.get_inbounds = AsyncMock(return_value={
            "success": True,
            "obj": [
                {"id": 2, "enable": True, "protocol": "vless"},
                {"id": 3, "enable": True, "protocol": "vless"},
            ],
        })
        panel.find_clients_full_by_email = AsyncMock(return_value=[
            {
                "inboundId": 2,
                "clientId": "uuid-old",
                "password": "",
                "subId": "sub-old",
                "protocol": "vless",
                "clientObj": {
                    "id": "uuid-old",
                    "email": "user_1@vpnbot",
                    "subId": "sub-old",
                    "limitIp": 1,
                    "totalGB": 1,
                    "expiryTime": 1,
                },
            }
        ])
        seen_payloads = []

        async def fake_request(method, url, **kwargs):
            seen_payloads.append((url, kwargs["json"]))
            return 200, {"success": True}, ""

        panel._request_json_with_reauth = AsyncMock(side_effect=fake_request)

        with patch("services.panel.Config.PANEL_TARGET_INBOUND_IDS", "2,3"):
            client = await panel.upsert_client(email="1@vpnbot", limit_ip=2, total_gb=50, days=30)

        self.assertIsNotNone(client)
        self.assertEqual(len(seen_payloads), 2)
        self.assertIn("/panel/api/inbounds/update/2", seen_payloads[0][0])
        self.assertIn("/panel/api/inbounds/update/3", seen_payloads[1][0])
        self.assertIn('"email": "i2_1@vpnbot"', seen_payloads[0][1]["settings"])
        self.assertIn('"email": "i3_1@vpnbot"', seen_payloads[1][1]["settings"])
        self.assertIn('"id": "uuid-old"', seen_payloads[0][1]["settings"])
        self.assertIn('"subId": "sub-old"', seen_payloads[1][1]["settings"])


if __name__ == "__main__":
    unittest.main()
