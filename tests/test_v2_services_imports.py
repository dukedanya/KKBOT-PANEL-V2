import unittest


class V2ServicesImportTests(unittest.TestCase):
    def test_can_import_v2_and_legacy_namespaces(self) -> None:
        from kkbot.services.panel import PanelAPI
        from kkbot.services.payment_flow import PaymentFlowService
        from kkbot.services.subscriptions import SubscriptionService
        import kkbot.handlers.start  # noqa: F401
        import kkbot.utils.helpers  # noqa: F401

        self.assertIsNotNone(PanelAPI)
        self.assertIsNotNone(PaymentFlowService)
        self.assertIsNotNone(SubscriptionService)
