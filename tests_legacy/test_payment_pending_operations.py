import tempfile
import unittest
from unittest.mock import AsyncMock

from db import Database


class PendingPaymentOperationsDbTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.sqlite3', delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        await self.db.connect()
        await self.db.add_user(1)
        await self.db.add_user(2)
        await self.db.add_pending_payment('pay-refund', 1, 'basic', 100.0, provider='yookassa')
        await self.db.add_pending_payment('pay-cancel', 2, 'basic', 200.0, provider='telegram_stars')
        await self.db.record_payment_status_transition(
            'pay-refund',
            from_status='accepted',
            to_status='refund_requested',
            source='test',
            reason='admin=1',
        )
        await self.db.record_payment_status_transition(
            'pay-cancel',
            from_status='processing',
            to_status='cancel_requested',
            source='test',
            reason='admin=1',
        )

    async def asyncTearDown(self):
        await self.db.close()

    async def test_returns_pending_operations_with_filters(self):
        items = await self.db.get_pending_payment_operations(limit=10)
        self.assertEqual({row['payment_id'] for row in items}, {'pay-refund', 'pay-cancel'})

        refund_items = await self.db.get_pending_payment_operations(limit=10, operation='refund')
        self.assertEqual(len(refund_items), 1)
        self.assertEqual(refund_items[0]['payment_id'], 'pay-refund')

        provider_items = await self.db.get_pending_payment_operations(limit=10, provider='telegram_stars')
        self.assertEqual(len(provider_items), 1)
        self.assertEqual(provider_items[0]['payment_id'], 'pay-cancel')

    async def test_excludes_finalized_payments(self):
        await self.db.conn.execute("UPDATE pending_payments SET status = 'refunded' WHERE payment_id = ?", ('pay-refund',))
        await self.db.conn.commit()
        items = await self.db.get_pending_payment_operations(limit=10, operation='refund')
        self.assertEqual(items, [])


class PendingPaymentOperationsRenderTests(unittest.IsolatedAsyncioTestCase):
    async def test_render_pending_operations_text_contains_filters(self):
        from handlers.payment_diagnostics import _render_pending_operations_text

        db = type('DbStub', (), {})()
        db.get_pending_payment_operations = AsyncMock(return_value=[
            {
                'payment_id': 'pay-1',
                'provider': 'yookassa',
                'status': 'accepted',
                'requested_status': 'refund_requested',
                'requested_at': '2026-03-24 12:00:00',
            }
        ])

        text, items = await _render_pending_operations_text(db, provider='yookassa', operation='refund')
        self.assertEqual(len(items), 1)
        self.assertIn('ЮKassa', text)
        self.assertIn('Refund', text)
        self.assertIn('pay-1', text)


if __name__ == '__main__':
    unittest.main()
