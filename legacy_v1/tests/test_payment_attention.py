import tempfile
import unittest
from unittest.mock import AsyncMock

from db import Database


class PaymentAttentionDbTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.sqlite3', delete=False)
        self.tmp.close()
        self.db = Database(self.tmp.name)
        await self.db.connect()
        await self.db.add_user(1)
        await self.db.add_user(2)
        await self.db.add_user(3)

        await self.db.add_pending_payment('pay-processing', 1, 'basic', 100.0, provider='yookassa')
        await self.db.conn.execute(
            "UPDATE pending_payments SET status = 'processing', processing_started_at = datetime('now', '-40 minutes') WHERE payment_id = ?",
            ('pay-processing',),
        )

        await self.db.add_pending_payment('pay-refund', 2, 'basic', 200.0, provider='telegram_stars')
        await self.db.record_payment_status_transition(
            'pay-refund',
            from_status='accepted',
            to_status='refund_requested',
            source='test',
            reason='admin=1',
        )
        await self.db.conn.execute(
            "UPDATE payment_status_history SET created_at = datetime('now', '-90 minutes') WHERE payment_id = ? AND to_status = 'refund_requested'",
            ('pay-refund',),
        )

        await self.db.add_pending_payment('pay-mismatch', 3, 'basic', 300.0, provider='yookassa')
        await self.db.register_payment_event(
            'evt-pay-mismatch',
            payment_id='pay-mismatch',
            source='yookassa/webhook',
            event_type='payment.succeeded',
            payload_excerpt='ok',
        )
        await self.db.conn.commit()

    async def asyncTearDown(self):
        await self.db.close()

    async def test_returns_stale_processing_payments(self):
        rows = await self.db.get_stale_processing_payments(minutes=15)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['payment_id'], 'pay-processing')

    async def test_returns_overdue_operations(self):
        rows = await self.db.get_overdue_payment_operations(minutes=20)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['payment_id'], 'pay-refund')

    async def test_returns_confirmed_mismatches(self):
        rows = await self.db.get_confirmed_payment_status_mismatches(hours=24)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['payment_id'], 'pay-mismatch')
        self.assertEqual(rows[0]['event_type'], 'payment.succeeded')


class PaymentAttentionRenderTests(unittest.IsolatedAsyncioTestCase):
    async def test_render_attention_text_contains_sections(self):
        from handlers.payment_diagnostics import _render_attention_text

        db = type('DbStub', (), {})()
        db.get_stale_processing_payments = AsyncMock(return_value=[
            {
                'payment_id': 'pay-processing',
                'provider': 'yookassa',
                'processing_started_at': '2026-03-24 12:00:00',
                'user_id': 1,
            }
        ])
        db.get_overdue_payment_operations = AsyncMock(return_value=[
            {
                'payment_id': 'pay-refund',
                'provider': 'telegram_stars',
                'status': 'accepted',
                'requested_status': 'refund_requested',
                'requested_at': '2026-03-24 11:00:00',
            }
        ])
        db.get_confirmed_payment_status_mismatches = AsyncMock(return_value=[
            {
                'payment_id': 'pay-mismatch',
                'provider': 'yookassa',
                'status': 'pending',
                'event_type': 'payment.succeeded',
                'event_created_at': '2026-03-24 12:30:00',
            }
        ])

        text, items = await _render_attention_text(db, provider='all', issue_type='all')
        self.assertEqual(len(items), 3)
        self.assertIn('Требует внимания', text)
        self.assertIn('Stale processing', text)
        self.assertIn('Webhook/status mismatch', text)
        self.assertIn('pay-processing', text)
        self.assertIn('pay-refund', text)
        self.assertIn('pay-mismatch', text)


if __name__ == '__main__':
    unittest.main()
