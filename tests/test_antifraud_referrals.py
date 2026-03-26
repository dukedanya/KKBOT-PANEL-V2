import unittest
from unittest.mock import AsyncMock


class AntifraudReferralTests(unittest.IsolatedAsyncioTestCase):
    async def test_self_referral_does_not_mark_user_suspicious_forever(self) -> None:
        from services.antifraud import evaluate_referral_link

        db = AsyncMock()
        db.add_antifraud_event = AsyncMock()
        db.get_user = AsyncMock(return_value=None)

        allowed, reason = await evaluate_referral_link(10, 10, db=db)

        self.assertFalse(allowed)
        self.assertEqual(reason, "self_referral")
        db.add_antifraud_event.assert_awaited_once()
        self.assertFalse(hasattr(db, "mark_referral_suspicious") and db.mark_referral_suspicious.await_count)

    async def test_referrer_with_only_self_referral_note_is_not_hard_blocked(self) -> None:
        from services.antifraud import evaluate_referral_link

        db = AsyncMock()
        db.add_antifraud_event = AsyncMock()
        db.get_user = AsyncMock(
            return_value={
                "user_id": 99,
                "ref_suspicious": 1,
                "partner_note": "Попытка self-referral",
            }
        )
        db.count_recent_referrals_by_referrer = AsyncMock(return_value=0)

        allowed, reason = await evaluate_referral_link(11, 99, db=db)

        self.assertTrue(allowed)
        self.assertEqual(reason, "")
