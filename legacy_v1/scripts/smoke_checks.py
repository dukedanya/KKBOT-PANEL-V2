import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from app.operational import run_startup_checks
from db.database import Database
from tariffs.loader import load_tariffs, get_all_active


async def main() -> None:
    load_tariffs()
    plans = get_all_active()
    assert plans, "No active tariffs loaded"

    db = Database(":memory:")
    await db.connect()
    try:
        user_id = 1
        await db.add_user(user_id)

        plan = plans[0]
        created = await db.add_pending_payment(
            "pay_1", user_id, plan["id"], float(plan.get("price_rub", 0) or 0)
        )
        assert created is True, "Pending payment should be created"

        duplicate = await db.add_pending_payment(
            "pay_1", user_id, plan["id"], float(plan.get("price_rub", 0) or 0)
        )
        assert duplicate is False, "Duplicate payment insert should be ignored"

        payment = await db.get_pending_payment("pay_1")
        assert payment is not None, "Pending payment not created"
        assert payment["status"] == "pending", "Unexpected initial status"

        updated = await db.update_payment_status("pay_1", "accepted")
        assert updated is True, "Status update should succeed for pending payment"
        updated_again = await db.update_payment_status("pay_1", "rejected")
        assert updated_again is False, "Second status update should be idempotently rejected"

        await db.add_pending_payment("pay_2", user_id, plan["id"], 10.0)
        claimed = await db.claim_pending_payment("pay_2")
        assert claimed is True, "Claim should succeed for pending payment"
        released = await db.release_processing_payment("pay_2", error_text="manual_test")
        assert released is True, "Release should return processing payment back to pending"

        await db.claim_pending_payment("pay_2")
        assert db.conn is not None
        await db.conn.execute(
            "UPDATE pending_payments SET processing_started_at = datetime('now', '-30 minutes') WHERE payment_id = ?",
            ("pay_2",),
        )
        await db.conn.commit()
        reclaimed = await db.reclaim_stale_processing_payments(timeout_minutes=15)
        assert reclaimed >= 1, "Stale processing payment should be reclaimed"

        await db.add_balance(user_id, 100.0)
        request_id = await db.create_withdraw_request(user_id, 30.0)
        accepted = await db.process_withdraw_request(request_id, accept=True)
        assert accepted is True, "Withdraw request should complete with sufficient balance"
        balance = await db.get_balance(user_id)
        assert abs(balance - 70.0) < 1e-9, "Balance should decrease after accepted withdraw"

        request_id_2 = await db.create_withdraw_request(user_id, 1000.0)
        accepted_2 = await db.process_withdraw_request(request_id_2, accept=True)
        assert accepted_2 is False, "Withdraw request should fail with insufficient balance"

        container = SimpleNamespace(
            db=db,
            panel=SimpleNamespace(get_inbounds=AsyncMock(return_value={"success": True})),
            itpay=SimpleNamespace(_get_session=AsyncMock(return_value=SimpleNamespace(closed=False))),
        )
        report = await run_startup_checks(container=container, base_dir=str(PROJECT_ROOT))
        assert report.ok, f"Startup checks are not healthy: {report.checks}"

        print("Smoke checks passed")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
