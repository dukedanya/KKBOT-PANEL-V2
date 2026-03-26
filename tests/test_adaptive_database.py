import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


class AdaptiveDatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def test_connect_runs_postgres_migrations_and_import_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "legacy.db"
            sqlite_path.touch()

            fake_pg = AsyncMock()
            fake_pg.pool = object()
            fake_pg.get_meta = AsyncMock(return_value=None)
            fake_pg.set_meta = AsyncMock()

            with (
                patch("db.adaptive_database.Config.DATABASE_URL", "postgresql://test"),
                patch("db.adaptive_database.Config.DATABASE_MIN_POOL", 1),
                patch("db.adaptive_database.Config.DATABASE_MAX_POOL", 2),
                patch("db.adaptive_database.PostgresDatabase", return_value=fake_pg),
                patch("db.adaptive_database.apply_postgres_migrations", new=AsyncMock(return_value=["001_bootstrap.sql"])),
                patch(
                    "db.adaptive_database.import_legacy_sqlite_to_postgres",
                    new=AsyncMock(return_value=type("Report", (), {"total_rows": 9})()),
                ) as import_mock,
            ):
                from db.adaptive_database import Database

                db = Database(str(sqlite_path))
                await db.connect()
                await db.close()

            fake_pg.connect.assert_awaited()
            fake_pg.get_meta.assert_awaited_with("legacy_sqlite_import")
            import_mock.assert_awaited_once()
            fake_pg.set_meta.assert_awaited()
            fake_pg.close.assert_awaited()

    async def test_connect_skips_import_when_already_marked_completed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "legacy.db"
            sqlite_path.touch()

            fake_pg = AsyncMock()
            fake_pg.pool = object()
            fake_pg.get_meta = AsyncMock(return_value={"completed": True})
            fake_pg.set_meta = AsyncMock()

            with (
                patch("db.adaptive_database.Config.DATABASE_URL", "postgresql://test"),
                patch("db.adaptive_database.Config.DATABASE_MIN_POOL", 1),
                patch("db.adaptive_database.Config.DATABASE_MAX_POOL", 2),
                patch("db.adaptive_database.PostgresDatabase", return_value=fake_pg),
                patch("db.adaptive_database.apply_postgres_migrations", new=AsyncMock(return_value=["001_bootstrap.sql"])),
                patch("db.adaptive_database.import_legacy_sqlite_to_postgres", new=AsyncMock()) as import_mock,
            ):
                from db.adaptive_database import Database

                db = Database(str(sqlite_path))
                await db.connect()
                await db.close()

            import_mock.assert_not_awaited()
            fake_pg.set_meta.assert_not_awaited()

    async def test_settings_and_user_reads_can_use_postgres_side(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "legacy.db"
            sqlite_path.touch()

            fake_pg = AsyncMock()
            fake_pg.pool = object()
            fake_pg.get_meta = AsyncMock(return_value={"completed": True})

            with (
                patch("db.adaptive_database.Config.DATABASE_URL", "postgresql://test"),
                patch("db.adaptive_database.Config.DATABASE_MIN_POOL", 1),
                patch("db.adaptive_database.Config.DATABASE_MAX_POOL", 2),
                patch("db.adaptive_database.PostgresDatabase", return_value=fake_pg),
                patch("db.adaptive_database.apply_postgres_migrations", new=AsyncMock(return_value=[])),
            ):
                from db.adaptive_database import Database

                db = Database(str(sqlite_path))
                await db.connect()

                db.legacy.get_setting = AsyncMock(return_value="legacy")
                db.legacy.get_total_users = AsyncMock(return_value=3)
                db.legacy.get_user = AsyncMock(return_value=None)

                meta_repo = AsyncMock()
                meta_repo.get_legacy_setting = AsyncMock(return_value="pg-setting")
                meta_repo.get_legacy_payload = AsyncMock(return_value={"user_id": 42, "vpn_url": "sub"})

                user_repo = AsyncMock()
                user_repo.count_users = AsyncMock(return_value=9)

                db._meta_repo = lambda: meta_repo  # type: ignore[method-assign]
                db._user_repo = lambda: user_repo  # type: ignore[method-assign]

                self.assertEqual(await db.get_setting("foo", "default"), "pg-setting")
                self.assertEqual(await db.get_total_users(), 9)
                self.assertEqual((await db.get_user(42))["user_id"], 42)

                await db.close()

    async def test_add_user_mirrors_into_postgres_side(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "legacy.db"
            sqlite_path.touch()

            fake_pg = AsyncMock()
            fake_pg.pool = object()
            fake_pg.get_meta = AsyncMock(return_value={"completed": True})

            with (
                patch("db.adaptive_database.Config.DATABASE_URL", "postgresql://test"),
                patch("db.adaptive_database.Config.DATABASE_MIN_POOL", 1),
                patch("db.adaptive_database.Config.DATABASE_MAX_POOL", 2),
                patch("db.adaptive_database.PostgresDatabase", return_value=fake_pg),
                patch("db.adaptive_database.apply_postgres_migrations", new=AsyncMock(return_value=[])),
            ):
                from db.adaptive_database import Database

                db = Database(str(sqlite_path))
                await db.connect()

                db.legacy.add_user = AsyncMock(return_value=True)
                db.legacy.get_user = AsyncMock(return_value={"user_id": 7, "has_subscription": 0})

                meta_repo = AsyncMock()
                user_repo = AsyncMock()
                db._meta_repo = lambda: meta_repo  # type: ignore[method-assign]
                db._user_repo = lambda: user_repo  # type: ignore[method-assign]

                created = await db.add_user(7)
                self.assertTrue(created)
                user_repo.upsert_basic_user.assert_awaited_once()
                meta_repo.set_legacy_payload.assert_awaited_once()

                await db.close()

    async def test_pending_payment_is_mirrored_into_postgres_side(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "legacy.db"
            sqlite_path.touch()

            fake_pg = AsyncMock()
            fake_pg.pool = object()
            fake_pg.get_meta = AsyncMock(return_value={"completed": True})

            with (
                patch("db.adaptive_database.Config.DATABASE_URL", "postgresql://test"),
                patch("db.adaptive_database.Config.DATABASE_MIN_POOL", 1),
                patch("db.adaptive_database.Config.DATABASE_MAX_POOL", 2),
                patch("db.adaptive_database.PostgresDatabase", return_value=fake_pg),
                patch("db.adaptive_database.apply_postgres_migrations", new=AsyncMock(return_value=[])),
            ):
                from db.adaptive_database import Database

                db = Database(str(sqlite_path))
                await db.connect()

                payload = {"payment_id": "pay-1", "user_id": 7, "status": "pending", "amount": 100}
                db.legacy.add_pending_payment = AsyncMock(return_value=True)
                db.legacy.get_pending_payment = AsyncMock(return_value=payload)

                payment_repo = AsyncMock()
                db._payment_repo = lambda: payment_repo  # type: ignore[method-assign]

                created = await db.add_pending_payment("pay-1", 7, "plan", 100)
                self.assertTrue(created)
                payment_repo.upsert_legacy_intent.assert_awaited_once_with(payload)

                await db.close()

    async def test_payment_status_transition_is_mirrored_into_postgres_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "legacy.db"
            sqlite_path.touch()

            fake_pg = AsyncMock()
            fake_pg.pool = object()
            fake_pg.get_meta = AsyncMock(return_value={"completed": True})

            with (
                patch("db.adaptive_database.Config.DATABASE_URL", "postgresql://test"),
                patch("db.adaptive_database.Config.DATABASE_MIN_POOL", 1),
                patch("db.adaptive_database.Config.DATABASE_MAX_POOL", 2),
                patch("db.adaptive_database.PostgresDatabase", return_value=fake_pg),
                patch("db.adaptive_database.apply_postgres_migrations", new=AsyncMock(return_value=[])),
            ):
                from db.adaptive_database import Database

                db = Database(str(sqlite_path))
                await db.connect()

                db.legacy.record_payment_status_transition = AsyncMock(return_value=11)
                payment_repo = AsyncMock()
                db._payment_repo = lambda: payment_repo  # type: ignore[method-assign]

                result = await db.record_payment_status_transition(
                    "pay-2",
                    from_status="pending",
                    to_status="processing",
                    source="test",
                    reason="move",
                    metadata="raw",
                )
                self.assertEqual(result, 11)
                payment_repo.append_status_history.assert_awaited_once()

                await db.close()

    async def test_balance_and_ref_code_changes_refresh_user_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "legacy.db"
            sqlite_path.touch()

            fake_pg = AsyncMock()
            fake_pg.pool = object()
            fake_pg.get_meta = AsyncMock(return_value={"completed": True})

            with (
                patch("db.adaptive_database.Config.DATABASE_URL", "postgresql://test"),
                patch("db.adaptive_database.Config.DATABASE_MIN_POOL", 1),
                patch("db.adaptive_database.Config.DATABASE_MAX_POOL", 2),
                patch("db.adaptive_database.PostgresDatabase", return_value=fake_pg),
                patch("db.adaptive_database.apply_postgres_migrations", new=AsyncMock(return_value=[])),
            ):
                from db.adaptive_database import Database

                db = Database(str(sqlite_path))
                await db.connect()

                db.legacy.add_balance = AsyncMock(return_value=True)
                db.legacy.subtract_balance = AsyncMock(return_value=True)
                db.legacy.ensure_ref_code = AsyncMock(return_value="REF12345")
                db.legacy.set_ref_by = AsyncMock(return_value=True)
                db.legacy.get_user = AsyncMock(return_value={"user_id": 5, "balance": 10.0, "ref_code": "REF12345"})

                meta_repo = AsyncMock()
                user_repo = AsyncMock()
                db._meta_repo = lambda: meta_repo  # type: ignore[method-assign]
                db._user_repo = lambda: user_repo  # type: ignore[method-assign]

                self.assertTrue(await db.add_balance(5, 10))
                self.assertTrue(await db.subtract_balance(5, 2))
                self.assertEqual(await db.ensure_ref_code(5), "REF12345")
                self.assertTrue(await db.set_ref_by(5, 9))
                self.assertGreaterEqual(meta_repo.set_legacy_payload.await_count, 4)

                await db.close()

    async def test_promo_code_is_mirrored_and_can_be_read_from_postgres_meta(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "legacy.db"
            sqlite_path.touch()

            fake_pg = AsyncMock()
            fake_pg.pool = object()
            fake_pg.get_meta = AsyncMock(return_value={"completed": True})

            with (
                patch("db.adaptive_database.Config.DATABASE_URL", "postgresql://test"),
                patch("db.adaptive_database.Config.DATABASE_MIN_POOL", 1),
                patch("db.adaptive_database.Config.DATABASE_MAX_POOL", 2),
                patch("db.adaptive_database.PostgresDatabase", return_value=fake_pg),
                patch("db.adaptive_database.apply_postgres_migrations", new=AsyncMock(return_value=[])),
            ):
                from db.adaptive_database import Database

                db = Database(str(sqlite_path))
                await db.connect()

                promo_payload = {"code": "WELCOME", "active": 1}
                db.legacy.create_or_update_promo_code = AsyncMock(return_value=True)
                db.legacy.get_promo_code = AsyncMock(side_effect=[promo_payload, None])

                meta_repo = AsyncMock()
                meta_repo.get_legacy_payload = AsyncMock(return_value=promo_payload)
                db._meta_repo = lambda: meta_repo  # type: ignore[method-assign]

                self.assertTrue(await db.create_or_update_promo_code("welcome", title="Hello"))
                promo = await db.get_promo_code("welcome")
                self.assertEqual(promo["code"], "WELCOME")
                meta_repo.set_legacy_payload.assert_awaited_once()

                await db.close()

    async def test_withdraw_request_is_mirrored_into_postgres_side(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "legacy.db"
            sqlite_path.touch()

            fake_pg = AsyncMock()
            fake_pg.pool = object()
            fake_pg.get_meta = AsyncMock(return_value={"completed": True})

            with (
                patch("db.adaptive_database.Config.DATABASE_URL", "postgresql://test"),
                patch("db.adaptive_database.Config.DATABASE_MIN_POOL", 1),
                patch("db.adaptive_database.Config.DATABASE_MAX_POOL", 2),
                patch("db.adaptive_database.PostgresDatabase", return_value=fake_pg),
                patch("db.adaptive_database.apply_postgres_migrations", new=AsyncMock(return_value=[])),
            ):
                from db.adaptive_database import Database

                db = Database(str(sqlite_path))
                await db.connect()

                payload = {"id": 9, "user_id": 5, "amount": 100.0, "status": "pending"}
                db.legacy.create_withdraw_request = AsyncMock(return_value=9)
                db.legacy.get_withdraw_request = AsyncMock(return_value=payload)

                ops_repo = AsyncMock()
                db._operations_repo = lambda: ops_repo  # type: ignore[method-assign]

                request_id = await db.create_withdraw_request(5, 100.0)
                self.assertEqual(request_id, 9)
                ops_repo.upsert_withdraw_request.assert_awaited_once_with(payload)

                await db.close()

    async def test_support_message_is_mirrored_into_postgres_side(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "legacy.db"
            sqlite_path.touch()

            fake_pg = AsyncMock()
            fake_pg.pool = object()
            fake_pg.get_meta = AsyncMock(return_value={"completed": True})

            with (
                patch("db.adaptive_database.Config.DATABASE_URL", "postgresql://test"),
                patch("db.adaptive_database.Config.DATABASE_MIN_POOL", 1),
                patch("db.adaptive_database.Config.DATABASE_MAX_POOL", 2),
                patch("db.adaptive_database.PostgresDatabase", return_value=fake_pg),
                patch("db.adaptive_database.apply_postgres_migrations", new=AsyncMock(return_value=[])),
            ):
                from db.adaptive_database import Database

                db = Database(str(sqlite_path))
                await db.connect()

                db.legacy.add_support_message = AsyncMock(return_value=17)
                db.legacy.get_support_ticket = AsyncMock(return_value={"id": 3, "user_id": 5, "status": "open"})
                db.legacy.get_last_support_message = AsyncMock(return_value={"id": 17, "ticket_id": 3, "sender_role": "user"})

                ops_repo = AsyncMock()
                db._operations_repo = lambda: ops_repo  # type: ignore[method-assign]

                message_id = await db.add_support_message(3, "user", 5, "hello")
                self.assertEqual(message_id, 17)
                ops_repo.upsert_support_ticket.assert_awaited_once()
                ops_repo.add_support_message.assert_awaited_once()

                await db.close()

    async def test_admin_and_payment_events_are_mirrored_into_postgres_side(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "legacy.db"
            sqlite_path.touch()

            fake_pg = AsyncMock()
            fake_pg.pool = object()
            fake_pg.get_meta = AsyncMock(return_value={"completed": True})

            with (
                patch("db.adaptive_database.Config.DATABASE_URL", "postgresql://test"),
                patch("db.adaptive_database.Config.DATABASE_MIN_POOL", 1),
                patch("db.adaptive_database.Config.DATABASE_MAX_POOL", 2),
                patch("db.adaptive_database.PostgresDatabase", return_value=fake_pg),
                patch("db.adaptive_database.apply_postgres_migrations", new=AsyncMock(return_value=[])),
            ):
                from db.adaptive_database import Database

                db = Database(str(sqlite_path))
                await db.connect()

                db.legacy.add_admin_user_action = AsyncMock(return_value=1)
                db.legacy.add_payment_admin_action = AsyncMock(return_value=2)
                db.legacy.register_payment_event = AsyncMock(return_value=True)
                db.legacy.add_antifraud_event = AsyncMock(return_value=3)

                ops_repo = AsyncMock()
                db._operations_repo = lambda: ops_repo  # type: ignore[method-assign]

                self.assertEqual(await db.add_admin_user_action(5, 99, "ban", "x"), 1)
                self.assertEqual(
                    await db.add_payment_admin_action("pay-7", 99, "approve", provider="itpay", result="ok", details="done"),
                    2,
                )
                self.assertTrue(
                    await db.register_payment_event(
                        "evt-1",
                        payment_id="pay-7",
                        source="webhook",
                        event_type="paid",
                        payload_excerpt="{}",
                    )
                )
                self.assertEqual(await db.add_antifraud_event(5, "spam", "details", "high"), 3)

                ops_repo.insert_admin_user_action.assert_awaited_once()
                ops_repo.insert_payment_admin_action.assert_awaited_once()
                ops_repo.register_payment_event.assert_awaited_once()
                ops_repo.insert_antifraud_event.assert_awaited_once()

                await db.close()

    async def test_payment_queries_can_read_from_postgres_side(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "legacy.db"
            sqlite_path.touch()

            fake_pg = AsyncMock()
            fake_pg.pool = object()
            fake_pg.get_meta = AsyncMock(return_value={"completed": True})

            with (
                patch("db.adaptive_database.Config.DATABASE_URL", "postgresql://test"),
                patch("db.adaptive_database.Config.DATABASE_MIN_POOL", 1),
                patch("db.adaptive_database.Config.DATABASE_MAX_POOL", 2),
                patch("db.adaptive_database.PostgresDatabase", return_value=fake_pg),
                patch("db.adaptive_database.apply_postgres_migrations", new=AsyncMock(return_value=[])),
            ):
                from db.adaptive_database import Database

                db = Database(str(sqlite_path))
                await db.connect()

                db.legacy.get_pending_payment_by_provider_id = AsyncMock(return_value=None)
                db.legacy.get_pending_payments_by_user = AsyncMock(return_value=[])
                db.legacy.get_all_pending_payments = AsyncMock(return_value=[])
                db.legacy.get_stale_processing_payments = AsyncMock(return_value=[])
                db.legacy.get_confirmed_payment_status_mismatches = AsyncMock(return_value=[])

                repo = AsyncMock()
                repo.get_by_provider_payment_id = AsyncMock(return_value={"payment_id": "p1", "meta": {"legacy_payload": {"payment_id": "p1"}}})
                repo.list_by_user = AsyncMock(return_value=[{"payment_id": "p2", "status": "pending", "plan_id": "pro", "meta": {"legacy_payload": {"payment_id": "p2", "status": "pending", "plan_id": "pro"}}}])
                repo.list_by_statuses = AsyncMock(return_value=[{"payment_id": "p3", "meta": {"legacy_payload": {"payment_id": "p3"}}}])
                repo.list_stale_processing = AsyncMock(return_value=[{"payment_id": "p4", "meta": {"legacy_payload": {"payment_id": "p4"}}}])
                repo.list_confirmed_mismatches = AsyncMock(return_value=[{"payment_id": "p5"}])
                db._payment_repo = lambda: repo  # type: ignore[method-assign]

                self.assertEqual((await db.get_pending_payment_by_provider_id("itpay", "prov1"))["payment_id"], "p1")
                self.assertEqual((await db.get_user_pending_payment(7, plan_id="pro"))["payment_id"], "p2")
                self.assertEqual((await db.get_all_pending_payments())[0]["payment_id"], "p3")
                self.assertEqual((await db.get_stale_processing_payments())[0]["payment_id"], "p4")
                self.assertEqual((await db.get_confirmed_payment_status_mismatches())[0]["payment_id"], "p5")

                await db.close()

    async def test_withdraw_and_support_settings_queries_can_read_from_postgres_side(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "legacy.db"
            sqlite_path.touch()

            fake_pg = AsyncMock()
            fake_pg.pool = object()
            fake_pg.get_meta = AsyncMock(return_value={"completed": True})

            with (
                patch("db.adaptive_database.Config.DATABASE_URL", "postgresql://test"),
                patch("db.adaptive_database.Config.DATABASE_MIN_POOL", 1),
                patch("db.adaptive_database.Config.DATABASE_MAX_POOL", 2),
                patch("db.adaptive_database.PostgresDatabase", return_value=fake_pg),
                patch("db.adaptive_database.apply_postgres_migrations", new=AsyncMock(return_value=[])),
            ):
                from db.adaptive_database import Database

                db = Database(str(sqlite_path))
                await db.connect()

                db.legacy.get_user_pending_withdraw_request = AsyncMock(return_value=None)
                db.legacy.get_withdraw_requests_by_user = AsyncMock(return_value=[])

                ops_repo = AsyncMock()
                ops_repo.get_pending_withdraw_request_for_user = AsyncMock(return_value={"id": 12, "meta": {"legacy_payload": {"id": 12, "status": "pending"}}})
                ops_repo.list_withdraw_requests_by_user = AsyncMock(return_value=[{"id": 13, "meta": {"legacy_payload": {"id": 13, "status": "completed"}}}])
                db._operations_repo = lambda: ops_repo  # type: ignore[method-assign]

                db.get_setting = AsyncMock(return_value="0")  # type: ignore[method-assign]

                self.assertEqual((await db.get_user_pending_withdraw_request(7))["id"], 12)
                self.assertEqual((await db.get_withdraw_requests_by_user(7))[0]["id"], 13)
                self.assertFalse(await db.support_restriction_notifications_enabled())

                await db.close()

    async def test_referral_queries_can_read_from_postgres_side(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "legacy.db"
            sqlite_path.touch()

            fake_pg = AsyncMock()
            fake_pg.pool = object()
            fake_pg.get_meta = AsyncMock(return_value={"completed": True})

            with (
                patch("db.adaptive_database.Config.DATABASE_URL", "postgresql://test"),
                patch("db.adaptive_database.Config.DATABASE_MIN_POOL", 1),
                patch("db.adaptive_database.Config.DATABASE_MAX_POOL", 2),
                patch("db.adaptive_database.PostgresDatabase", return_value=fake_pg),
                patch("db.adaptive_database.apply_postgres_migrations", new=AsyncMock(return_value=[])),
            ):
                from db.adaptive_database import Database

                db = Database(str(sqlite_path))
                await db.connect()

                db.legacy.get_partner_settings = AsyncMock(
                    return_value={
                        "status": "standard",
                        "custom_percent_level1": None,
                        "custom_percent_level2": None,
                        "custom_percent_level3": None,
                        "note": "",
                        "suspicious": False,
                    }
                )
                db.legacy.get_ref_history = AsyncMock(return_value=[])
                db.legacy.get_referrals_list = AsyncMock(return_value=[])
                db.legacy.get_top_referrers_extended = AsyncMock(return_value=[])
                db.legacy.count_recent_referrals_by_referrer = AsyncMock(return_value=0)
                db.legacy.get_user = AsyncMock(return_value={"user_id": 1, "ref_suspicious": 0})
                db.legacy.mark_referral_suspicious = AsyncMock(return_value=True)

                ref_repo = AsyncMock()
                ref_repo.list_history = AsyncMock(return_value=[{"id": 1, "amount": 50}])
                ref_repo.list_referrals = AsyncMock(return_value=[{"user_id": 2, "ref_rewarded": 1}])
                ref_repo.get_summary = AsyncMock(return_value={"total_refs": 1, "paid_refs": 1, "earned_rub": 50.0, "earned_bonus_days": 0, "completed_withdraw_rub": 0.0, "pending_withdraw_rub": 0.0})
                ref_repo.list_top_referrers_extended = AsyncMock(return_value=[{"ref_by": 1, "paid_count": 1}])
                ref_repo.count_recent_referrals = AsyncMock(return_value=3)

                user_repo = AsyncMock()
                user_repo.list_suspicious_referrals = AsyncMock(return_value=[{"user_id": 3, "ref_by": 1}])

                db._referral_repo = lambda: ref_repo  # type: ignore[method-assign]
                db._user_repo = lambda: user_repo  # type: ignore[method-assign]
                db._sync_legacy_user_payload = AsyncMock()  # type: ignore[method-assign]

                self.assertEqual((await db.get_ref_history(1))[0]["id"], 1)
                self.assertEqual((await db.get_referrals_list(1))[0]["user_id"], 2)
                self.assertEqual((await db.get_referral_summary(1))["earned_rub"], 50.0)
                self.assertEqual((await db.get_top_referrers_extended())[0]["ref_by"], 1)
                self.assertEqual(await db.count_recent_referrals_by_referrer(1), 3)
                self.assertTrue(await db.mark_referral_suspicious(1, True, "note"))
                self.assertEqual((await db.get_suspicious_referrals())[0]["user_id"], 3)

                cabinet = await db.get_referral_partner_cabinet(1)
                self.assertEqual(cabinet["total_refs"], 1)
                self.assertEqual(cabinet["conversion_pct"], 100.0)

                await db.close()

    async def test_support_restriction_list_and_user_reads_can_use_postgres_side(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "legacy.db"
            sqlite_path.touch()

            fake_pg = AsyncMock()
            fake_pg.pool = object()
            fake_pg.get_meta = AsyncMock(return_value={"completed": True})

            with (
                patch("db.adaptive_database.Config.DATABASE_URL", "postgresql://test"),
                patch("db.adaptive_database.Config.DATABASE_MIN_POOL", 1),
                patch("db.adaptive_database.Config.DATABASE_MAX_POOL", 2),
                patch("db.adaptive_database.PostgresDatabase", return_value=fake_pg),
                patch("db.adaptive_database.apply_postgres_migrations", new=AsyncMock(return_value=[])),
            ):
                from db.adaptive_database import Database

                db = Database(str(sqlite_path))
                await db.connect()

                db.get_support_restriction = AsyncMock(return_value={"active": True, "expires_at": "2026-01-01", "reason": "spam"})  # type: ignore[method-assign]
                meta_repo = AsyncMock()
                meta_repo.list_legacy_settings = AsyncMock(
                    return_value=[
                        ("legacy_setting:support:blocked_until:7", {"value": "2026-01-01"}),
                        ("legacy_setting:support:blocked_until:8", {"value": "2026-01-02"}),
                    ]
                )
                user_repo = AsyncMock()
                user_repo.list_all_legacy_users = AsyncMock(return_value=[{"user_id": 1}, {"user_id": 2}])
                user_repo.count_banned_users = AsyncMock(return_value=4)

                db._meta_repo = lambda: meta_repo  # type: ignore[method-assign]
                db._user_repo = lambda: user_repo  # type: ignore[method-assign]

                rows = await db.list_support_restricted_users(limit=5)
                self.assertEqual(len(rows), 2)
                self.assertEqual(await db.get_banned_users_count(), 4)
                self.assertEqual(len(await db.get_all_users()), 2)

                await db.close()
