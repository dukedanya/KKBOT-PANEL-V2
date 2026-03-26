import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiohttp.test_utils import make_mocked_request


class StartupChecksTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_startup_checks_reports_pending_migrations(self):
        from app.operational import run_startup_checks

        fake_db = SimpleNamespace(
            get_applied_migration_versions=AsyncMock(return_value=[]),
            reclaim_stale_processing_payments=AsyncMock(return_value=0),
        )
        container = SimpleNamespace(db=fake_db, panel=object(), itpay=object())

        with tempfile.TemporaryDirectory() as tmpdir, \
             patch('app.operational.get_all_active', return_value=[{'id': 'basic'}]), \
             patch('app.operational.collect_health_snapshot', AsyncMock(return_value={'ok': True})):
            migrations_dir = Path(tmpdir) / 'migrations'
            migrations_dir.mkdir()
            (migrations_dir / '001_schema.sql').write_text('-- test', encoding='utf-8')

            report = await run_startup_checks(container=container, base_dir=tmpdir)

        self.assertEqual(report.checks['migrations']['status'], 'warn')
        self.assertIn('001_schema.sql', report.checks['migrations']['pending'])

    async def test_run_startup_checks_can_fail_on_pending_migrations(self):
        from app.operational import run_startup_checks

        fake_db = SimpleNamespace(
            get_applied_migration_versions=AsyncMock(return_value=[]),
            reclaim_stale_processing_payments=AsyncMock(return_value=0),
        )
        container = SimpleNamespace(db=fake_db, panel=object(), itpay=object())

        with tempfile.TemporaryDirectory() as tmpdir, \
             patch('app.operational.get_all_active', return_value=[{'id': 'basic'}]), \
             patch('app.operational.collect_health_snapshot', AsyncMock(return_value={'ok': True})), \
             patch('app.operational.Config.STARTUP_FAIL_ON_PENDING_MIGRATIONS', True):
            migrations_dir = Path(tmpdir) / 'migrations'
            migrations_dir.mkdir()
            (migrations_dir / '001_schema.sql').write_text('-- test', encoding='utf-8')

            with self.assertRaises(RuntimeError):
                await run_startup_checks(container=container, base_dir=tmpdir)

    async def test_run_startup_checks_recovers_stale_processing(self):
        from app.operational import run_startup_checks

        fake_db = SimpleNamespace(
            get_applied_migration_versions=AsyncMock(return_value=[]),
            reclaim_stale_processing_payments=AsyncMock(return_value=2),
        )
        container = SimpleNamespace(db=fake_db, panel=object(), itpay=object())

        with tempfile.TemporaryDirectory() as tmpdir, \
             patch('app.operational.get_all_active', return_value=[{'id': 'basic'}]), \
             patch('app.operational.collect_health_snapshot', AsyncMock(return_value={'ok': True})), \
             patch('app.operational.Config.STARTUP_RECOVER_STALE_PROCESSING', True), \
             patch('app.operational.Config.STALE_PROCESSING_TIMEOUT_MIN', 15):
            report = await run_startup_checks(container=container, base_dir=tmpdir)

        self.assertEqual(report.checks['stale_processing_recovery']['recovered'], 2)
        fake_db.reclaim_stale_processing_payments.assert_awaited_once_with(timeout_minutes=15)

    async def test_run_startup_checks_reports_schema_drift(self):
        from app.operational import run_startup_checks

        fake_db = SimpleNamespace(
            get_applied_migration_versions=AsyncMock(return_value=[]),
            reclaim_stale_processing_payments=AsyncMock(return_value=0),
            get_schema_drift_issues=AsyncMock(return_value=["missing_column:support_tickets.assigned_admin_id"]),
        )
        container = SimpleNamespace(db=fake_db, panel=object(), itpay=object())

        with tempfile.TemporaryDirectory() as tmpdir, \
             patch('app.operational.get_all_active', return_value=[{'id': 'basic'}]), \
             patch('app.operational.collect_health_snapshot', AsyncMock(return_value={'ok': True})), \
             patch('app.operational.Config.STARTUP_FAIL_ON_SCHEMA_DRIFT', False):
            report = await run_startup_checks(container=container, base_dir=tmpdir)

        self.assertEqual(report.checks['schema']['status'], 'fail')
        self.assertIn('missing_column:support_tickets.assigned_admin_id', report.checks['schema']['issues'])

    async def test_run_startup_checks_can_fail_on_schema_drift(self):
        from app.operational import run_startup_checks

        fake_db = SimpleNamespace(
            get_applied_migration_versions=AsyncMock(return_value=[]),
            reclaim_stale_processing_payments=AsyncMock(return_value=0),
            get_schema_drift_issues=AsyncMock(return_value=["missing_table:support_tickets"]),
        )
        container = SimpleNamespace(db=fake_db, panel=object(), itpay=object())

        with tempfile.TemporaryDirectory() as tmpdir, \
             patch('app.operational.get_all_active', return_value=[{'id': 'basic'}]), \
             patch('app.operational.collect_health_snapshot', AsyncMock(return_value={'ok': True})), \
             patch('app.operational.Config.STARTUP_FAIL_ON_SCHEMA_DRIFT', True):
            with self.assertRaises(RuntimeError):
                await run_startup_checks(container=container, base_dir=tmpdir)


class WebhookAppTests(unittest.IsolatedAsyncioTestCase):
    async def test_build_webhook_app_registers_operational_routes(self):
        from services.webhook import build_webhook_app

        with patch('services.webhook.Config.ITPAY_WEBHOOK_PATH', '/itpay/webhook'), \
             patch('services.webhook.Config.HEALTHCHECK_PATH', '/healthz'), \
             patch('services.webhook.Config.READINESS_PATH', '/readyz'), \
             patch('services.webhook.Config.ENABLE_HEALTH_ENDPOINTS', True):
            app = build_webhook_app(bot=object(), db=object(), panel=object(), payment_gateway=object())

        paths = sorted(route.resource.canonical for route in app.router.routes())
        self.assertIn('/itpay/webhook', paths)
        self.assertIn('/healthz', paths)
        self.assertIn('/readyz', paths)

    async def test_readiness_handler_returns_503_for_degraded_state(self):
        from services.webhook import readiness_handler, DB_APP_KEY, PANEL_APP_KEY, ITPAY_APP_KEY

        request = make_mocked_request('GET', '/readyz', app={DB_APP_KEY: object(), PANEL_APP_KEY: object(), ITPAY_APP_KEY: object()})
        with patch('services.webhook.collect_health_snapshot', AsyncMock(return_value={'ok': False, 'database': False})):
            response = await readiness_handler(request)

        self.assertEqual(response.status, 503)
