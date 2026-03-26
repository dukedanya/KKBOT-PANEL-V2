import unittest
from unittest.mock import patch


class BackgroundJobsTests(unittest.TestCase):
    def test_build_job_specs_respects_feature_flags(self):
        from app.background import build_job_specs

        with patch("app.background.Config.ENABLE_HEALTH_MONITOR_JOB", False), \
             patch("app.background.Config.ENABLE_PAYMENT_RECONCILE_JOB", True):
            jobs = {job.name: job.enabled for job in build_job_specs()}

        self.assertFalse(jobs["health_monitor"])
        self.assertTrue(jobs["reconcile_itpay_payments"])


if __name__ == "__main__":
    unittest.main()
