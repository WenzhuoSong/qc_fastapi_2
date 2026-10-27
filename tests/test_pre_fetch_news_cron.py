import unittest
from pathlib import Path


class PreFetchNewsCronContractTests(unittest.TestCase):
    def test_pre_fetch_news_is_audited_and_documented(self):
        source = Path("cron/pre_fetch_news.py").read_text()
        readme = Path("README.md").read_text()

        self.assertIn("from services.cron_audit import audit_cron_run", source)
        self.assertIn('audit_cron_run("pre_fetch_news")', source)
        self.assertIn("audit.add_rows(result.get(\"total_new\"))", source)
        self.assertIn("audit.set_summary(**result)", source)
        self.assertIn("*/30 * * * *", source)
        self.assertIn("*/30 * * * *", readme)

    def test_slot_plan(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        from cron.pre_fetch_news import plan_run

        et = ZoneInfo("America/New_York")
        at = lambda d, h, m: plan_run(datetime(2026, 10, d, h, m, tzinfo=et))
        self.assertEqual(at(26, 5, 30), "full")          # Monday, before the pre-market brief
        self.assertEqual(at(26, 16, 45), "full")         # after the close
        self.assertEqual(at(26, 10, 30), "core")         # US session, every 30 minutes
        self.assertEqual(at(26, 22, 0), "core")          # evening: every second hour
        self.assertIsNone(at(26, 21, 30))
        self.assertIsNone(at(26, 3, 0))
        self.assertEqual(at(25, 10, 0), "core")          # Sunday: every second hour, never full
        self.assertIsNone(at(25, 16, 30))

if __name__ == "__main__":
    unittest.main()
