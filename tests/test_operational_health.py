import unittest
from datetime import datetime, timedelta

from services.operational_health import (
    _freshness_check,
    _heartbeat_freshness_check,
    _trading_day_freshness_check,
    classify_operational_health,
    format_operational_health_report,
)


class OperationalHealthTests(unittest.TestCase):
    def test_classifies_execution_blocker_vs_research_degradation(self):
        now = datetime(2026, 5, 15, 12, 0, 0)
        checks = {
            "qc_heartbeat": _freshness_check(
                label="QC heartbeat",
                timestamp=now - timedelta(hours=3),
                now=now,
                max_age_hours=2,
                blocker=True,
                missing_blocker=True,
            ),
            "news_cache": _freshness_check(
                label="News cache",
                timestamp=now - timedelta(hours=8),
                now=now,
                max_age_hours=6,
                blocker=False,
                missing_blocker=False,
            ),
            "pipeline_status": {"label": "Pipeline", "status": "semi_auto_pending"},
        }

        snapshot = classify_operational_health(checks, [], now=now)

        self.assertEqual(snapshot["overall"], "execution_blocked")
        self.assertIn("QC heartbeat", snapshot["execution_blockers"][0])
        self.assertIn("News cache", snapshot["research_degradations"][0])
        self.assertFalse(any("Pipeline" in item for item in snapshot["research_degradations"]))

    def test_pipeline_pending_without_state_does_not_degrade_health(self):
        now = datetime(2026, 5, 19, 13, 0, 0)
        checks = {
            "qc_heartbeat": {"label": "QC heartbeat", "state": "ok", "age_hours": 17.3},
            "daily_feature_snapshot": {"label": "Daily features", "state": "ok", "age_hours": 16.9},
            "yfinance_backfill": {"label": "YFinance backfill", "state": "ok", "age_hours": 16.6},
            "news_cache": {"label": "News cache", "state": "ok", "age_hours": 1.1},
            "memory_write": {"label": "Memory write", "state": "ok", "age_hours": 15.2},
            "pipeline_status": {"label": "Pipeline", "status": "pending"},
        }

        snapshot = classify_operational_health(checks, [], now=now)

        self.assertEqual(snapshot["overall"], "healthy")
        self.assertEqual(snapshot["research_degradations"], [])

    def test_formats_short_telegram_report(self):
        snapshot = {
            "overall": "research_degraded",
            "checks": {
                "qc_heartbeat": {"label": "QC heartbeat", "state": "ok", "age_hours": 0.5},
                "daily_feature_snapshot": {"label": "Daily features", "state": "ok", "age_hours": 10.0},
                "yfinance_backfill": {"label": "YFinance backfill", "state": "stale", "age_hours": 40.0},
                "news_cache": {"label": "News cache", "state": "ok", "age_hours": 2.0},
                "memory_write": {"label": "Memory write", "state": "ok", "age_hours": 20.0},
                "pipeline_status": {"status": "pending"},
            },
            "execution_blockers": [],
            "research_degradations": ["YFinance backfill: stale 40.0h > 36h"],
        }

        report = format_operational_health_report(snapshot)

        self.assertIn("Ops health: research degraded", report)
        self.assertIn("YFinance backfill", report)
        self.assertLessEqual(len(report.splitlines()), 12)

    def test_heartbeat_stale_is_not_blocking_during_opening_grace(self):
        now = datetime(2026, 5, 15, 13, 31, 0)  # 09:31 ET

        check = _heartbeat_freshness_check(
            label="QC heartbeat",
            timestamp=now - timedelta(hours=17.3),
            now=now,
            max_age_hours=2,
            blocker=True,
            missing_blocker=True,
        )

        self.assertEqual(check["state"], "ok")
        self.assertFalse(check["blocking"])
        self.assertEqual(check["reason"], "opening grace")

    def test_heartbeat_stale_blocks_during_regular_market_hours(self):
        now = datetime(2026, 5, 15, 15, 0, 0)  # 11:00 ET

        check = _heartbeat_freshness_check(
            label="QC heartbeat",
            timestamp=now - timedelta(hours=3),
            now=now,
            max_age_hours=2,
            blocker=True,
            missing_blocker=True,
        )

        self.assertEqual(check["state"], "stale")
        self.assertTrue(check["blocking"])

    def test_trading_day_freshness_allows_friday_data_before_monday_open(self):
        now = datetime(2026, 5, 18, 13, 2, 0)  # Monday 09:02 ET
        friday_after_close = datetime(2026, 5, 15, 20, 30, 0)  # Friday 16:30 ET

        check = _trading_day_freshness_check(
            label="YFinance backfill",
            timestamp=friday_after_close,
            now=now,
            max_age_hours=36,
            blocker=False,
            missing_blocker=False,
        )

        self.assertGreater(check["age_hours"], 36)
        self.assertEqual(check["state"], "ok")
        self.assertEqual(check["reason"], "trading calendar grace")
        self.assertEqual(check["expected_research_date"], "2026-05-15")
        self.assertFalse(check["blocking"])

    def test_trading_day_freshness_marks_older_session_stale_before_monday_open(self):
        now = datetime(2026, 5, 18, 13, 2, 0)  # Monday 09:02 ET
        thursday_after_close = datetime(2026, 5, 14, 20, 30, 0)  # Thursday 16:30 ET

        check = _trading_day_freshness_check(
            label="Memory write",
            timestamp=thursday_after_close,
            now=now,
            max_age_hours=36,
            blocker=False,
            missing_blocker=False,
        )

        self.assertEqual(check["state"], "stale")
        self.assertIn("stale", check["reason"])

    def test_trading_day_freshness_requires_current_session_after_close(self):
        now = datetime(2026, 5, 18, 21, 0, 0)  # Monday 17:00 ET
        friday_after_close = datetime(2026, 5, 15, 20, 30, 0)  # Friday 16:30 ET

        check = _trading_day_freshness_check(
            label="Daily features",
            timestamp=friday_after_close,
            now=now,
            max_age_hours=36,
            blocker=False,
            missing_blocker=False,
        )

        self.assertEqual(check["state"], "stale")
        self.assertIn("stale", check["reason"])


if __name__ == "__main__":
    unittest.main()
