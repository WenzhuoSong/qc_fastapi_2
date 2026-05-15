import unittest
from datetime import datetime, timedelta

from services.operational_health import (
    _freshness_check,
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


if __name__ == "__main__":
    unittest.main()
