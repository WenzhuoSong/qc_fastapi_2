import unittest
from datetime import datetime, timedelta

from services.operational_health import (
    _freshness_check,
    _heartbeat_freshness_check,
    _news_cache_freshness_check,
    _trading_day_freshness_check,
    _yfinance_ticker_health_check,
    classify_operational_health,
    format_operational_health_report,
)


class OperationalHealthTests(unittest.TestCase):
    def test_yfinance_ticker_health_reports_each_etf(self):
        class Row:
            def __init__(self, ticker, trading_date, **values):
                self.ticker = ticker
                self.trading_date = trading_date
                self.data_quality_flag = values.pop("data_quality_flag", "ok")
                for key, value in values.items():
                    setattr(self, key, value)

        now = datetime(2026, 5, 26, 21, 0, 0)  # Tuesday after close ET
        full_row = {
            "close_price": 100,
            "return_1d": 0.01,
            "return_20d": 0.05,
            "hist_vol_20d": 0.02,
            "rsi_14": 55,
            "atr_pct": 0.01,
            "beta_vs_spy": 1.0,
            "return_60d": 0.08,
            "return_252d": 0.2,
            "sma_200": 95,
        }
        check = _yfinance_ticker_health_check(
            universe=["SPY", "DRAM", "MISSING"],
            latest_rows={
                "SPY": Row("SPY", datetime(2026, 5, 26).date(), **full_row),
                "DRAM": Row(
                    "DRAM",
                    datetime(2026, 5, 26).date(),
                    **{**full_row, "return_60d": None, "return_252d": None, "sma_200": None},
                ),
            },
            stats_by_ticker={
                "SPY": {"row_count": 300, "first_date": datetime(2025, 1, 1).date()},
                "DRAM": {"row_count": 37, "first_date": datetime(2026, 4, 2).date()},
            },
            now=now,
        )

        self.assertEqual(check["ticker_count"], 3)
        self.assertEqual(check["issue_count"], 1)
        self.assertEqual(check["insufficient_history_count"], 1)
        dram = next(row for row in check["rows"] if row["ticker"] == "DRAM")
        self.assertEqual(dram["state"], "ok")
        self.assertEqual(dram["history_status"], "insufficient_history")
        missing = next(row for row in check["rows"] if row["ticker"] == "MISSING")
        self.assertEqual(missing["state"], "missing")

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

    def test_heartbeat_stale_is_not_blocking_on_market_holiday(self):
        now = datetime(2026, 5, 25, 15, 0, 0)  # Memorial Day 11:00 ET

        check = _heartbeat_freshness_check(
            label="QC heartbeat",
            timestamp=now - timedelta(hours=65),
            now=now,
            max_age_hours=2,
            blocker=True,
            missing_blocker=True,
        )

        self.assertEqual(check["state"], "ok")
        self.assertFalse(check["blocking"])
        self.assertEqual(check["reason"], "market closed: Memorial Day")

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

    def test_trading_day_freshness_allows_friday_data_on_memorial_day(self):
        now = datetime(2026, 5, 25, 15, 0, 0)  # Memorial Day 11:00 ET
        friday_after_close = datetime(2026, 5, 22, 20, 30, 0)  # Friday 16:30 ET

        check = _trading_day_freshness_check(
            label="Daily features",
            timestamp=friday_after_close,
            now=now,
            max_age_hours=36,
            blocker=False,
            missing_blocker=False,
        )

        self.assertGreater(check["age_hours"], 36)
        self.assertEqual(check["state"], "ok")
        self.assertEqual(check["expected_research_date"], "2026-05-22")
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

    def test_news_cache_freshness_uses_24_7_cron_schedule(self):
        now = datetime(2026, 6, 8, 2, 44, 0)
        recent_news = datetime(2026, 6, 8, 0, 51, 0)

        check = _news_cache_freshness_check(
            label="News cache",
            timestamp=recent_news,
            now=now,
            max_age_hours=6,
            blocker=False,
            missing_blocker=False,
        )

        self.assertEqual(check["state"], "ok")
        self.assertEqual(check["freshness_policy"], "24_7_event_stream")
        self.assertEqual(check["expected_schedule"], "50 */2 * * * UTC")
        self.assertEqual(check["latest_expected_run_at"], "2026-06-08T00:50:00")
        self.assertEqual(check["next_expected_run_at"], "2026-06-08T02:50:00")
        self.assertEqual(check["missed_scheduled_runs"], 0)

    def test_news_cache_freshness_stales_after_missed_24_7_runs(self):
        now = datetime(2026, 6, 8, 10, 0, 0)
        old_news = datetime(2026, 6, 8, 2, 51, 0)

        check = _news_cache_freshness_check(
            label="News cache",
            timestamp=old_news,
            now=now,
            max_age_hours=12,
            blocker=True,
            missing_blocker=False,
        )

        self.assertEqual(check["state"], "stale")
        self.assertTrue(check["blocking"])
        self.assertEqual(check["missed_scheduled_runs"], 3)
        self.assertIn("missed 3 scheduled news runs", check["reason"])


if __name__ == "__main__":
    unittest.main()
