import unittest
from pathlib import Path

from services.feature_authority import (
    FeatureAuthority,
    QC_HEARTBEAT_SOURCE,
    YFINANCE_SOURCE,
    authority_for_field,
    source_of_truth_policy,
)
from services.market_snapshot_merge import merge_market_snapshots


REPO_ROOT = Path(__file__).resolve().parents[1]


class DataPipelineDoDTests(unittest.TestCase):
    def test_source_of_truth_policy_declares_qc_live_and_yfinance_research(self):
        policy = source_of_truth_policy()

        self.assertEqual(policy["live_state"]["source"], QC_HEARTBEAT_SOURCE)
        self.assertEqual(policy["intraday"]["source"], QC_HEARTBEAT_SOURCE)
        self.assertEqual(policy["daily_research"]["source"], YFINANCE_SOURCE)
        self.assertEqual(policy["daily_research"]["fallback_source"], "qc_daily_snapshot")
        self.assertEqual(policy["fallback_semantics"], "fallbacks may tighten risk but must not increase execution permission")

    def test_research_and_live_field_authorities_match_dod(self):
        for field in ["return_20d", "return_60d", "rsi", "atr", "hist_vol"]:
            self.assertEqual(
                authority_for_field(field, YFINANCE_SOURCE),
                FeatureAuthority.DAILY_RESEARCH,
                field,
            )

        for field in ["weight_current", "unrealized_pnl", "holding_days", "current_drawdown"]:
            self.assertEqual(
                authority_for_field(field, QC_HEARTBEAT_SOURCE),
                FeatureAuthority.LIVE_STATE,
                field,
            )

    def test_merge_freezes_canonical_fields_and_preserves_qc_legacy_debug(self):
        heartbeat = {
            "schema_version": "1.5",
            "timestamp_utc": "2026-05-22T14:00:00Z",
            "holdings": [{
                "ticker": "SPY",
                "weight_current": 0.12,
                "unrealized_pnl_pct": 0.03,
                "holding_days": 8,
                "current_drawdown_pct": -0.01,
                "mom_60d": 35.0,
                "open_price": 999.0,
                "intraday_open_price": 100.0,
                "intraday_high_price": 102.0,
                "intraday_low_price": 99.0,
                "intraday_volume": 12345,
            }],
        }
        qc_daily = {
            "timestamp_utc": "2026-05-22T00:00:00Z",
            "features": [{
                "ticker": "SPY",
                "return_60d": 0.35,
                "mom_60d": 35.0,
                "rsi_14": 80.0,
                "atr_pct": 0.05,
                "hist_vol_20d": 0.25,
                "open_price": 90.0,
                "high_price": 91.0,
                "low_price": 89.0,
                "close_price": 90.5,
                "volume": 1000,
            }],
        }
        yfinance = {
            "SPY": {
                "ticker": "SPY",
                "trading_date": "2026-05-22",
                "return_20d": 0.03,
                "return_60d": 0.08,
                "rsi_14": 55.0,
                "atr_pct": 0.02,
                "hist_vol_20d": 0.12,
                "open_price": 101.0,
                "high_price": 103.0,
                "low_price": 100.0,
                "close_price": 102.0,
                "volume": 2000,
            }
        }

        merged = merge_market_snapshots(heartbeat, qc_daily, yfinance, mode="yfinance_research")
        row = merged["holdings"][0]

        self.assertEqual(row["return_60d"], 0.08)
        self.assertEqual(row["rsi_14"], 55.0)
        self.assertEqual(row["open_price"], 101.0)
        self.assertEqual(row["intraday_open_price"], 100.0)
        self.assertEqual(row["intraday_high_price"], 102.0)
        self.assertNotIn("mom_60d", row)
        self.assertEqual(row["legacy_qc_indicators"]["mom_60d"], 35.0)
        self.assertEqual(merged["schema_capabilities"]["daily_research_authority"], "yfinance")
        self.assertEqual(merged["schema_capabilities"]["intraday_live_state"], "available")

    def test_missing_or_stale_yfinance_is_marked_as_fallback_not_silent(self):
        heartbeat = {
            "timestamp_utc": "2026-05-22T14:00:00Z",
            "holdings": [{"ticker": "SPY", "weight_current": 0.12, "price": 102.0}],
        }
        qc_daily = {
            "timestamp_utc": "2026-05-22T00:00:00Z",
            "features": [{"ticker": "SPY", "return_60d": 0.05}],
        }

        fallback = merge_market_snapshots(heartbeat, qc_daily, {}, mode="yfinance_research")
        self.assertEqual(fallback["holdings"][0]["return_60d"], 0.05)
        self.assertEqual(fallback["schema_capabilities"]["daily_research_authority"], "qc_daily_fallback")

        stale = merge_market_snapshots(
            heartbeat,
            qc_daily,
            {"SPY": {"ticker": "SPY", "return_60d": 0.99, "data_quality_flag": "stale"}},
            mode="yfinance_research",
        )
        self.assertEqual(stale["holdings"][0]["return_60d"], 0.05)
        self.assertEqual(stale["stale_yfinance_tickers"], ["SPY"])

    def test_old_schema_heartbeat_degrades_without_blocking(self):
        old_schema = merge_market_snapshots(
            {"holdings": [{"ticker": "SPY", "weight_current": 0.1, "price": 100.0}]},
            {},
            {},
            mode="yfinance_research",
        )
        self.assertEqual(old_schema["schema_capabilities"]["heartbeat_schema_version"], "legacy")
        self.assertEqual(old_schema["schema_capabilities"]["intraday_live_state"], "partial")

        no_live_price = merge_market_snapshots(
            {"holdings": [{"ticker": "SPY", "weight_current": 0.1}]},
            {},
            {},
            mode="yfinance_research",
        )
        self.assertEqual(no_live_price["schema_capabilities"]["intraday_live_state"], "unavailable")

    def test_downstream_and_observability_surfaces_are_provenance_aware(self):
        expected_markers = {
            "services/playground.py": "authority_by_field",
            "services/quant_baseline.py": "feature_authority",
            "services/sector_rotation.py": "legacy_fallback",
            "agents/risk_manager.py": "feature_source_summary",
            "services/pipeline.py": "feature_source_summary",
            "dashboard/app.py": "Feature Source Summary",
            "agents/communicator.py": "Feature source summary",
        }
        for rel_path, marker in expected_markers.items():
            text = (REPO_ROOT / rel_path).read_text(encoding="utf-8")
            self.assertIn(marker, text, rel_path)


if __name__ == "__main__":
    unittest.main()
