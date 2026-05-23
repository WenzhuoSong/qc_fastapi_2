import unittest
from datetime import date

from services.market_snapshot_merge import merge_market_snapshots
from services.strategy_feature_contract import build_strategy_feature_contract
from strategies import get_strategy
from tools.audit_qc_yfinance_features import build_markdown_report, build_summary


class DataPipelineFeatureMatrixTests(unittest.TestCase):
    def test_only_heartbeat_runs_but_strategy_evidence_degrades(self):
        merged = merge_market_snapshots(
            {
                "packet_type": "heartbeat",
                "holdings": [
                    {
                        "ticker": "SPY",
                        "weight_current": 0.1,
                        "mom_20d": 0.02,
                        "mom_60d": 0.05,
                        "mom_252d": 0.12,
                        "rsi_14": 55,
                        "atr_pct": 0.011,
                    }
                ],
            },
            {},
            {},
        )
        contract = build_strategy_feature_contract(
            get_strategy("momentum_lite_v1"),
            merged["holdings"],
            as_of=date(2026, 5, 14),
        )

        self.assertEqual(len(merged["holdings"]), 1)
        self.assertEqual(merged["schema_capabilities"]["daily_research_authority"], "missing")
        self.assertFalse(contract["can_influence_allocation"])
        self.assertIn(contract["verdict"], {"blocked_missing_required_fields", "blocked_non_authoritative_required_fields"})

    def test_heartbeat_plus_yfinance_keeps_live_state_and_uses_research_layer(self):
        merged = merge_market_snapshots(
            {
                "packet_type": "heartbeat",
                "schema_version": "1.5",
                "holdings": [
                    {
                        "ticker": "SPY",
                        "weight_current": 0.1,
                        "mom_20d": 99.0,
                        "mom_60d": 99.0,
                        "mom_252d": 99.0,
                    }
                ],
            },
            {},
            {
                "SPY": {
                    "ticker": "SPY",
                    "return_20d": 0.02,
                    "return_60d": 0.05,
                    "return_252d": 0.12,
                    "rsi_14": 55,
                    "atr_pct": 0.011,
                    "trading_date": "2026-05-14",
                }
            },
        )
        spy = merged["holdings"][0]

        self.assertEqual(spy["weight_current"], 0.1)
        self.assertEqual(spy["return_20d"], 0.02)
        self.assertEqual(spy["return_60d"], 0.05)
        self.assertEqual(spy["rsi_14"], 55)
        self.assertEqual(spy["atr_pct"], 0.011)
        self.assertNotIn("mom_20d", spy)
        self.assertEqual(spy["legacy_qc_indicators"]["mom_20d"], 99.0)

    def test_stale_yfinance_with_qc_daily_fallback_cannot_promote_strategy(self):
        merged = merge_market_snapshots(
            {"packet_type": "heartbeat", "holdings": [{"ticker": "SPY", "weight_current": 0.1}]},
            {
                "packet_type": "daily_feature_snapshot",
                "features": [
                    {
                        "ticker": "SPY",
                        "mom_20d": 0.02,
                        "mom_60d": 0.05,
                        "mom_252d": 0.12,
                        "rsi_14": 55,
                        "atr_pct": 0.011,
                    }
                ],
            },
            {
                "SPY": {
                    "ticker": "SPY",
                    "return_20d": 0.99,
                    "return_60d": 0.99,
                    "return_252d": 0.99,
                    "rsi_14": 99,
                    "atr_pct": 0.99,
                    "data_quality_flag": "stale",
                    "trading_date": "2026-05-01",
                }
            },
        )
        contract = build_strategy_feature_contract(
            get_strategy("momentum_lite_v1"),
            merged["holdings"],
            as_of=date(2026, 5, 14),
        )
        spy = merged["holdings"][0]

        self.assertEqual(spy["return_60d"], 0.05)
        self.assertEqual(merged["schema_capabilities"]["daily_research_authority"], "qc_daily_fallback")
        self.assertEqual(merged["stale_yfinance_tickers"], ["SPY"])
        self.assertFalse(contract["can_influence_allocation"])
        self.assertEqual(contract["verdict"], "blocked_non_authoritative_required_fields")

    def test_old_schema_heartbeat_degrades_without_crashing(self):
        merged = merge_market_snapshots(
            {
                "packet_type": "heartbeat",
                "schema_version": "1.4",
                "holdings": [{"ticker": "SPY", "price": 630.0, "mom_60d": 99.0}],
            },
            {},
            {},
        )
        spy = merged["holdings"][0]

        self.assertEqual(merged["schema_capabilities"]["intraday_live_state"], "partial")
        self.assertNotIn("mom_60d", spy)
        self.assertEqual(spy["legacy_qc_indicators"]["mom_60d"], 99.0)

    def test_levered_etf_audit_is_high_drift_not_system_failure(self):
        summary = build_summary(
            [
                {
                    "packet_type": "heartbeat",
                    "ticker_role": "hedge_levered",
                    "joined_rows": 3,
                    "n_mom_60d": 3,
                    "mae_mom_60d": 0.30,
                    "mae_mom_60d_norm": 0.28,
                }
            ],
            lookback_days=45,
        )
        report = build_markdown_report(summary)

        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["high_drift_classes"][0]["ticker_role"], "hedge_levered")
        self.assertIn("High Drift Classes", report)


if __name__ == "__main__":
    unittest.main()
