import unittest
from datetime import date

from services.strategy_feature_contract import build_strategy_feature_contract
from strategies import get_strategy


class StrategyFeatureContractTest(unittest.TestCase):
    def test_contract_reports_ready_with_provenance(self):
        strategy = get_strategy("momentum_lite_v1")
        holdings = [
            {
                "ticker": "SPY",
                "mom_20d": 0.02,
                "mom_60d": 0.05,
                "mom_252d": 0.12,
                "rsi_14": 55,
                "atr_pct": 0.011,
                "feature_sources": [
                    {
                        "source": "yfinance",
                        "filled_fields": ["mom_252d"],
                        "trading_date": "2026-05-13",
                    }
                ],
            },
            {
                "ticker": "QQQ",
                "mom_20d": 0.04,
                "mom_60d": 0.08,
                "mom_252d": 0.20,
                "rsi_14": 68,
                "atr_pct": 0.018,
            },
        ]

        contract = build_strategy_feature_contract(
            strategy,
            holdings,
            as_of=date(2026, 5, 14),
        )

        self.assertTrue(contract["ready"])
        self.assertTrue(contract["can_influence_allocation"])
        self.assertEqual(contract["verdict"], "ready")
        mom252 = next(item for item in contract["field_contracts"] if item["field"] == "mom_252d")
        self.assertEqual(mom252["coverage"], 1.0)
        self.assertEqual(mom252["source_counts"]["yfinance"], 1)
        self.assertEqual(mom252["source_counts"]["qc_snapshot"], 1)
        self.assertEqual(mom252["freshness"], "fresh")

    def test_contract_blocks_missing_required_fields(self):
        strategy = get_strategy("momentum_lite_v1")
        contract = build_strategy_feature_contract(
            strategy,
            [
                {
                    "ticker": "SPY",
                    "mom_20d": 0.02,
                    "mom_60d": 0.05,
                    "rsi_14": 55,
                    "atr_pct": 0.011,
                },
                {
                    "ticker": "QQQ",
                    "mom_20d": 0.04,
                    "mom_60d": 0.08,
                    "rsi_14": 68,
                    "atr_pct": 0.018,
                },
            ],
            as_of=date(2026, 5, 14),
        )

        self.assertFalse(contract["ready"])
        self.assertFalse(contract["can_influence_allocation"])
        self.assertEqual(contract["verdict"], "blocked_missing_required_fields")
        self.assertIn("mom_252d", contract["missing_required_fields"])

    def test_contract_blocks_stale_required_yfinance_fields(self):
        strategy = get_strategy("low_vol_factor")
        holdings = [
            {
                "ticker": "SPY",
                "hist_vol_20d": 0.14,
                "atr_pct": 0.011,
                "mom_252d": 0.12,
                "feature_sources": [
                    {
                        "source": "yfinance",
                        "filled_fields": ["hist_vol_20d", "mom_252d"],
                        "trading_date": "2026-05-01",
                    }
                ],
            },
            {
                "ticker": "QQQ",
                "hist_vol_20d": 0.22,
                "atr_pct": 0.018,
                "mom_252d": 0.20,
                "feature_sources": [
                    {
                        "source": "yfinance",
                        "filled_fields": ["hist_vol_20d", "mom_252d"],
                        "trading_date": "2026-05-01",
                    }
                ],
            },
        ]

        contract = build_strategy_feature_contract(
            strategy,
            holdings,
            as_of=date(2026, 5, 14),
            stale_after_days=5,
        )

        self.assertFalse(contract["ready"])
        self.assertFalse(contract["can_influence_allocation"])
        self.assertEqual(contract["verdict"], "blocked_stale_required_fields")
        self.assertIn("hist_vol_20d", contract["stale_required_fields"])
        self.assertIn("mom_252d", contract["stale_required_fields"])


if __name__ == "__main__":
    unittest.main()
