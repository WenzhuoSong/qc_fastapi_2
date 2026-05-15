import unittest

from services.evidence_bundle import build_evidence_bundle


class EvidenceBundleTest(unittest.TestCase):
    def test_builds_bundle_with_playground_data(self):
        brief = {
            "key_facts": {
                "breadth_pct": 0.65,
                "spy_mom_60d": 0.07,
                "avg_atr_pct": 0.014,
                "risk_on_score": 0.03,
                "drawdown_pct": 0.02,
                "top5_momentum": ["XLK", "QQQ"],
                "bottom5_momentum": ["TLT"],
                "n_etfs": 12,
            },
            "portfolio": {"vix": 18.0},
            "sector_rotation": {
                "rotation_label": "risk_on_rotation",
                "risk_appetite_score": 0.03,
            },
            "news_context": {
                "processed_at": "2026-05-15T09:30:00+00:00",
                "macro_signals": [{"direction": "positive"}],
                "ticker_signals": {},
            },
            "per_ticker_news": {"SPY": [{"headline": "x"}]},
            "hard_risks_map": {},
            "memory_context": {
                "has_memory": True,
                "regime_trend": "Past days consistently trending_bull",
            },
            "holdings": [{"ticker": "SPY"}],
        }
        quant = {
            "regime_result": {
                "regime": "trending_bull",
                "confidence": "medium",
                "reasoning": "Bull trend",
                "signals": {
                    "spy_mom_20d": 0.03,
                    "spy_mom_60d": 0.07,
                    "spy_rsi": 61,
                    "vix": 18,
                    "drawdown": 0.02,
                },
            }
        }
        playground = {
            "generated_at": "2026-05-15T10:00:00+00:00",
            "regime_label": "trending_bull",
            "regime_confidence": "medium",
            "snapshot_count": 30,
            "consensus_weights": {"SPY": 0.2, "QQQ": 0.1, "CASH": 0.7},
            "strategies": [
                {
                    "strategy_name": "momentum_lite_v1",
                    "data_ready": True,
                    "feature_contract": {"can_influence_allocation": True},
                    "risk_profile": {"turnover": 0.25},
                    "selected_tickers": ["SPY", "QQQ"],
                }
            ],
            "replay_metrics": {
                "momentum_lite_v1": {
                    "n_forward_return_samples": 12,
                    "metric_reliability": {"level": "medium"},
                }
            },
        }

        bundle = build_evidence_bundle(
            brief=brief,
            quant_baseline=quant,
            playground_bundle=playground,
        )

        self.assertIn("generated_at", bundle)
        self.assertEqual(bundle["market"]["regime"], "trending_bull")
        self.assertEqual(bundle["market"]["spy_mom_20d"], 0.03)
        self.assertTrue(bundle["strategies"]["playground_available"])
        self.assertEqual(bundle["strategies"]["snapshot_count"], 30)
        self.assertEqual(bundle["strategies"]["forward_return_samples"], 12)
        self.assertEqual(bundle["strategies"]["data_quality"], "fresh")
        self.assertEqual(bundle["data_quality"]["overall"], "fresh")

    def test_missing_playground_uses_fallback_and_marks_missing_quality(self):
        bundle = build_evidence_bundle(
            brief={
                "key_facts": {},
                "sector_rotation": {},
                "news_context": {},
                "holdings": [{"ticker": "SPY"}],
            },
            quant_baseline={"regime_result": {"regime": "unknown", "confidence": "low"}},
            playground_bundle=None,
        )

        strategies = bundle["strategies"]
        self.assertFalse(strategies["playground_available"])
        self.assertEqual(strategies["snapshot_count"], 0)
        self.assertEqual(strategies["forward_return_samples"], 0)
        self.assertEqual(strategies["data_quality"], "missing")
        self.assertIn("No recent Playground result available", strategies["warnings"][0])
        self.assertEqual(bundle["data_quality"]["overall"], "missing")

    def test_high_turnover_strategy_adds_warning(self):
        bundle = build_evidence_bundle(
            brief={
                "key_facts": {},
                "sector_rotation": {},
                "news_context": {"macro_signals": []},
                "holdings": [{"ticker": "SPY"}],
            },
            quant_baseline={"regime_result": {"regime": "trending_bull", "confidence": "medium"}},
            playground_bundle={
                "snapshot_count": 30,
                "consensus_weights": {"SPY": 0.2, "CASH": 0.8},
                "strategies": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "data_ready": True,
                        "risk_profile": {"turnover": 0.67},
                    }
                ],
                "replay_metrics": {
                    "momentum_lite_v1": {"n_forward_return_samples": 12}
                },
            },
        )

        self.assertTrue(bundle["strategies"]["turnover_warnings"])
        self.assertIn("may erode returns", bundle["strategies"]["warnings"][0])


if __name__ == "__main__":
    unittest.main()
