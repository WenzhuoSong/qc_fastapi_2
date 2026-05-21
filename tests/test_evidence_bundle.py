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
            "historical_snapshot_count": 290,
            "consensus_weights": {"SPY": 0.2, "QQQ": 0.1, "CASH": 0.7},
            "evidence_summary": {
                "historical_evidence": "strong",
                "walk_forward_validation": "medium",
                "live_fit": "insufficient",
                "execution_permission": "advisory",
            },
            "strategy_confidence": {
                "momentum_lite_v1": {
                    "confidence_score": 0.72,
                    "suggested_use": "advisory",
                    "reason_codes": ["historical_strong", "regime_fit_strong"],
                }
            },
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
            "historical_replay_metrics": {
                "momentum_lite_v1": {
                    "n_forward_return_samples": 289,
                    "metric_reliability": {"level": "high"},
                    "sharpe": 1.55,
                    "hit_rate": 0.52,
                }
            },
            "walk_forward_validation": {
                "items": {
                    "momentum_lite_v1": {
                        "level": "medium",
                        "valid_fold_count": 4,
                        "pass_rate": 0.50,
                        "stability_score": 0.68,
                    }
                },
                "execution_authority": "none",
            },
        }

        bundle = build_evidence_bundle(
            brief=brief,
            quant_baseline=quant,
            playground_bundle=playground,
            empirical_profiles={
                "SPY": {
                    "source": "yfinance",
                    "samples": 80,
                    "avg_return": 0.001,
                    "volatility": 0.01,
                    "max_drawdown": -0.04,
                    "data_quality": "fresh",
                }
            },
        )

        self.assertIn("generated_at", bundle)
        self.assertEqual(bundle["market"]["regime"], "trending_bull")
        self.assertEqual(bundle["market"]["spy_mom_20d"], 0.03)
        self.assertTrue(bundle["strategies"]["playground_available"])
        self.assertEqual(bundle["strategies"]["snapshot_count"], 30)
        self.assertEqual(bundle["strategies"]["forward_return_samples"], 12)
        self.assertEqual(bundle["strategies"]["historical_forward_return_samples"], 289)
        self.assertEqual(bundle["strategies"]["data_quality"], "historical_supported")
        self.assertEqual(
            bundle["strategies"]["strategy_results"][0]["suggested_use"],
            "advisory",
        )
        self.assertEqual(
            bundle["strategies"]["strategy_results"][0]["walk_forward_level"],
            "medium",
        )
        self.assertEqual(
            bundle["strategies"]["walk_forward_validation"]["execution_authority"],
            "none",
        )
        self.assertIn("strategy_certification", bundle["strategies"])
        self.assertEqual(
            bundle["strategies"]["strategy_certification"]["items"]["momentum_lite_v1"]["status"],
            "advisory",
        )
        self.assertEqual(
            bundle["strategies"]["strategy_results"][0]["reason_codes"],
            ["historical_strong", "regime_fit_strong"],
        )
        self.assertEqual(bundle["strategies"]["strategy_use_summary"]["actionable_count"], 1)
        self.assertEqual(bundle["strategies"]["evidence_summary"]["historical_evidence"], "strong")
        self.assertEqual(bundle["strategies"]["evidence_summary"]["execution_permission"], "advisory")
        self.assertEqual(
            bundle["strategies"]["strategy_use_summary"]["best_actionable"]["strategy_name"],
            "momentum_lite_v1",
        )
        self.assertIn("news_evidence", bundle)
        self.assertEqual(bundle["news_evidence"]["macro_news_score"]["overall_bias"], "positive")
        self.assertIn("ticker_news_scores", bundle["news_evidence"])
        self.assertTrue(bundle["knowledge"]["available"])
        self.assertIn(
            "momentum_lite_v1",
            [item["id"] for item in bundle["knowledge"]["strategies"]],
        )
        self.assertIn("SPY", [item["id"] for item in bundle["knowledge"]["assets"]])
        self.assertIn("trending_bull", [item["id"] for item in bundle["knowledge"]["regimes"]])
        self.assertIn("resolution", bundle["knowledge"])
        self.assertEqual(
            bundle["knowledge"]["resolution"]["confidence_adjustments"]["intended_consumer"],
            "strategy_confidence_calibrator",
        )
        spy_context = next(
            item for item in bundle["knowledge"]["resolution"]["advisory_context"]
            if item.get("id") == "SPY"
        )
        self.assertEqual(spy_context["empirical_behavior"]["samples"], 80)
        self.assertEqual(bundle["data_quality"]["overall"], "fresh")

    def test_calibrates_strategy_confidence_once_when_resolver_finds_conflict(self):
        bundle = build_evidence_bundle(
            brief={
                "key_facts": {},
                "sector_rotation": {},
                "news_context": {"macro_signals": []},
                "holdings": [{"ticker": "SOXL"}],
            },
            quant_baseline={
                "regime_result": {
                    "regime": "mean_reverting",
                    "confidence": "medium",
                }
            },
            playground_bundle={
                "snapshot_count": 30,
                "consensus_weights": {"SOXL": 0.2, "CASH": 0.8},
                "strategy_confidence": {
                    "momentum_lite_v1": {
                        "confidence_score": 0.60,
                        "suggested_use": "advisory",
                        "reason_codes": [],
                    }
                },
                "strategies": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "data_ready": True,
                        "risk_profile": {"turnover": 0.2},
                        "selected_tickers": ["SOXL"],
                    }
                ],
                "replay_metrics": {
                    "momentum_lite_v1": {"n_forward_return_samples": 12}
                },
            },
        )

        strategies = bundle["strategies"]
        self.assertAlmostEqual(
            strategies["strategy_confidence"]["momentum_lite_v1"]["confidence_score"],
            0.50,
        )
        self.assertAlmostEqual(
            strategies["strategy_confidence_pre_calibration"]["momentum_lite_v1"]["confidence_score"],
            0.60,
        )
        self.assertEqual(
            strategies["strategy_confidence_calibration"]["summary"]["accepted"],
            1,
        )
        self.assertIn(
            "regime_strategy_conflict",
            [item["id"] for item in bundle["knowledge"]["resolution"]["conflicts"]],
        )

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
        self.assertEqual(bundle["news_evidence"]["macro_news_score"]["data_quality"], "limited")

    def test_accepts_prebuilt_news_evidence(self):
        prebuilt_news = {
            "macro_news_score": {
                "overall_bias": "negative",
                "confidence": "high",
                "dominant_themes": ["credit stress"],
                "market_impact": "high",
                "time_horizon": "intraday",
                "data_quality": "fresh",
                "warnings": [],
            },
            "ticker_news_scores": {
                "XLF": {
                    "bias": "negative",
                    "action_bias": "block_new_buy",
                    "effective_credibility": 0.9,
                    "supporting_items": [],
                    "conflicting_items": [],
                }
            },
            "hard_risk_events": {"XLF": ["credit_stress"]},
            "ignored_items": [],
            "data_gaps": [],
        }

        bundle = build_evidence_bundle(
            brief={
                "key_facts": {},
                "sector_rotation": {},
                "news_context": {"macro_signals": [{"direction": "positive"}]},
                "holdings": [{"ticker": "SPY"}],
            },
            quant_baseline={"regime_result": {"regime": "unknown", "confidence": "low"}},
            playground_bundle=None,
            news_evidence=prebuilt_news,
        )

        self.assertIs(bundle["news_evidence"], prebuilt_news)
        self.assertEqual(bundle["news_evidence"]["macro_news_score"]["overall_bias"], "negative")
        self.assertEqual(
            bundle["knowledge"]["computed_facts_available"],
            {
                "news_evidence": True,
                "scorecard": False,
                "position_governance": False,
                "empirical_profiles": False,
            },
        )
        self.assertEqual(
            bundle["knowledge"]["resolution"]["computed_facts_summary"]["news_evidence"]["overall_bias"],
            "negative",
        )
        self.assertEqual(
            bundle["knowledge"]["resolution"]["computed_facts_summary"]["news_evidence"]["hard_risk_tickers"],
            ["XLF"],
        )

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
