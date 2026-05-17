import unittest

from services.knowledge_base import build_knowledge_context
from services.knowledge_resolver import resolve_knowledge
from services.strategy_confidence_calibrator import calibrate_strategy_confidence


class KnowledgeResolverTest(unittest.TestCase):
    def test_resolver_emits_mvp_constraints_and_conflict(self):
        context = build_knowledge_context(
            tickers=["SOXL", "QQQ"],
            strategy_names=["momentum_lite_v1"],
            regime="mean_reverting",
            reason_codes=["high_atr"],
        )

        resolution = resolve_knowledge(
            knowledge_context=context,
            computed_facts={"market": {"regime": "mean_reverting"}},
        )

        constraint_ids = [item["id"] for item in resolution["hard_constraints"]]
        self.assertIn("high_atr_no_add", constraint_ids)
        self.assertIn("leveraged_etf_caution", constraint_ids)
        conflict_ids = [item["id"] for item in resolution["conflicts"]]
        self.assertIn("regime_strategy_conflict", conflict_ids)
        self.assertEqual(
            resolution["confidence_adjustments"]["intended_consumer"],
            "strategy_confidence_calibrator",
        )
        self.assertEqual(
            resolution["confidence_adjustments"]["items"][0]["target"],
            "momentum_lite_v1",
        )

    def test_missing_strategy_is_blocking(self):
        context = build_knowledge_context(
            tickers=["QQQ"],
            strategy_names=["unknown_strategy"],
            regime="trending_bull",
        )

        resolution = resolve_knowledge(knowledge_context=context)

        strategy_missing = next(
            item for item in resolution["missing_knowledge"]
            if item["kind"] == "strategy_profile"
        )
        self.assertEqual(strategy_missing["severity"], "blocking")

    def test_resolver_merges_empirical_profile_and_warns_when_missing(self):
        context = build_knowledge_context(
            tickers=["QQQ", "TLT"],
            strategy_names=[],
            regime="trending_bull",
        )

        resolution = resolve_knowledge(
            knowledge_context=context,
            computed_facts={
                "empirical_profiles": {
                    "QQQ": {
                        "source": "yfinance",
                        "samples": 80,
                        "avg_return": 0.001,
                        "volatility": 0.02,
                        "max_drawdown": -0.08,
                        "correlation_top": {"XLK": 0.9},
                        "data_quality": "fresh",
                    }
                }
            },
        )

        qqq_context = next(
            item for item in resolution["advisory_context"]
            if item.get("id") == "QQQ"
        )
        self.assertEqual(qqq_context["empirical_behavior"]["samples"], 80)
        self.assertEqual(qqq_context["empirical_behavior"]["correlation_top"], {"XLK": 0.9})
        missing_ids = {
            item["id"] for item in resolution["missing_knowledge"]
            if item["kind"] == "empirical_profile"
        }
        self.assertIn("TLT", missing_ids)


class StrategyConfidenceCalibratorTest(unittest.TestCase):
    def test_calibrator_applies_adjustment_once(self):
        strategy_confidence = {
            "momentum_lite_v1": {
                "confidence_score": 0.60,
                "suggested_use": "advisory",
                "reason_codes": [],
            }
        }
        resolution = {
            "confidence_adjustments": {
                "intended_consumer": "strategy_confidence_calibrator",
                "items": [
                    {
                        "target_type": "strategy",
                        "target": "momentum_lite_v1",
                        "delta": -0.10,
                        "max_abs_delta": 0.15,
                        "reason": "regime_strategy_conflict",
                    },
                    {
                        "target_type": "strategy",
                        "target": "momentum_lite_v1",
                        "delta": -0.05,
                        "max_abs_delta": 0.15,
                        "reason": "duplicate",
                    },
                ],
            },
            "missing_knowledge": [],
        }

        out = calibrate_strategy_confidence(
            strategy_confidence=strategy_confidence,
            knowledge_resolution=resolution,
        )

        row = out["strategy_confidence"]["momentum_lite_v1"]
        self.assertAlmostEqual(row["confidence_score"], 0.50)
        self.assertAlmostEqual(row["confidence_score_pre_calibration"], 0.60)
        self.assertEqual(out["summary"]["accepted"], 1)
        self.assertEqual(out["summary"]["rejected"], 1)
        self.assertEqual(out["records"][1]["rejection_reason"], "duplicate_target_adjustment")

    def test_calibrator_rejects_when_blocking_missing_knowledge(self):
        out = calibrate_strategy_confidence(
            strategy_confidence={"momentum_lite_v1": {"confidence_score": 0.60}},
            knowledge_resolution={
                "confidence_adjustments": {
                    "intended_consumer": "strategy_confidence_calibrator",
                    "items": [
                        {
                            "target_type": "strategy",
                            "target": "momentum_lite_v1",
                            "delta": -0.10,
                            "max_abs_delta": 0.15,
                            "reason": "regime_strategy_conflict",
                        }
                    ],
                },
                "missing_knowledge": [{"severity": "blocking"}],
            },
        )

        self.assertAlmostEqual(out["strategy_confidence"]["momentum_lite_v1"]["confidence_score"], 0.60)
        self.assertEqual(out["records"][0]["status"], "rejected")
        self.assertEqual(out["records"][0]["rejection_reason"], "blocking_missing_knowledge")


if __name__ == "__main__":
    unittest.main()
