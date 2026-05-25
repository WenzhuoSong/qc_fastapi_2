import unittest

from services.strategy_diversity import (
    build_strategy_diversity_summary,
    canonical_strategy_family,
    is_strategy_alpha_source,
)


class StrategyDiversityTest(unittest.TestCase):
    def test_canonicalizes_legacy_family_names(self):
        self.assertEqual(canonical_strategy_family("trend_following"), "momentum")
        self.assertEqual(canonical_strategy_family("dual_momentum"), "momentum")
        self.assertEqual(canonical_strategy_family("leveraged_rotation"), "momentum")
        self.assertEqual(canonical_strategy_family("sector_theme_rotation"), "momentum")
        self.assertEqual(canonical_strategy_family("macro_rate"), "carry_or_cash_proxy")
        self.assertEqual(canonical_strategy_family("macro_cycle_rotation"), "macro_regime")
        self.assertEqual(canonical_strategy_family("defensive_factor"), "low_vol_defensive")
        self.assertEqual(canonical_strategy_family("seasonality_flow"), "seasonality_flow")

    def test_non_alpha_benchmarks_are_not_counted(self):
        self.assertFalse(is_strategy_alpha_source("equal_weight_benchmark", "benchmark"))
        self.assertFalse(is_strategy_alpha_source("risk_parity_lite", "risk_budgeting"))

        summary = build_strategy_diversity_summary([
            {
                "strategy_name": "risk_parity_lite",
                "raw_family": "risk_budgeting",
                "canonical_family": "low_vol_defensive",
                "alpha_source": False,
                "suggested_use": "advisory",
            }
        ])

        self.assertEqual(summary["independent_alpha_family_count"], 0)
        self.assertEqual(summary["alpha_source_strategy_count"], 0)
        self.assertEqual(summary["actionable_alpha_strategy_count"], 0)

    def test_same_family_variants_count_as_one_independent_alpha_source(self):
        summary = build_strategy_diversity_summary([
            {
                "strategy_name": "momentum_lite_v1",
                "raw_family": "trend_following",
                "suggested_use": "advisory",
                "confidence_score": 0.75,
                "data_ready": True,
            },
            {
                "strategy_name": "dual_momentum_rotation",
                "raw_family": "dual_momentum",
                "suggested_use": "primary",
                "confidence_score": 0.82,
                "data_ready": True,
            },
            {
                "strategy_name": "mean_reversion_lite",
                "raw_family": "mean_reversion",
                "suggested_use": "watch_only",
                "confidence_score": 0.41,
                "data_ready": True,
            },
        ])

        self.assertEqual(summary["independent_alpha_family_count"], 1)
        self.assertEqual(summary["actionable_alpha_families"], ["momentum"])
        self.assertIn(
            "same_family_not_independent:momentum:dual_momentum_rotation,momentum_lite_v1",
            summary["warnings"],
        )
        momentum = next(row for row in summary["family_rows"] if row["family"] == "momentum")
        self.assertEqual(momentum["actionable_alpha_strategy_count"], 2)
        self.assertTrue(momentum["independent_alpha_counted"])


if __name__ == "__main__":
    unittest.main()
