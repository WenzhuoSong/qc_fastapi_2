import unittest

from services.strategy_breadth_calibration import build_strategy_breadth_calibration_report


class StrategyBreadthCalibrationTests(unittest.TestCase):
    def test_separates_duplicate_and_diversifying_pairs(self):
        report = build_strategy_breadth_calibration_report(_independence_summary())

        self.assertEqual(report["report_version"], "strategy_breadth_calibration_v1")
        self.assertEqual(report["execution_authority"], "none")
        self.assertEqual(report["target_weight_mutation"], "none")
        self.assertEqual(report["total_strategies"], 4)
        self.assertEqual(report["eligible_alpha_strategy_count"], 4)
        self.assertEqual(report["estimated_independent_clusters"], 3)
        self.assertEqual(report["duplication_ratio"], 0.25)
        self.assertEqual(report["high_correlation_pairs"][0]["a"], "absolute_trend_following_lite")
        self.assertEqual(report["high_correlation_pairs"][0]["b"], "momentum_lite_v1")
        self.assertEqual(report["diversifying_pairs"][0]["a"], "momentum_lite_v1")
        self.assertEqual(report["diversifying_pairs"][0]["b"], "mean_reversion_lite")
        self.assertTrue(report["estimated_breadth_is_approximation"])
        self.assertFalse(report["active_basket_input"]["use_as_execution_authority"])

    def test_insufficient_overlap_pairs_do_not_create_fake_clusters(self):
        raw = _independence_summary()
        raw["pair_rows"] = [
            {
                "left": "momentum_lite_v1",
                "right": "mean_reversion_lite",
                "overlap": 12,
                "correlation": None,
                "status": "insufficient_overlap",
            }
        ]

        report = build_strategy_breadth_calibration_report(raw)

        self.assertEqual(report["status"], "insufficient_overlap")
        self.assertEqual(report["insufficient_overlap_pairs"], 1)
        self.assertEqual(report["estimated_independent_clusters"], 4)
        self.assertEqual(report["high_correlation_pairs"], [])
        self.assertEqual(report["diversifying_pairs"], [])


def _independence_summary():
    return {
        "contract_version": "strategy_independence_diagnostics_v1",
        "status": "available",
        "min_overlap": 30,
        "strategy_count": 4,
        "alpha_strategy_count": 4,
        "strategy_rows": [
            {"strategy_name": "momentum_lite_v1", "alpha_source": True, "sample_count": 80},
            {"strategy_name": "absolute_trend_following_lite", "alpha_source": True, "sample_count": 80},
            {"strategy_name": "mean_reversion_lite", "alpha_source": True, "sample_count": 80},
            {"strategy_name": "low_vol_factor", "alpha_source": True, "sample_count": 80},
        ],
        "pair_rows": [
            {
                "left": "absolute_trend_following_lite",
                "right": "momentum_lite_v1",
                "left_family": "momentum",
                "right_family": "momentum",
                "same_family": True,
                "overlap": 80,
                "correlation": 0.82,
                "abs_correlation": 0.82,
                "status": "available",
            },
            {
                "left": "momentum_lite_v1",
                "right": "mean_reversion_lite",
                "left_family": "momentum",
                "right_family": "mean_reversion",
                "same_family": False,
                "overlap": 80,
                "correlation": -0.28,
                "abs_correlation": 0.28,
                "status": "available",
            },
            {
                "left": "low_vol_factor",
                "right": "momentum_lite_v1",
                "left_family": "low_vol_defensive",
                "right_family": "momentum",
                "same_family": False,
                "overlap": 80,
                "correlation": 0.35,
                "abs_correlation": 0.35,
                "status": "available",
            },
        ],
    }


if __name__ == "__main__":
    unittest.main()
