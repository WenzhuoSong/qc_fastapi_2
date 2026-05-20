import unittest

from services.walk_forward_validation import validate_walk_forward


class WalkForwardValidationTest(unittest.TestCase):
    def test_high_when_strategy_passes_most_folds(self):
        out = validate_walk_forward({
            "momentum_lite_v1": [
                [0.01, -0.002, 0.012, 0.004, 0.006, -0.001, 0.008, 0.003],
                [0.009, 0.004, -0.001, 0.007, 0.006, 0.002, -0.002, 0.005],
                [0.011, 0.003, 0.006, -0.001, 0.004, 0.008, 0.001, 0.007],
                [0.006, 0.002, 0.009, -0.001, 0.003, 0.005, 0.004, 0.008],
            ],
        })

        row = out["items"]["momentum_lite_v1"]
        self.assertEqual(row["level"], "high")
        self.assertEqual(row["valid_fold_count"], 4)
        self.assertEqual(row["pass_rate"], 1.0)
        self.assertIn("walk_forward_high", row["reason_codes"])
        self.assertEqual(row["execution_authority"], "none")
        self.assertEqual(out["summary"]["stable_strategy_count"], 1)

    def test_weak_when_folds_are_unstable(self):
        out = validate_walk_forward({
            "mean_reversion_lite": [
                [-0.01, -0.002, 0.001, -0.004, -0.006, 0.001, -0.008, -0.003],
                [0.009, 0.004, -0.001, 0.007, 0.006, 0.002, -0.002, 0.005],
                [-0.011, -0.003, 0.006, -0.001, -0.004, -0.008, 0.001, -0.007],
            ],
        })

        row = out["items"]["mean_reversion_lite"]
        self.assertEqual(row["level"], "weak")
        self.assertLess(row["pass_rate"], 0.50)
        self.assertIn("walk_forward_pass_rate_low", row["reason_codes"])

    def test_insufficient_when_too_few_valid_folds(self):
        out = validate_walk_forward({
            "low_vol_factor": [
                [0.001, 0.002],
                [0.002, 0.001],
                [0.001, -0.001],
            ],
        })

        row = out["items"]["low_vol_factor"]
        self.assertEqual(row["level"], "insufficient")
        self.assertIn("walk_forward_folds_insufficient", row["reason_codes"])


if __name__ == "__main__":
    unittest.main()
