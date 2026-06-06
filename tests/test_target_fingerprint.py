import unittest

from services.target_fingerprint import build_target_fingerprint


class TargetFingerprintTests(unittest.TestCase):
    def test_same_weights_with_different_order_produce_same_fingerprint(self):
        left = build_target_fingerprint(
            {"QQQ": 0.1026, "SPY": 0.0711},
            policy_version="sprint8a",
            tolerance=0.0025,
        )
        right = build_target_fingerprint(
            {"SPY": 0.0711, "QQQ": 0.1026},
            policy_version="sprint8a",
            tolerance=0.0025,
        )

        self.assertEqual(left["fingerprint"], right["fingerprint"])
        self.assertEqual(list(left["normalized_weights"]), ["QQQ", "SPY"])

    def test_tiny_drift_within_bucket_produces_same_fingerprint(self):
        left = build_target_fingerprint(
            {"QQQ": 0.3000},
            policy_version="sprint8a",
            tolerance=0.0025,
        )
        right = build_target_fingerprint(
            {"QQQ": 0.3001},
            policy_version="sprint8a",
            tolerance=0.0025,
        )

        self.assertEqual(left["fingerprint"], right["fingerprint"])

    def test_material_drift_changes_fingerprint(self):
        left = build_target_fingerprint(
            {"QQQ": 0.3000},
            policy_version="sprint8a",
            tolerance=0.0025,
        )
        right = build_target_fingerprint(
            {"QQQ": 0.3050},
            policy_version="sprint8a",
            tolerance=0.0025,
        )

        self.assertNotEqual(left["fingerprint"], right["fingerprint"])

    def test_policy_version_changes_fingerprint(self):
        left = build_target_fingerprint({"QQQ": 0.30}, policy_version="sprint8a")
        right = build_target_fingerprint({"QQQ": 0.30}, policy_version="sprint8b")

        self.assertNotEqual(left["fingerprint"], right["fingerprint"])

    def test_command_type_changes_fingerprint(self):
        left = build_target_fingerprint(
            {"QQQ": 0.30},
            command_type="SetWeights",
            policy_version="sprint8a",
        )
        right = build_target_fingerprint(
            {"QQQ": 0.30},
            command_type="CancelOrders",
            policy_version="sprint8a",
        )

        self.assertNotEqual(left["fingerprint"], right["fingerprint"])

    def test_command_and_correlation_metadata_do_not_change_fingerprint(self):
        left = build_target_fingerprint(
            {"QQQ": 0.30},
            policy_version="sprint8a",
            metadata={
                "command_id": "analysis_242",
                "correlation_id": "corr_a",
                "construction_epoch_id": "epoch_a",
                "timestamp": "2026-06-06T00:00:00Z",
            },
        )
        right = build_target_fingerprint(
            {"QQQ": 0.30},
            policy_version="sprint8a",
            metadata={
                "command_id": "analysis_243",
                "correlation_id": "corr_b",
                "construction_epoch_id": "epoch_b",
                "timestamp": "2026-06-06T01:00:00Z",
            },
        )

        self.assertEqual(left["fingerprint"], right["fingerprint"])
        self.assertEqual(left["metadata_not_hashed"]["correlation_id"], "corr_a")
        self.assertEqual(right["metadata_not_hashed"]["correlation_id"], "corr_b")

    def test_cash_is_excluded_because_qc_setweights_treats_it_as_residual(self):
        left = build_target_fingerprint(
            {"QQQ": 0.30, "CASH": 0.70},
            policy_version="sprint8a",
        )
        right = build_target_fingerprint(
            {"QQQ": 0.30, "CASH": 0.60},
            policy_version="sprint8a",
        )

        self.assertEqual(left["fingerprint"], right["fingerprint"])
        self.assertNotIn("CASH", left["normalized_weights"])


if __name__ == "__main__":
    unittest.main()
