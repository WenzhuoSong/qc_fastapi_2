import unittest

from services.final_risk_validation_config import (
    default_final_risk_validation_config,
    format_final_risk_validation_config,
    resolve_final_risk_validation_mode,
)


class FinalRiskValidationConfigTests(unittest.TestCase):
    def test_default_mode_is_observe_until_operator_enables_blocking(self):
        cfg = default_final_risk_validation_config({})

        self.assertEqual(cfg["mode"], "observe")
        self.assertEqual(cfg["material_drift_threshold"], 0.015)
        self.assertEqual(cfg["full_auto_effective_mode"], "blocking")
        self.assertEqual(cfg["semi_auto_effective_mode"], "observe")
        self.assertIn("threshold_basis", cfg)

    def test_invalid_mode_falls_back_to_observe(self):
        cfg = default_final_risk_validation_config({"mode": "unsafe"})

        self.assertEqual(cfg["mode"], "observe")

    def test_format_mentions_threshold(self):
        text = format_final_risk_validation_config({"mode": "blocking", "material_drift_threshold": 0.02})

        self.assertIn("mode: blocking", text)
        self.assertIn("full_auto_effective_mode: blocking", text)
        self.assertIn("material_drift_threshold: 2.0%", text)

    def test_auto_mode_blocks_only_when_auth_mode_is_full_auto_by_default(self):
        cfg = default_final_risk_validation_config({"mode": "auto"})

        self.assertEqual(resolve_final_risk_validation_mode(cfg, auth_mode="FULL_AUTO"), "blocking")
        self.assertEqual(resolve_final_risk_validation_mode(cfg, auth_mode="SEMI_AUTO"), "observe")

    def test_auto_mode_can_be_overridden_for_canary(self):
        cfg = default_final_risk_validation_config({
            "mode": "auto",
            "full_auto_effective_mode": "observe",
        })

        self.assertEqual(resolve_final_risk_validation_mode(cfg, auth_mode="FULL_AUTO"), "observe")


if __name__ == "__main__":
    unittest.main()
