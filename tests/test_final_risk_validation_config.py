import unittest

from services.final_risk_validation_config import (
    default_final_risk_validation_config,
    format_final_risk_validation_config,
)


class FinalRiskValidationConfigTests(unittest.TestCase):
    def test_default_mode_is_observe_until_operator_enables_blocking(self):
        cfg = default_final_risk_validation_config({})

        self.assertEqual(cfg["mode"], "observe")
        self.assertEqual(cfg["material_drift_threshold"], 0.015)
        self.assertIn("threshold_basis", cfg)

    def test_invalid_mode_falls_back_to_observe(self):
        cfg = default_final_risk_validation_config({"mode": "unsafe"})

        self.assertEqual(cfg["mode"], "observe")

    def test_format_mentions_threshold(self):
        text = format_final_risk_validation_config({"mode": "blocking", "material_drift_threshold": 0.02})

        self.assertIn("mode: blocking", text)
        self.assertIn("material_drift_threshold: 2.0%", text)


if __name__ == "__main__":
    unittest.main()
