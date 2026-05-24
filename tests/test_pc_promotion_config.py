import unittest

from services.pc_promotion_config import default_pc_promotion_config, format_pc_promotion_config


class PCPromotionConfigTests(unittest.TestCase):
    def test_default_config_is_shadow_only(self):
        cfg = default_pc_promotion_config({})

        self.assertEqual(cfg["portfolio_construction_mode"], "shadow")
        self.assertFalse(cfg["enabled"])
        self.assertEqual(cfg["min_shadow_cycles"], 20)
        self.assertEqual(cfg["min_pass_rate"], 0.90)
        self.assertEqual(cfg["max_material_diff"], 0.015)
        self.assertTrue(cfg["require_semi_auto_gated_before_full_auto"])
        self.assertEqual(cfg["min_gated_semi_auto_confirmed_cycles"], 5)
        self.assertFalse(cfg["allow_full_auto_gated"])

    def test_format_mentions_auto_mode_and_no_execution_authority(self):
        text = format_pc_promotion_config(default_pc_promotion_config({}))

        self.assertIn("construction_mode: shadow", text)
        self.assertIn("enabled: False", text)
        self.assertIn("approval_mode: auto", text)
        self.assertIn("min_gated_semi_auto_confirmed_cycles: 5", text)
        self.assertIn("allow_full_auto_gated: False", text)
        self.assertIn("execution_authority: none", text)

    def test_format_respects_explicit_zero_for_paper_live_canary(self):
        text = format_pc_promotion_config(
            default_pc_promotion_config(
                {
                    "portfolio_construction_mode": "gated",
                    "enabled": True,
                    "min_shadow_cycles": 0,
                    "min_cycles": 0,
                    "min_pass_rate": 0.0,
                }
            )
        )

        self.assertIn("min_shadow_cycles: 0", text)
        self.assertIn("min_pass_rate: 0%", text)


if __name__ == "__main__":
    unittest.main()
