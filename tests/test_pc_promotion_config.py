import unittest

from services.pc_promotion_config import default_pc_promotion_config, format_pc_promotion_config


class PCPromotionConfigTests(unittest.TestCase):
    def test_default_config_is_enabled_auto(self):
        cfg = default_pc_promotion_config({})

        self.assertTrue(cfg["enabled"])
        self.assertFalse(cfg["require_manual_approval"])
        self.assertEqual(cfg["min_cycles"], 20)

    def test_format_mentions_auto_mode_and_no_execution_authority(self):
        text = format_pc_promotion_config(default_pc_promotion_config({}))

        self.assertIn("enabled: True", text)
        self.assertIn("approval_mode: auto", text)
        self.assertIn("execution_authority: none", text)


if __name__ == "__main__":
    unittest.main()
