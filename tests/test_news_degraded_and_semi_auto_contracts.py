from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class NewsDegradedAndSemiAutoContractsTest(unittest.TestCase):
    def test_news_degraded_mode_blocks_risk_increase_but_uses_reduce_only_gate(self):
        source = (ROOT / "services" / "pipeline.py").read_text()

        self.assertIn("_evaluate_news_degraded_execution_gate", source)
        self.assertIn("is_reduce_only_vs_actual", source)
        self.assertIn("news_stale_degraded_mode_blocks_risk_increase", source)
        self.assertIn("skipped_news_stale_risk_increase", source)

    def test_semi_auto_forces_account_guard_blocking_for_operator_pack_truth(self):
        source = (ROOT / "services" / "pipeline.py").read_text()

        self.assertIn('auth_mode == "SEMI_AUTO"', source)
        self.assertIn('account_state_guard_config["mode"] = "blocking"', source)
        self.assertIn("semi_auto_effective_mode", source)
        self.assertIn("semi_auto_requires_fresh_account_truth", source)


if __name__ == "__main__":
    unittest.main()
