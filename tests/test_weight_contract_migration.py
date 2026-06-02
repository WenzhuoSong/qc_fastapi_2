from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class WeightContractMigrationTest(unittest.TestCase):
    def test_strategy_use_constraints_uses_weight_ops_normalization(self):
        source = (ROOT / "services" / "strategy_use_constraints.py").read_text()

        self.assertIn("from services.weight_ops import normalize_cash_first", source)
        self.assertIn("normalize_cash_first(", source)
        self.assertNotIn("def _normalize(", source)

    def test_pipeline_legacy_pm_hard_clip_uses_weight_ops_normalization(self):
        source = (ROOT / "services" / "pipeline.py").read_text()

        self.assertIn("from services.weight_ops import normalize_cash_first", source)
        self.assertIn("normalized, _ = normalize_cash_first(clipped)", source)
        self.assertIn("working, _ = normalize_cash_first(working)", source)
        self.assertNotIn("def _normalize_regime_constraint_cash", source)
        self.assertNotIn("post-clip total weight is 0", source)

    def test_execution_throttle_uses_weight_ops_normalization(self):
        source = (ROOT / "services" / "execution_throttle.py").read_text()

        self.assertIn("from services.weight_ops import normalize_cash_first", source)
        self.assertIn("desired, _ = normalize_cash_first(_clean_weights(target_weights))", source)
        self.assertIn("staged, _ = normalize_cash_first(staged)", source)
        self.assertNotIn("def _normalize_cash_first", source)

    def test_position_layers_use_weight_ops_normalization(self):
        position_manager = (ROOT / "services" / "position_manager.py").read_text()
        position_governance = (ROOT / "services" / "position_governance.py").read_text()

        self.assertIn("from services.weight_ops import normalize_cash_first", position_manager)
        self.assertIn("adjusted, _ = normalize_cash_first(target)", position_manager)
        self.assertNotIn("def _normalize_weights", position_manager)

        self.assertIn("from services.weight_ops import normalize_cash_first", position_governance)
        self.assertIn("adjusted, _ = normalize_cash_first(work)", position_governance)
        self.assertNotIn("def _normalize_weights", position_governance)

    def test_portfolio_risk_diagnostic_uses_weight_ops_normalization(self):
        source = (ROOT / "services" / "portfolio_risk_diagnostic.py").read_text()

        self.assertIn("from services.weight_ops import normalize_cash_first", source)
        self.assertIn("target, _ = normalize_cash_first(_clean_weights(target_weights))", source)
        self.assertIn("current, _ = normalize_cash_first(_clean_weights(current_weights or {}))", source)
        self.assertNotIn("def _normalize_cash_first", source)


if __name__ == "__main__":
    unittest.main()
