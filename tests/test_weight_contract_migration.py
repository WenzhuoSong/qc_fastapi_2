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
        self.assertNotIn("post-clip total weight is 0", source)


if __name__ == "__main__":
    unittest.main()
