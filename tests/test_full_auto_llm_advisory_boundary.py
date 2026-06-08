from pathlib import Path
import unittest


class FullAutoLlmAdvisoryBoundaryTests(unittest.TestCase):
    def test_full_auto_llm_advisory_is_trim_only_source_contract(self):
        source = Path("services/pipeline.py").read_text()

        self.assertIn('auth_mode == "FULL_AUTO"', source)
        self.assertIn('governance_config["llm_advisory_max_add_pct"] = 0.0', source)
        self.assertIn(
            'governance_config["llm_advisory_full_auto_policy"] = "trim_only_no_add"',
            source,
        )


if __name__ == "__main__":
    unittest.main()
