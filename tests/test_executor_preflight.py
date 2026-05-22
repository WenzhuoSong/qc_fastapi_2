import unittest
from pathlib import Path

from services.execution_preflight import preflight_execution_weights


class ExecutorPreflightTests(unittest.TestCase):
    def test_blocks_unknown_positive_weight(self):
        result = preflight_execution_weights({"COMPLETELY_UNKNOWN": 0.01, "CASH": 0.99})

        self.assertFalse(result["allowed"])
        self.assertEqual(result["cap_violations"][0]["ticker"], "COMPLETELY_UNKNOWN")

    def test_blocks_single_cap_violation(self):
        result = preflight_execution_weights({"PSI": 0.08, "CASH": 0.92})

        self.assertFalse(result["allowed"])
        self.assertEqual(result["cap_violations"][0]["ticker"], "PSI")

    def test_allows_policy_compliant_weights(self):
        result = preflight_execution_weights({"SPY": 0.20, "PSI": 0.075, "SQQQ": 0.03, "CASH": 0.695})

        self.assertTrue(result["allowed"], result)

    def test_preflight_block_copy_points_to_final_cap_system_bug(self):
        text = Path("agents/executor.py").read_text()

        self.assertIn("Executor preflight blocked", text)
        self.assertIn("final_policy_cap stage failed to enforce execution limits", text)
        self.assertIn("This is a system bug, not a business decision", text)


if __name__ == "__main__":
    unittest.main()
