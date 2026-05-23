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

    def test_executor_syncs_policy_before_setweights(self):
        text = Path("agents/executor.py").read_text()
        sync_pos = text.index("tool_send_policy_sync")
        send_pos = text.index("tool_send_weight_command")

        self.assertLess(sync_pos, send_pos)
        self.assertIn("PolicySync failed before", text)
        self.assertIn("No command sent to QC", text)

    def test_setweights_command_carries_policy_snapshot(self):
        text = Path("tools/qc_tools.py").read_text()

        self.assertIn("policy = inp.get(\"policy\") or policy_snapshot()", text)
        self.assertIn('"policy_version": policy.get("version")', text)
        self.assertIn('"policy": policy', text)


if __name__ == "__main__":
    unittest.main()
