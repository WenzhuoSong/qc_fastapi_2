import unittest
from pathlib import Path


class PolicySyncRecoveryPipelineTests(unittest.TestCase):
    def test_pipeline_runs_recovery_between_account_guard_and_auto_pause(self):
        text = Path("services/pipeline.py").read_text()

        guard_pos = text.index("account_guard_effect = account_state_guard_pipeline_effect")
        recovery_pos = text.index("run_policy_sync_recovery(")
        auto_pause_pos = text.index("load_auto_pause_verdict(")

        self.assertLess(guard_pos, recovery_pos)
        self.assertLess(recovery_pos, auto_pause_pos)
        self.assertIn("policy_sync_recovery=policy_sync_recovery", text)
        self.assertIn("skipped_policy_sync_recovery", text)

    def test_pipeline_expected_policy_version_comes_from_execution_policy(self):
        text = Path("services/pipeline.py").read_text()

        self.assertIn('account_state_guard_config["expected_policy_version"]', text)
        self.assertIn('policy_snapshot().get("version")', text)
        self.assertIn("policy_sync_recovery_config", text)


if __name__ == "__main__":
    unittest.main()
