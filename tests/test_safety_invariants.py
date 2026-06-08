import unittest

from services.safety_invariants import build_config_fail_safe_report


class SafetyInvariantTests(unittest.TestCase):
    def test_missing_safety_configs_are_reported_fail_safe(self):
        report = build_config_fail_safe_report({})

        codes = {finding["code"] for finding in report["findings"]}
        self.assertTrue(report["fail_safe_required"])
        self.assertIn("operator_halt_state_missing_or_invalid", codes)
        self.assertIn("circuit_state_missing_or_invalid", codes)
        self.assertIn("account_state_guard_config_missing_or_malformed", codes)
        self.assertIn("auto_pause_config_missing_or_malformed", codes)
        self.assertIn("execution_lifecycle_config_missing_or_malformed", codes)
        self.assertIn("reconciliation_guard_config_missing_or_malformed", codes)
        self.assertTrue(report["effective_states"]["operator_halt"]["halted"])
        self.assertEqual(report["effective_states"]["circuit_state"]["value"], "ALERT")

    def test_explicit_safe_configs_have_no_findings(self):
        report = build_config_fail_safe_report(
            {
                "operator_halt_state": {"halted": False, "reason": "seeded"},
                "circuit_state": {"value": "CLOSED"},
                "emergency_auto_liquidate": False,
                "account_state_guard_config": {"mode": "blocking"},
                "auto_pause_config": {"mode": "active"},
                "execution_lifecycle_config": {"mode": "strict"},
                "reconciliation_guard_config": {"mode": "blocking"},
            }
        )

        self.assertFalse(report["fail_safe_required"])
        self.assertEqual(report["finding_count"], 0)
        self.assertFalse(report["effective_states"]["operator_halt"]["halted"])
        self.assertEqual(report["effective_states"]["circuit_state"]["value"], "CLOSED")

    def test_emergency_auto_liquidate_requires_lifecycle_controls(self):
        report = build_config_fail_safe_report(
            {
                "operator_halt_state": {"halted": False},
                "circuit_state": {"value": "CLOSED"},
                "emergency_auto_liquidate": True,
                "account_state_guard_config": {"mode": "blocking"},
                "auto_pause_config": {"mode": "active"},
                "execution_lifecycle_config": {"mode": "strict"},
                "reconciliation_guard_config": {"mode": "blocking"},
            }
        )

        codes = {finding["code"] for finding in report["findings"]}
        self.assertIn("emergency_auto_liquidate_enabled_requires_lifecycle", codes)
        self.assertTrue(report["fail_safe_required"])


if __name__ == "__main__":
    unittest.main()
