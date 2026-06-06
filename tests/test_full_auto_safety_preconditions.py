import unittest

from services.full_auto_safety import full_auto_safety_precondition_violations


class FullAutoSafetyPreconditionTests(unittest.TestCase):
    def test_semi_auto_does_not_require_blocking_safety_modes(self):
        violations = full_auto_safety_precondition_violations(
            auth_mode="SEMI_AUTO",
            account_state_guard_config={"mode": "observe"},
            final_risk_validation_config={"mode": "observe"},
            auto_pause_config={"mode": "observe"},
            execution_lifecycle_config={"mode": "observe"},
            reconciliation_guard_config={"mode": "observe"},
        )

        self.assertEqual(violations, [])

    def test_full_auto_requires_code_enforced_safety_layers(self):
        violations = full_auto_safety_precondition_violations(
            auth_mode="FULL_AUTO",
            account_state_guard_config={"mode": "observe"},
            final_risk_validation_config={"mode": "observe"},
            auto_pause_config={"mode": "observe"},
            execution_lifecycle_config={"mode": "observe"},
            reconciliation_guard_config={"mode": "observe"},
        )

        self.assertIn("account_state_guard.mode must be blocking in FULL_AUTO", violations)
        self.assertIn("final_risk_validation effective mode must be blocking in FULL_AUTO", violations)
        self.assertIn("auto_pause.mode must be active in FULL_AUTO", violations)
        self.assertIn(
            "execution_lifecycle_config.mode must be active or strict in FULL_AUTO (current: observe)",
            violations,
        )
        self.assertIn(
            "reconciliation_guard_config.mode must be blocking in FULL_AUTO (current: observe)",
            violations,
        )

    def test_full_auto_accepts_auto_final_validation_when_effective_blocking(self):
        violations = full_auto_safety_precondition_violations(
            auth_mode="FULL_AUTO",
            account_state_guard_config={"mode": "blocking"},
            final_risk_validation_config={"mode": "auto", "full_auto_effective_mode": "blocking"},
            auto_pause_config={"mode": "active"},
            execution_lifecycle_config={"mode": "active"},
            reconciliation_guard_config={"mode": "blocking"},
        )

        self.assertEqual(violations, [])

    def test_full_auto_accepts_strict_execution_lifecycle(self):
        violations = full_auto_safety_precondition_violations(
            auth_mode="FULL_AUTO",
            account_state_guard_config={"mode": "blocking"},
            final_risk_validation_config={"mode": "blocking"},
            auto_pause_config={"mode": "active"},
            execution_lifecycle_config={"mode": "strict"},
            reconciliation_guard_config={"mode": "blocking"},
        )

        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
