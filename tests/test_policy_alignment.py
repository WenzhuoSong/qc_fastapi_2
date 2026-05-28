from datetime import datetime, timedelta
import unittest

from services.policy_alignment import (
    default_manual_confirm_policy_alignment_config,
    policy_alignment_from_account_guard,
)


class PolicyAlignmentTests(unittest.TestCase):
    def test_manual_confirm_policy_alignment_config_defaults_to_five_minutes(self):
        cfg = default_manual_confirm_policy_alignment_config({})

        self.assertTrue(cfg["enabled"])
        self.assertEqual(cfg["max_age_seconds"], 300.0)

    def test_policy_alignment_requires_recent_matching_account_guard(self):
        now = datetime(2026, 5, 28, 15, 0, 0)
        guard = {
            "enabled": True,
            "status": "pass",
            "would_block": False,
            "blockers": [],
            "snapshot": {
                "recorded_at": (now - timedelta(seconds=60)).isoformat(),
                "policy_version": "sprint8a",
            },
            "checks": {
                "policy_version_present": {"pass": True, "actual": "sprint8a"},
                "policy_version_matches_expected": {"pass": True, "actual": "sprint8a"},
            },
        }

        result = policy_alignment_from_account_guard(
            guard,
            expected_policy_version="sprint8a",
            now=now,
            max_age_seconds=300,
        )

        self.assertTrue(result["aligned"], result)
        self.assertTrue(result["age_ok"])

    def test_policy_alignment_rejects_stale_account_guard(self):
        now = datetime(2026, 5, 28, 15, 0, 0)
        guard = {
            "enabled": True,
            "status": "pass",
            "would_block": False,
            "blockers": [],
            "snapshot": {
                "recorded_at": (now - timedelta(seconds=301)).isoformat(),
                "policy_version": "sprint8a",
            },
            "checks": {
                "policy_version_present": {"pass": True, "actual": "sprint8a"},
                "policy_version_matches_expected": {"pass": True, "actual": "sprint8a"},
            },
        }

        result = policy_alignment_from_account_guard(
            guard,
            expected_policy_version="sprint8a",
            now=now,
            max_age_seconds=300,
        )

        self.assertFalse(result["aligned"])
        self.assertFalse(result["age_ok"])


if __name__ == "__main__":
    unittest.main()
