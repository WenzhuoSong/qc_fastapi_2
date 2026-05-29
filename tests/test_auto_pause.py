from datetime import datetime, timedelta
import json
import unittest

from services.auto_pause import default_auto_pause_config, evaluate_auto_pause_triggers


class AutoPauseTests(unittest.TestCase):
    def test_consecutive_qc_rejects_would_pause_in_observe(self):
        now = datetime(2026, 5, 24, 15, 0, 0)
        result = evaluate_auto_pause_triggers(
            execution_events=[
                _event("cmd_2", "rejected", now - timedelta(minutes=1), "role cap"),
                _event("cmd_1", "rejected", now - timedelta(minutes=2), "single cap"),
            ],
            account_state_guard=_clean_guard(),
            now=now,
        )

        self.assertEqual(result["status"], "would_pause")
        self.assertTrue(result["would_pause"])
        self.assertFalse(result["should_pause"])
        self.assertEqual(result["primary_trigger"], "consecutive_qc_rejects")

    def test_accepted_breaks_consecutive_reject_count(self):
        now = datetime(2026, 5, 24, 15, 0, 0)
        result = evaluate_auto_pause_triggers(
            execution_events=[
                _event("cmd_3", "rejected", now - timedelta(minutes=1), "single cap"),
                _event("cmd_2", "accepted", now - timedelta(minutes=2), ""),
                _event("cmd_1", "rejected", now - timedelta(minutes=3), "role cap"),
            ],
            account_state_guard=_clean_guard(),
            now=now,
        )

        self.assertEqual(result["status"], "pass")
        self.assertFalse(result["would_pause"])

    def test_policy_sync_reject_does_not_count_as_trading_reject(self):
        now = datetime(2026, 5, 24, 15, 0, 0)
        result = evaluate_auto_pause_triggers(
            execution_events=[
                _event(
                    "policy_recovery_1",
                    "rejected",
                    now - timedelta(minutes=1),
                    "policy_sync_missing_roles_or_caps",
                    command_type="policy_sync",
                ),
                _event("cmd_1", "rejected", now - timedelta(minutes=2), "policy_version_mismatch_with_buy"),
            ],
            account_state_guard=_clean_guard(),
            config={"mode": "active"},
            now=now,
        )

        self.assertEqual(result["status"], "pass")
        self.assertFalse(result["would_pause"])

    def test_old_reject_does_not_count_as_fresh_consecutive_reject(self):
        now = datetime(2026, 5, 24, 15, 0, 0)
        result = evaluate_auto_pause_triggers(
            execution_events=[
                _event("cmd_2", "rejected", now - timedelta(minutes=10), "role cap"),
                _event("cmd_1", "rejected", now - timedelta(hours=7), "single cap"),
            ],
            account_state_guard=_clean_guard(),
            config={"mode": "active", "max_qc_reject_event_age_hours": 6},
            now=now,
        )

        self.assertEqual(result["status"], "pass")
        self.assertFalse(result["would_pause"])

    def test_not_sent_breaks_qc_reject_streak(self):
        now = datetime(2026, 5, 24, 15, 0, 0)
        result = evaluate_auto_pause_triggers(
            execution_events=[
                _event("cmd_3", "rejected", now - timedelta(minutes=1), "role cap"),
                _event("cmd_2", "not_sent", now - timedelta(minutes=2), "fastapi_no_qc_command"),
                _event("cmd_1", "rejected", now - timedelta(minutes=3), "single cap"),
            ],
            account_state_guard=_clean_guard(),
            config={"mode": "active"},
            now=now,
        )

        self.assertEqual(result["status"], "pass")
        self.assertFalse(result["would_pause"])

    def test_active_mode_should_pause_on_consecutive_rejects(self):
        now = datetime(2026, 5, 24, 15, 0, 0)
        result = evaluate_auto_pause_triggers(
            execution_events=[
                _event("cmd_2", "rejected", now - timedelta(minutes=1), "role cap"),
                _event("cmd_1", "rejected", now - timedelta(minutes=2), "single cap"),
            ],
            account_state_guard=_clean_guard(),
            config={"mode": "active"},
            now=now,
        )

        self.assertEqual(result["status"], "pause_required")
        self.assertTrue(result["should_pause"])
        self.assertEqual(result["execution_effect"], "circuit_alert")

    def test_policy_mismatch_timeout_triggers_only_when_latest_is_stale(self):
        now = datetime(2026, 5, 24, 15, 10, 0)
        old_mismatch = _event(
            "cmd_policy",
            "rejected",
            now - timedelta(minutes=7),
            "policy_version_mismatch_with_buy",
            qc_response={"policy_mismatch": True, "policy_version": "sprint8a"},
        )
        result = evaluate_auto_pause_triggers(
            execution_events=[old_mismatch],
            account_state_guard=_clean_guard(),
            now=now,
        )

        self.assertEqual(result["primary_trigger"], "policy_mismatch_timeout")
        self.assertTrue(result["would_pause"])

        fresh = dict(old_mismatch)
        fresh["qc_ack_at"] = (now - timedelta(minutes=1)).isoformat()
        result_fresh = evaluate_auto_pause_triggers(
            execution_events=[fresh],
            account_state_guard=_clean_guard(),
            now=now,
        )
        self.assertFalse(result_fresh["would_pause"])

    def test_account_state_guard_stale_triggers_would_pause(self):
        result = evaluate_auto_pause_triggers(
            execution_events=[],
            account_state_guard={
                "status": "would_block",
                "would_block": True,
                "blockers": ["account_state_snapshot_stale_or_missing_time"],
                "snapshot": {"age_seconds": 600},
            },
            now=datetime(2026, 5, 24, 15, 10, 0),
        )

        self.assertTrue(result["would_pause"])
        self.assertEqual(result["primary_trigger"], "account_state_stale")

    def test_recoverable_policy_sync_recovery_suppresses_account_guard_pause(self):
        now = datetime(2026, 5, 24, 15, 10, 0)
        result = evaluate_auto_pause_triggers(
            execution_events=[
                _event(
                    "analysis_205",
                    "rejected",
                    now - timedelta(minutes=30),
                    "policy_version_mismatch_with_buy",
                    qc_response={"policy_mismatch": True},
                )
            ],
            account_state_guard={
                "status": "blocked",
                "would_block": True,
                "blockers": ["policy_version_mismatch"],
                "snapshot": {"age_seconds": 60},
            },
            policy_sync_recovery={
                "status": "recoverable",
                "action": "waiting_for_confirmation",
                "reason": "waiting_for_policy_version_confirmation",
            },
            config={"mode": "active"},
            now=now,
        )

        self.assertFalse(result["would_pause"])
        self.assertFalse(result["should_pause"])
        self.assertEqual(result["status"], "pass")

    def test_unrecoverable_policy_sync_recovery_triggers_pause(self):
        result = evaluate_auto_pause_triggers(
            execution_events=[],
            account_state_guard=_clean_guard(),
            policy_sync_recovery={
                "status": "unrecoverable",
                "action": "none",
                "reason": "max_recovery_attempts_exhausted",
            },
            config={"mode": "active"},
            now=datetime(2026, 5, 24, 15, 10, 0),
        )

        self.assertTrue(result["should_pause"])
        self.assertEqual(result["primary_trigger"], "policy_sync_recovery_exhausted")

    def test_default_config_is_json_serializable(self):
        config = default_auto_pause_config({"mode": "surprise"})

        self.assertEqual(config["mode"], "observe")
        json.dumps(config)


def _event(
    command_id: str,
    status: str,
    when: datetime,
    reason: str,
    qc_response: dict | None = None,
    command_type: str = "weight_adjustment",
):
    return {
        "command_id": command_id,
        "command_type": command_type,
        "qc_status": status,
        "qc_ack_at": when.isoformat(),
        "executed_at": when.isoformat(),
        "qc_rejection_reason": reason,
        "qc_response": qc_response or {"reason": reason},
    }


def _clean_guard():
    return {
        "status": "pass",
        "would_block": False,
        "blockers": [],
        "snapshot": {"age_seconds": 60},
    }


if __name__ == "__main__":
    unittest.main()
