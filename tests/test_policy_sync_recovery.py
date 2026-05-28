import unittest
from datetime import datetime

from services.policy_sync_recovery import (
    default_policy_sync_recovery_config,
    evaluate_policy_sync_recovery,
    policy_sync_recovery_suppresses_auto_pause,
)


class PolicySyncRecoveryTests(unittest.TestCase):
    def test_recoverable_policy_mismatch_sends_sync(self):
        result = evaluate_policy_sync_recovery(
            account_guard_result=_guard(blockers=["policy_version_mismatch"], observed="sprint8a_fallback"),
            recovery_state={},
            execution_policy_version="sprint8a",
            now=datetime(2026, 5, 27, 15, 0, 0),
        )

        self.assertEqual(result["status"], "recoverable")
        self.assertEqual(result["action"], "send_sync")
        self.assertTrue(result["trading_blocked"])
        self.assertEqual(result["next_state"]["attempt_count"], 1)
        self.assertEqual(result["next_state"]["consecutive_mismatch_cycles"], 1)

    def test_waiting_cycle_does_not_send_duplicate_sync(self):
        result = evaluate_policy_sync_recovery(
            account_guard_result=_guard(blockers=["policy_version_mismatch"], observed="sprint8a_fallback"),
            recovery_state={
                "status": "pending_confirmation",
                "last_sync_command_id": "policy_recovery_x",
                "attempt_count": 1,
                "consecutive_mismatch_cycles": 1,
            },
            execution_policy_version="sprint8a",
            now=datetime(2026, 5, 27, 15, 15, 0),
        )

        self.assertEqual(result["status"], "recoverable")
        self.assertEqual(result["action"], "waiting_for_confirmation")
        self.assertEqual(result["next_state"]["attempt_count"], 1)
        self.assertEqual(result["next_state"]["consecutive_mismatch_cycles"], 2)

    def test_send_failed_state_retries_until_attempt_limit(self):
        result = evaluate_policy_sync_recovery(
            account_guard_result=_guard(blockers=["policy_version_mismatch"], observed="sprint8a_fallback"),
            recovery_state={
                "status": "send_failed",
                "attempt_count": 1,
                "consecutive_mismatch_cycles": 1,
            },
            execution_policy_version="sprint8a",
        )

        self.assertEqual(result["action"], "send_sync")
        self.assertEqual(result["next_state"]["attempt_count"], 2)

    def test_attempt_limit_becomes_unrecoverable(self):
        result = evaluate_policy_sync_recovery(
            account_guard_result=_guard(blockers=["policy_version_mismatch"], observed="sprint8a_fallback"),
            recovery_state={
                "status": "send_failed",
                "attempt_count": 3,
                "consecutive_mismatch_cycles": 2,
            },
            execution_policy_version="sprint8a",
        )

        self.assertEqual(result["status"], "unrecoverable")
        self.assertEqual(result["reason"], "max_recovery_attempts_exhausted")

    def test_cycle_limit_becomes_unrecoverable(self):
        result = evaluate_policy_sync_recovery(
            account_guard_result=_guard(blockers=["policy_version_mismatch"], observed="sprint8a_fallback"),
            recovery_state={
                "status": "pending_confirmation",
                "attempt_count": 1,
                "consecutive_mismatch_cycles": 4,
            },
            execution_policy_version="sprint8a",
        )

        self.assertEqual(result["status"], "unrecoverable")
        self.assertEqual(result["reason"], "max_consecutive_mismatch_cycles_exhausted")

    def test_stale_account_does_not_recover(self):
        result = evaluate_policy_sync_recovery(
            account_guard_result=_guard(
                blockers=[
                    "account_state_snapshot_stale_or_missing_time",
                    "policy_version_mismatch",
                ],
                observed="sprint8a_fallback",
                snapshot_fresh=False,
            ),
            recovery_state={},
            execution_policy_version="sprint8a",
        )

        self.assertEqual(result["status"], "unrecoverable")
        self.assertEqual(result["reason"], "non_recoverable_account_guard_blockers")

    def test_open_orders_do_not_recover(self):
        result = evaluate_policy_sync_recovery(
            account_guard_result=_guard(
                blockers=["policy_version_mismatch"],
                observed="sprint8a_fallback",
                no_open_orders=False,
            ),
            recovery_state={},
            execution_policy_version="sprint8a",
        )

        self.assertEqual(result["status"], "unrecoverable")
        self.assertEqual(result["reason"], "account_state_not_safe_for_policy_sync_recovery")

    def test_qc_rejected_sync_is_unrecoverable(self):
        result = evaluate_policy_sync_recovery(
            account_guard_result=_guard(blockers=["policy_version_mismatch"], observed="sprint8a_fallback"),
            recovery_state={
                "status": "pending_confirmation",
                "attempt_count": 1,
                "consecutive_mismatch_cycles": 1,
                "last_qc_status": "rejected",
                "last_sync_protocol_version": "v2_payload_json",
            },
            execution_policy_version="sprint8a",
        )

        self.assertEqual(result["status"], "unrecoverable")
        self.assertEqual(result["reason"], "policy_sync_rejected")

    def test_old_protocol_rejection_can_retry_after_protocol_upgrade(self):
        result = evaluate_policy_sync_recovery(
            account_guard_result=_guard(blockers=["policy_version_mismatch"], observed="sprint8a_fallback"),
            recovery_state={
                "status": "unrecoverable",
                "attempt_count": 1,
                "consecutive_mismatch_cycles": 1,
                "last_qc_status": "rejected",
                "last_qc_rejection_reason": "policy_sync_missing_roles_or_caps",
                "last_sync_protocol_version": "v1_nested_payload",
            },
            execution_policy_version="sprint8a",
            config={"sync_protocol_version": "v2_payload_json"},
        )

        self.assertEqual(result["status"], "recoverable")
        self.assertEqual(result["action"], "send_sync")
        self.assertEqual(result["next_state"]["attempt_count"], 2)

    def test_recovery_state_cleared_on_policy_restore(self):
        result = evaluate_policy_sync_recovery(
            account_guard_result=_guard(blockers=[], observed="sprint8a", would_block=False),
            recovery_state={
                "status": "pending_confirmation",
                "attempt_count": 2,
                "consecutive_mismatch_cycles": 3,
            },
            execution_policy_version="sprint8a",
        )

        self.assertEqual(result["status"], "recovered")
        self.assertEqual(result["action"], "mark_recovered")
        self.assertEqual(result["next_state"]["attempt_count"], 0)
        self.assertEqual(result["next_state"]["consecutive_mismatch_cycles"], 0)

    def test_recoverable_recovery_suppresses_auto_pause(self):
        self.assertTrue(policy_sync_recovery_suppresses_auto_pause({
            "status": "recoverable",
            "action": "waiting_for_confirmation",
        }))
        self.assertFalse(policy_sync_recovery_suppresses_auto_pause({
            "status": "unrecoverable",
            "action": "none",
        }))

    def test_default_config_is_normalized(self):
        cfg = default_policy_sync_recovery_config({
            "max_recovery_attempts": 0,
            "max_consecutive_mismatch_cycles": "bad",
        })

        self.assertEqual(cfg["max_recovery_attempts"], 1)
        self.assertEqual(cfg["max_consecutive_mismatch_cycles"], 5)
        self.assertEqual(cfg["sync_protocol_version"], "v2_payload_json")


def _guard(
    *,
    blockers: list[str],
    observed: str,
    would_block: bool = True,
    snapshot_fresh: bool = True,
    no_open_orders: bool = True,
) -> dict:
    checks = {
        "snapshot_fresh": {"pass": snapshot_fresh, "actual": 60, "threshold": 1200},
        "explicit_account_state": {"pass": True},
        "account_status_ok": {"pass": True, "actual": "ok"},
        "data_status_ok": {"pass": True, "actual": "ok"},
        "policy_version_present": {"pass": True, "actual": observed},
        "policy_version_matches_expected": {
            "pass": observed == "sprint8a",
            "actual": observed,
            "threshold": "sprint8a",
        },
        "no_open_orders": {"pass": no_open_orders},
        "buying_power_present": {"pass": True},
        "holdings_weights_present": {"pass": True},
        "holdings_match_snapshot_rows": {"pass": True},
    }
    return {
        "mode": "blocking",
        "status": "blocked" if would_block else "pass",
        "allowed": not would_block,
        "would_block": would_block,
        "blockers": blockers,
        "checks": checks,
        "snapshot": {
            "age_seconds": 60,
            "account_status": "ok",
            "data_status": "ok",
            "open_order_count": 0 if no_open_orders else 1,
            "has_open_orders": not no_open_orders,
            "policy_version": observed,
        },
    }


if __name__ == "__main__":
    unittest.main()
