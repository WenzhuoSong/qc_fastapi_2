from datetime import datetime, timedelta
import json
import unittest

from services.account_state_guard import (
    account_state_guard_pipeline_effect,
    default_account_state_guard_config,
    evaluate_account_state_guard,
)


class AccountStateGuardTests(unittest.TestCase):
    def test_passes_fresh_clean_account_state(self):
        now = datetime(2026, 5, 24, 15, 0, 0)
        result = evaluate_account_state_guard(
            {
                "id": 7,
                "qc_snapshot_id": 11,
                "recorded_at": now - timedelta(seconds=60),
                "source_packet_type": "heartbeat",
                "contract_version": "v1",
                "account_status": "ok",
                "data_status": "ok",
                "policy_version": "sprint8a",
                "buying_power": 50000,
                "open_order_count": 0,
                "has_open_orders": False,
                "is_market_open": True,
                "holdings_weights": {"SPY": 0.4, "QQQ": 0.3},
                "raw_snapshot": {"explicit_account_state": True, "warnings": []},
            },
            now=now,
            reference_weights={"SPY": 0.4001, "QQQ": 0.2999},
        )

        self.assertEqual(result["status"], "pass")
        self.assertTrue(result["allowed"])
        self.assertFalse(result["would_block"])
        self.assertEqual(result["execution_effect"], "diagnostic_only")
        self.assertEqual(result["snapshot"]["holdings_count"], 2)

    def test_observe_mode_reports_would_block_but_allows_pipeline(self):
        now = datetime(2026, 5, 26, 15, 0, 0)
        result = evaluate_account_state_guard(
            {
                "recorded_at": now - timedelta(minutes=10),
                "source_packet_type": "heartbeat",
                "contract_version": "v1",
                "account_status": "ok",
                "data_status": "ok",
                "policy_version": "sprint8a",
                "buying_power": 50000,
                "open_order_count": 0,
                "holdings_weights": {"SPY": 0.4},
                "raw_snapshot": {"explicit_account_state": True},
            },
            now=now,
        )

        self.assertEqual(result["mode"], "observe")
        self.assertEqual(result["status"], "would_block")
        self.assertTrue(result["allowed"])
        self.assertTrue(result["would_block"])
        self.assertIn("account_state_snapshot_stale_or_missing_time", result["blockers"])
        self.assertEqual(
            result["checks"]["snapshot_fresh"]["classification"],
            "unexpected_market_open_stale",
        )

    def test_closed_market_stale_is_expected_no_action_needed(self):
        now = datetime(2026, 6, 6, 3, 40, 0)
        result = evaluate_account_state_guard(
            {
                "recorded_at": now - timedelta(hours=7),
                "source_packet_type": "daily_feature_snapshot",
                "contract_version": "v1",
                "account_status": "ok",
                "data_status": "ok",
                "policy_version": "sprint8a",
                "buying_power": 50000,
                "open_order_count": 0,
                "holdings_weights": {"SPY": 0.4},
                "raw_snapshot": {"explicit_account_state": True},
            },
            config={
                "mode": "blocking",
                "max_snapshot_age_seconds": 1200,
                "max_market_closed_stale_seconds": 72 * 3600,
            },
            now=now,
        )

        self.assertEqual(result["status"], "pass")
        self.assertTrue(result["allowed"])
        self.assertFalse(result["would_block"])
        self.assertNotIn("account_state_snapshot_stale_or_missing_time", result["blockers"])
        self.assertEqual(
            result["checks"]["snapshot_fresh"]["classification"],
            "expected_market_closed_stale",
        )
        self.assertEqual(result["freshness"]["classification"], "expected_market_closed_stale")
        self.assertNotIn("extended_closed_stale", result["warnings"])

    def test_market_open_stale_remains_blocking(self):
        now = datetime(2026, 6, 8, 15, 0, 0)
        result = evaluate_account_state_guard(
            {
                "recorded_at": now - timedelta(minutes=30),
                "source_packet_type": "heartbeat",
                "contract_version": "v1",
                "account_status": "ok",
                "data_status": "ok",
                "policy_version": "sprint8a",
                "buying_power": 50000,
                "open_order_count": 0,
                "holdings_weights": {"SPY": 0.4},
                "raw_snapshot": {"explicit_account_state": True},
            },
            config={"mode": "blocking", "max_snapshot_age_seconds": 1200},
            now=now,
        )

        self.assertEqual(result["status"], "blocked")
        self.assertFalse(result["allowed"])
        self.assertIn("account_state_snapshot_stale_or_missing_time", result["blockers"])
        self.assertEqual(
            result["checks"]["snapshot_fresh"]["classification"],
            "unexpected_market_open_stale",
        )
        self.assertTrue(result["checks"]["snapshot_fresh"]["market_status"]["is_open"])

    def test_extended_closed_stale_warns_without_blocking_diagnostic_analysis(self):
        now = datetime(2026, 6, 6, 3, 40, 0)
        result = evaluate_account_state_guard(
            {
                "recorded_at": now - timedelta(hours=96),
                "source_packet_type": "daily_feature_snapshot",
                "contract_version": "v1",
                "account_status": "ok",
                "data_status": "ok",
                "policy_version": "sprint8a",
                "buying_power": 50000,
                "open_order_count": 0,
                "holdings_weights": {"SPY": 0.4},
                "raw_snapshot": {"explicit_account_state": True},
            },
            config={
                "mode": "blocking",
                "max_snapshot_age_seconds": 1200,
                "max_market_closed_stale_seconds": 72 * 3600,
            },
            now=now,
        )

        self.assertEqual(result["status"], "pass")
        self.assertTrue(result["allowed"])
        self.assertFalse(result["would_block"])
        self.assertNotIn("extended_closed_stale", result["blockers"])
        self.assertIn("extended_closed_stale", result["warnings"])
        self.assertFalse(result["checks"]["snapshot_fresh"]["pass"])
        self.assertEqual(
            result["checks"]["snapshot_fresh"]["classification"],
            "extended_closed_stale",
        )

    def test_blocking_mode_disallows_open_orders(self):
        now = datetime(2026, 5, 24, 15, 0, 0)
        result = evaluate_account_state_guard(
            {
                "recorded_at": now,
                "source_packet_type": "heartbeat",
                "contract_version": "v1",
                "account_status": "ok",
                "data_status": "ok",
                "policy_version": "sprint8a",
                "buying_power": 50000,
                "open_order_count": 1,
                "has_open_orders": True,
                "holdings_weights": {"SPY": 0.4},
                "raw_snapshot": {"explicit_account_state": True},
            },
            config={"mode": "blocking"},
            now=now,
        )

        self.assertEqual(result["status"], "blocked")
        self.assertFalse(result["allowed"])
        self.assertIn("open_orders_present_or_unknown", result["blockers"])

    def test_blocking_mode_disallows_policy_version_mismatch(self):
        now = datetime(2026, 5, 24, 15, 0, 0)
        result = evaluate_account_state_guard(
            {
                "recorded_at": now,
                "source_packet_type": "heartbeat",
                "contract_version": "v1",
                "account_status": "ok",
                "data_status": "ok",
                "policy_version": "sprint8b",
                "buying_power": 50000,
                "open_order_count": 0,
                "has_open_orders": False,
                "holdings_weights": {"SPY": 0.4},
                "raw_snapshot": {"explicit_account_state": True},
            },
            config={"mode": "blocking", "expected_policy_version": "sprint8a"},
            now=now,
        )

        self.assertEqual(result["status"], "blocked")
        self.assertFalse(result["allowed"])
        self.assertIn("policy_version_mismatch", result["blockers"])
        self.assertEqual(result["checks"]["policy_version_matches_expected"]["actual"], "sprint8b")

    def test_detects_account_holdings_mismatch_snapshot_rows(self):
        now = datetime(2026, 5, 24, 15, 0, 0)
        result = evaluate_account_state_guard(
            {
                "recorded_at": now,
                "source_packet_type": "heartbeat",
                "contract_version": "v1",
                "account_status": "ok",
                "data_status": "ok",
                "policy_version": "sprint8a",
                "buying_power": 50000,
                "open_order_count": 0,
                "holdings_weights": {"SPY": 0.4},
                "raw_snapshot": {"explicit_account_state": True},
            },
            now=now,
            reference_weights={"SPY": 0.35},
        )

        self.assertEqual(result["status"], "would_block")
        self.assertIn("account_holdings_mismatch_snapshot_rows", result["blockers"])
        self.assertEqual(result["checks"]["holdings_match_snapshot_rows"]["actual"], 0.05)

    def test_missing_snapshot_is_diagnostic_in_observe_mode(self):
        result = evaluate_account_state_guard(None)

        self.assertEqual(result["status"], "would_block")
        self.assertTrue(result["allowed"])
        self.assertIn("missing_account_state_snapshot", result["blockers"])

    def test_default_config_normalizes_invalid_mode(self):
        config = default_account_state_guard_config({"mode": "surprise"})

        self.assertEqual(config["mode"], "observe")
        json.dumps(config)
        self.assertEqual(config["ok_account_statuses"], ["ok"])

    def test_pipeline_effect_observe_never_blocks(self):
        effect = account_state_guard_pipeline_effect(
            {"mode": "observe", "allowed": False, "status": "would_block"}
        )

        self.assertEqual(effect["pipeline_enforcement"], "observe_only")
        self.assertFalse(effect["should_block_pipeline"])
        self.assertEqual(effect["pipeline_effect_status"], "observe")

    def test_pipeline_effect_blocking_blocks_only_when_not_allowed(self):
        blocked = account_state_guard_pipeline_effect(
            {"mode": "blocking", "allowed": False, "status": "blocked"}
        )
        passed = account_state_guard_pipeline_effect(
            {"mode": "blocking", "allowed": True, "status": "pass"}
        )

        self.assertTrue(blocked["should_block_pipeline"])
        self.assertEqual(blocked["pipeline_effect_status"], "blocked")
        self.assertFalse(passed["should_block_pipeline"])
        self.assertEqual(passed["pipeline_effect_status"], "pass")


if __name__ == "__main__":
    unittest.main()
