from datetime import datetime, timedelta
import unittest

from services.reconciliation_guard import (
    calculate_reconciliation_drift,
    default_reconciliation_guard_config,
    evaluate_reconciliation_guard,
    format_reconciliation_guard_alert,
)


class ReconciliationGuardTests(unittest.TestCase):
    def _snapshot(self, holdings=None, **overrides):
        data = {
            "id": 1,
            "recorded_at": datetime(2026, 6, 6, 14, 30),
            "is_market_open": True,
            "total_value": 100_000.0,
            "holdings_weights": holdings or {"SPY": 0.1005, "QQQ": 0.2, "CASH": 0.6995},
        }
        data.update(overrides)
        return data

    def _command(self, target=None, state="filled", **overrides):
        data = {
            "command_id": "analysis_244",
            "correlation_id": "analysis_244",
            "lifecycle_state": state,
            "qc_status": "filled",
            "submitted_at": datetime(2026, 6, 6, 14, 20),
            "latest_qc_ack_at": datetime(2026, 6, 6, 14, 25),
            "policy_version": "sprint8a",
            "target_weights": target or {"SPY": 0.1, "QQQ": 0.2, "CASH": 0.7},
            "feedback_trust": {
                "status": "trusted_for_reconciliation",
                "trusted_feedback": True,
                "trusted_for_reconciliation": True,
            },
        }
        data.update(overrides)
        return data

    def test_passes_when_expected_and_actual_inside_tolerance(self):
        result = evaluate_reconciliation_guard(
            snapshot=self._snapshot(),
            command=self._command(),
            now=datetime(2026, 6, 6, 14, 31),
        )

        self.assertEqual(result["status"], "pass")
        self.assertFalse(result["should_block_current_run"])

    def test_blocks_current_run_when_risk_asset_drift_exceeds_tolerance(self):
        result = evaluate_reconciliation_guard(
            snapshot=self._snapshot({"SPY": 0.14, "QQQ": 0.2, "CASH": 0.66}),
            command=self._command({"SPY": 0.1, "QQQ": 0.2, "CASH": 0.7}),
            now=datetime(2026, 6, 6, 14, 31),
        )

        self.assertEqual(result["status"], "diverged")
        self.assertTrue(result["should_block_current_run"])
        self.assertFalse(result["should_set_reconciliation_halt"])
        self.assertEqual(result["drift_tickers"][0]["ticker"], "SPY")

    def test_cash_residual_alone_does_not_diverge(self):
        drift = calculate_reconciliation_drift(
            {"SPY": 0.1, "CASH": 0.9},
            {"SPY": 0.1, "CASH": 0.7},
            total_value=100_000.0,
        )

        self.assertEqual(drift["drift_tickers"], [])

    def test_whole_share_rounding_tolerance_allows_untradeable_high_price_residual(self):
        drift = calculate_reconciliation_drift(
            {"QQQ": 0.040955, "CASH": 0.959045},
            {"QQQ": 0.0378, "CASH": 0.9622},
            total_value=133_020.14,
            prices={"QQQ": 717.56},
        )

        self.assertEqual(drift["drift_tickers"], [])
        self.assertAlmostEqual(drift["raw_max_abs_diff"], 0.003155, places=6)

    def test_whole_share_rounding_tolerance_still_blocks_multi_share_drift(self):
        drift = calculate_reconciliation_drift(
            {"QQQ": 0.040955, "CASH": 0.959045},
            {"QQQ": 0.0300, "CASH": 0.9700},
            total_value=133_020.14,
            prices={"QQQ": 717.56},
        )

        self.assertEqual(drift["drift_tickers"][0]["ticker"], "QQQ")
        self.assertGreater(drift["drift_tickers"][0]["threshold"], 0.005)

    def test_evaluate_guard_uses_snapshot_prices_for_whole_share_rounding(self):
        result = evaluate_reconciliation_guard(
            snapshot=self._snapshot(
                {"QQQ": 0.0378, "CASH": 0.9622},
                total_value=133_020.14,
                prices={"QQQ": 717.56},
            ),
            command=self._command({"QQQ": 0.040955, "CASH": 0.959045}),
            now=datetime(2026, 6, 6, 14, 31),
        )

        self.assertEqual(result["status"], "pass")
        self.assertFalse(result["should_block_current_run"])

    def test_partial_command_returns_in_flight_without_halt(self):
        result = evaluate_reconciliation_guard(
            snapshot=self._snapshot({"SPY": 0.12, "CASH": 0.88}),
            command=self._command(
                {"SPY": 0.2, "CASH": 0.8},
                state="partial",
                latest_qc_ack_at=datetime(2026, 6, 6, 14, 29),
                feedback_trust={"status": "partial", "trusted_feedback": True, "trusted_for_reconciliation": False},
            ),
            now=datetime(2026, 6, 6, 14, 31),
        )

        self.assertEqual(result["status"], "in_flight")
        self.assertTrue(result["should_block_current_run"])
        self.assertFalse(result["should_set_reconciliation_halt"])

    def test_stuck_in_flight_returns_warning_status_not_divergence(self):
        result = evaluate_reconciliation_guard(
            snapshot=self._snapshot(),
            command=self._command(
                state="pending_ack",
                submitted_at=datetime(2026, 6, 6, 14, 0),
                latest_qc_ack_at=None,
            ),
            config={"max_pending_ack_age_seconds": 60},
            now=datetime(2026, 6, 6, 14, 31),
        )

        self.assertEqual(result["status"], "stuck_in_flight")
        self.assertTrue(result["should_block_current_run"])
        self.assertIn("Elapsed:", format_reconciliation_guard_alert(result))

    def test_untrusted_feedback_blocks_current_run_but_does_not_auto_halt(self):
        result = evaluate_reconciliation_guard(
            snapshot=self._snapshot(),
            command=self._command(
                state="pending_reconcile",
                feedback_trust={
                    "status": "pending_reconcile",
                    "reason": "qc_feedback_incomplete_for_hard_reconciliation",
                    "trusted_feedback": False,
                    "trusted_for_reconciliation": False,
                },
            ),
            config={"auto_set_reconciliation_halt": True},
            now=datetime(2026, 6, 6, 14, 31),
        )

        self.assertEqual(result["status"], "untrusted_feedback")
        self.assertTrue(result["should_block_current_run"])
        self.assertFalse(result["should_set_reconciliation_halt"])

    def test_market_closed_skips_reconciliation(self):
        result = evaluate_reconciliation_guard(
            snapshot=self._snapshot(is_market_open=False),
            command=self._command(),
            now=datetime(2026, 6, 6, 14, 31),
        )

        self.assertEqual(result["status"], "skipped_market_closed")
        self.assertFalse(result["should_block_current_run"])

    def test_observe_mode_reports_without_blocking(self):
        result = evaluate_reconciliation_guard(
            snapshot=self._snapshot({"SPY": 0.14, "CASH": 0.86}),
            command=self._command({"SPY": 0.1, "CASH": 0.9}),
            config={"mode": "observe"},
            now=datetime(2026, 6, 6, 14, 31),
        )

        self.assertEqual(result["status"], "diverged")
        self.assertFalse(result["should_block_current_run"])
        self.assertEqual(result["execution_effect"], "diagnostic_only")

    def test_default_config_is_blocking_and_cash_residual(self):
        cfg = default_reconciliation_guard_config({})

        self.assertEqual(cfg["mode"], "blocking")
        self.assertTrue(cfg["ignore_cash"])
        self.assertEqual(cfg["cash_tolerance_mode"], "residual")
        self.assertTrue(cfg["whole_share_rounding_tolerance_enabled"])


if __name__ == "__main__":
    unittest.main()
