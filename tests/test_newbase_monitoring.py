import unittest
import json
import re
from datetime import UTC, date, datetime
from pathlib import Path

from services.newbase_monitoring import (
    ARCHITECTURE_INVARIANTS,
    CURRENT_NEWBASE_ALGORITHM_VERSION,
    build_newbase_operator_snapshot,
    build_newbase_registry_record,
    build_strategy_live_snapshot_record,
    evaluate_newbase_full_auto_monitor,
    format_newbase_operator_snapshot_text,
    is_newbase_observer_strategy,
)


class NewBaseMonitoringTests(unittest.TestCase):
    def test_registry_is_descriptive_not_execution_authority(self):
        record = build_newbase_registry_record()

        self.assertEqual(record["strategy_id"], "newbase")
        self.assertEqual(record["source"], "QuantConnect")
        self.assertEqual(record["benchmark_primary"], "QQQ")
        self.assertEqual(record["benchmark_secondary"], "SPY")
        self.assertTrue(record["review_only"])
        self.assertEqual(record["execution_authority"], "none")
        self.assertEqual(record["target_weight_mutation"], "none")
        self.assertEqual(record["display_name"], "newBase stronger252 target3")
        self.assertEqual(
            record["expected_profile"]["algorithm_version"],
            CURRENT_NEWBASE_ALGORITHM_VERSION,
        )
        self.assertEqual(record["expected_profile"]["strategy_variant"], "stronger252_target3")
        self.assertAlmostEqual(
            record["expected_profile"]["absolute_backtest_profile"]["full_2010_2026"]["cagr"],
            0.24425,
        )
        self.assertAlmostEqual(
            record["expected_profile"]["absolute_backtest_profile"]["recent_2023_2026"]["beta"],
            0.868,
        )
        self.assertTrue(record["expected_profile"]["operator_contract"]["red_flags_are_review_only"])
        self.assertEqual(
            record["expected_profile"]["operator_contract"]["automatic_trade_response"],
            "forbidden",
        )

    def test_live_snapshot_payload_normalizes_qc_newbase_export(self):
        example_payload = json.loads(
            Path("examples/newbase_live_snapshot_example.json").read_text(encoding="utf-8")
        )
        record = build_strategy_live_snapshot_record(
            example_payload,
            qc_snapshot_id=42,
        )

        self.assertEqual(record["snapshot_uid"], "newbase:2026-06-22:close:example")
        self.assertEqual(record["strategy_id"], "newbase")
        self.assertEqual(record["algorithm_version"], CURRENT_NEWBASE_ALGORITHM_VERSION)
        self.assertEqual(record["qc_snapshot_id"], 42)
        self.assertEqual(record["trading_date"], date(2026, 6, 22))
        self.assertEqual(record["benchmark_primary"], "QQQ")
        self.assertEqual(record["benchmark_secondary"], "SPY")
        self.assertAlmostEqual(record["benchmark_primary_return"], 0.003)
        self.assertEqual(record["diagnostics"]["architecture_invariants"], ARCHITECTURE_INVARIANTS)
        self.assertEqual(record["diagnostics"]["architecture_invariants"]["execution_authority"], "none")

    def test_operator_snapshot_leads_with_newbase_vs_qqq_and_has_no_hands(self):
        rows = [
            {
                "snapshot_uid": "a",
                "strategy_id": "newbase",
                "algorithm_version": CURRENT_NEWBASE_ALGORITHM_VERSION,
                "recorded_at": datetime(2026, 6, 20, 20, 0),
                "daily_return": 0.01,
                "benchmark_primary_return": 0.005,
                "benchmark_secondary_return": 0.004,
                "holdings": [{"ticker": "AAPL"}],
            },
            {
                "snapshot_uid": "b",
                "strategy_id": "newbase",
                "algorithm_version": CURRENT_NEWBASE_ALGORITHM_VERSION,
                "recorded_at": datetime(2026, 6, 21, 20, 0),
                "daily_return": 0.02,
                "benchmark_primary_return": 0.01,
                "benchmark_secondary_return": 0.006,
                "current_drawdown": -0.02,
                "turnover": 0.01,
                "fees": 2.0,
                "holdings": [{"ticker": "AAPL"}, {"ticker": "MSFT"}],
                "orders": [{"ticker": "MSFT"}],
                "fills": [{"ticker": "MSFT"}],
            },
        ]

        snapshot = build_newbase_operator_snapshot(rows, as_of=datetime(2026, 6, 22, 0, 0))
        text = format_newbase_operator_snapshot_text(snapshot)

        self.assertEqual(snapshot["execution_authority"], "none")
        self.assertEqual(snapshot["target_weight_mutation"], "none")
        self.assertTrue(snapshot["review_only"])
        self.assertEqual(snapshot["algorithm_version"], CURRENT_NEWBASE_ALGORITHM_VERSION)
        self.assertFalse(snapshot["architecture_invariants"]["monitoring_has_hands"])
        self.assertEqual(snapshot["benchmark_primary"], "QQQ")
        self.assertGreater(snapshot["headline"]["live_newbase_vs_qqq_cumulative_excess"], 0)
        self.assertIn("newBase vs QQQ cumulative excess", text.splitlines()[1])
        self.assertIn(CURRENT_NEWBASE_ALGORITHM_VERSION, text.splitlines()[0])
        self.assertIn("operator_action=review_only", text)

    def test_operator_snapshot_filters_prior_algorithm_versions(self):
        rows = [
            {
                "snapshot_uid": "legacy",
                "strategy_id": "newbase",
                "algorithm_version": "newBase_live_fastapi_v1",
                "recorded_at": datetime(2026, 6, 20, 20, 0),
                "daily_return": 0.20,
                "benchmark_primary_return": 0.0,
            },
            {
                "snapshot_uid": "current-1",
                "strategy_id": "newbase",
                "algorithm_version": CURRENT_NEWBASE_ALGORITHM_VERSION,
                "recorded_at": datetime(2026, 6, 21, 20, 0),
                "daily_return": 0.01,
                "benchmark_primary_return": 0.005,
            },
            {
                "snapshot_uid": "current-2",
                "strategy_id": "newbase",
                "algorithm_version": CURRENT_NEWBASE_ALGORITHM_VERSION,
                "recorded_at": datetime(2026, 6, 22, 20, 0),
                "daily_return": 0.01,
                "benchmark_primary_return": 0.005,
            },
        ]

        snapshot = build_newbase_operator_snapshot(rows, as_of=datetime(2026, 6, 23, 0, 0))

        self.assertEqual(snapshot["algorithm_version"], CURRENT_NEWBASE_ALGORITHM_VERSION)
        self.assertEqual(snapshot["sample_count_all_versions"], 3)
        self.assertEqual(snapshot["sample_count"], 2)
        self.assertEqual(snapshot["ignored_prior_version_count"], 1)
        self.assertIn("newBase_live_fastapi_v1", snapshot["observed_algorithm_versions"])
        self.assertLess(snapshot["headline"]["live_newbase_vs_qqq_cumulative_excess"], 0.02)

    def test_operator_snapshot_flags_wrong_algorithm_version(self):
        rows = [
            {
                "snapshot_uid": "legacy",
                "strategy_id": "newbase",
                "algorithm_version": "newBase_live_fastapi_v1",
                "recorded_at": datetime(2026, 6, 21, 20, 0),
                "daily_return": 0.01,
                "benchmark_primary_return": 0.005,
            }
        ]

        snapshot = build_newbase_operator_snapshot(rows, as_of=datetime(2026, 6, 22, 0, 0))
        flags = {flag["flag"] for flag in snapshot["review_flags"]}

        self.assertEqual(snapshot["algorithm_version"], "newBase_live_fastapi_v1")
        self.assertIn("algorithm_version_mismatch_review", flags)

    def test_operator_snapshot_ignores_stale_registry_profile(self):
        rows = [
            {
                "snapshot_uid": "current",
                "strategy_id": "newbase",
                "algorithm_version": CURRENT_NEWBASE_ALGORITHM_VERSION,
                "recorded_at": datetime(2026, 6, 21, 20, 0),
                "daily_return": 0.01,
                "benchmark_primary_return": 0.005,
                "rolling_beta_primary": 0.62,
            }
        ]
        stale_registry = {
            "expected_profile": {
                "schema_version": "newbase_expected_profile_v1",
                "absolute_backtest_profile": {
                    "recent_2023_2026": {"beta": 0.10}
                },
                "monitoring_thresholds": {"rolling_beta_review_drift": 0.25},
            }
        }

        snapshot = build_newbase_operator_snapshot(
            rows,
            registry=stale_registry,
            as_of=datetime(2026, 6, 22, 0, 0),
        )
        flags = {flag["flag"] for flag in snapshot["review_flags"]}

        self.assertEqual(snapshot["expected_algorithm_version"], CURRENT_NEWBASE_ALGORITHM_VERSION)
        self.assertNotIn("beta_profile_drift_review", flags)

    def test_webhook_and_ops_are_observer_only(self):
        webhook = Path("api/webhook.py").read_text(encoding="utf-8")
        ops = Path("api/ops.py").read_text(encoding="utf-8")
        docs = Path("docs/newbase_fastapi_observer_contract.md").read_text(encoding="utf-8")

        self.assertIn('"newbase_live_snapshot"', webhook)
        self.assertIn("persist_strategy_live_snapshot", webhook)
        self.assertIn('"/newbase/latest"', ops)
        self.assertIn("Monitoring has eyes, not hands", docs)
        self.assertNotIn("tool_submit", docs)

        handler_match = re.search(
            r"async def _process_newbase_live_snapshot\(.*?(?=\n\nasync def )",
            webhook,
            flags=re.S,
        )
        self.assertIsNotNone(handler_match)
        handler_body = handler_match.group(0)
        self.assertIn("persist_strategy_live_snapshot", handler_body)
        self.assertNotIn("_process_market_snapshot", handler_body)
        self.assertNotIn("append_reconciliation_from_account_snapshot", handler_body)
        self.assertNotIn("tool_emergency_liquidate", handler_body)
        self.assertNotIn("tool_send_telegram", handler_body)

    def test_newbase_strategy_aliases_route_to_observer_mode(self):
        self.assertTrue(is_newbase_observer_strategy("newbase"))
        self.assertTrue(is_newbase_observer_strategy("newbase_observer_v1"))
        self.assertFalse(is_newbase_observer_strategy("momentum_lite_v1"))

    def test_newbase_monitor_cron_is_observer_only(self):
        source = Path("cron/newbase_monitor.py").read_text(encoding="utf-8")

        self.assertIn('audit_cron_run("newbase_monitor")', source)
        self.assertIn("is_active_newbase_observer", source)
        self.assertIn("run_newbase_full_auto_monitor", source)
        self.assertIn('execution_authority="none"', source)
        self.assertIn('target_weight_mutation="none"', source)
        self.assertNotIn("tool_send_weight_command", source)
        self.assertNotIn("tool_send_cancel_orders_command", source)
        self.assertNotIn("tool_emergency_liquidate", source)

    def test_newbase_mode_blocks_legacy_qc_command_tools(self):
        source = Path("tools/qc_tools.py").read_text(encoding="utf-8")

        self.assertIn("_blocked_by_newbase_observer", source)
        self.assertIn('await _blocked_by_newbase_observer("SetWeights")', source)
        self.assertIn('await _blocked_by_newbase_observer("PolicySync")', source)
        self.assertIn('await _blocked_by_newbase_observer("CancelOrders")', source)
        self.assertIn('await _blocked_by_newbase_observer("EmergencyLiquidate")', source)
        self.assertIn('"error": "newbase_observer_only"', source)
        self.assertIn('"execution_authority": "none"', source)

    def test_newbase_mode_disables_high_risk_telegram_commands(self):
        source = Path("services/telegram_commands.py").read_text(encoding="utf-8")

        self.assertIn("_HIGH_RISK_COMMANDS_DISABLED_IN_NEWBASE", source)
        self.assertIn('"/confirm"', source)
        self.assertIn('"/cancel_orders"', source)
        self.assertIn('"/approve_strategy"', source)
        self.assertIn('"/skip_strategy"', source)
        self.assertIn('"/force_reconcile"', source)
        self.assertIn("await is_active_newbase_observer()", source)
        self.assertIn("active_strategy=newbase", source)
        self.assertIn("control_mode", source)

    def test_newbase_mode_blocks_emergency_auto_liquidation(self):
        source = Path("api/webhook.py").read_text(encoding="utf-8")

        self.assertIn("newbase_observer_only = await is_active_newbase_observer(db)", source)
        self.assertIn("Auto-liquidate: DISABLED (newBase observer-only)", source)
        self.assertIn("and not newbase_observer_only", source)

    def test_morning_health_reports_newbase_observer_status(self):
        source = Path("cron/morning_health.py").read_text(encoding="utf-8")

        self.assertIn("run_newbase_full_auto_monitor", source)
        self.assertIn("newBase observer:", source)
        self.assertIn('execution_authority="none"', source)
        self.assertIn('target_weight_mutation="none"', source)

    def test_full_auto_monitor_allows_prior_snapshot_before_post_close_due(self):
        monitor = evaluate_newbase_full_auto_monitor(
            {"as_of_recorded_at": "2026-06-23T20:10:00"},
            now=datetime(2026, 6, 24, 13, 50, tzinfo=UTC),
        )

        self.assertEqual(monitor["status"], "ok")
        self.assertFalse(monitor["should_alert"])
        self.assertEqual(monitor["expected_snapshot_trading_date"], "2026-06-23")
        self.assertEqual(monitor["execution_authority"], "none")
        self.assertEqual(monitor["target_weight_mutation"], "none")

    def test_full_auto_monitor_requires_today_after_post_close_due(self):
        monitor = evaluate_newbase_full_auto_monitor(
            {"as_of_recorded_at": "2026-06-23T20:10:00"},
            now=datetime(2026, 6, 24, 22, 0, tzinfo=UTC),
        )

        self.assertEqual(monitor["status"], "stale")
        self.assertTrue(monitor["should_alert"])
        self.assertEqual(monitor["reason"], "newbase_live_snapshot_stale")
        self.assertEqual(monitor["expected_snapshot_trading_date"], "2026-06-24")

    def test_pipeline_routes_newbase_observer_before_legacy_guards(self):
        source = Path("services/pipeline.py").read_text(encoding="utf-8")

        newbase_branch = source.index('fastapi_control_mode") == "newbase_observer_only"')
        account_guard = source.index("account_state_guard = await load_latest_account_state_guard")
        executor = source.index("result = await run_executor_async")

        self.assertLess(newbase_branch, account_guard)
        self.assertLess(newbase_branch, executor)
        self.assertIn("run_newbase_full_auto_monitor", source)
        self.assertIn("require_trading_gate and not newbase_observer_requested", source)
        self.assertIn("if not await is_active_newbase_observer():", source)

    def test_seed_defaults_newbase_observer_mode(self):
        source = Path("db/seed.py").read_text(encoding="utf-8")

        self.assertIn('"authorization_mode": {"value": "FULL_AUTO"}', source)
        self.assertIn('"active_strategy": {"value": "newbase"}', source)
        self.assertIn("FastAPI/Railway still has no execution authority", source)


if __name__ == "__main__":
    unittest.main()
