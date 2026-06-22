import unittest
import json
import re
from datetime import date, datetime
from pathlib import Path

from services.newbase_monitoring import (
    ARCHITECTURE_INVARIANTS,
    build_newbase_operator_snapshot,
    build_newbase_registry_record,
    build_strategy_live_snapshot_record,
    format_newbase_operator_snapshot_text,
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
                "recorded_at": datetime(2026, 6, 20, 20, 0),
                "daily_return": 0.01,
                "benchmark_primary_return": 0.005,
                "benchmark_secondary_return": 0.004,
                "holdings": [{"ticker": "AAPL"}],
            },
            {
                "snapshot_uid": "b",
                "strategy_id": "newbase",
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
        self.assertFalse(snapshot["architecture_invariants"]["monitoring_has_hands"])
        self.assertEqual(snapshot["benchmark_primary"], "QQQ")
        self.assertGreater(snapshot["headline"]["live_newbase_vs_qqq_cumulative_excess"], 0)
        self.assertIn("newBase vs QQQ cumulative excess", text.splitlines()[1])
        self.assertIn("operator_action=review_only", text)

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


if __name__ == "__main__":
    unittest.main()
