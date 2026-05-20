import unittest
from pathlib import Path


class DashboardTests(unittest.TestCase):
    def test_displayed_dashboard_content_is_not_truncated(self):
        source = Path("dashboard/app.py").read_text()
        governance_start = source.index("def _compact_governance")
        ledger_start = source.index("def _compact_ledger")
        function_source = source[governance_start:ledger_start]

        self.assertIn("_enrich_position_explanations_from_ledger", function_source)
        self.assertIn('"position_explanations": _sort_by_current_weight(explanations)', function_source)
        self.assertIn('"final_explanation"', source)
        self.assertIn('"llm_effect"', source)
        self.assertIn("def _sort_by_current_weight", function_source)
        self.assertNotIn('"position_explanations": (portfolio.get("position_explanations") or [])[:5]', function_source)
        self.assertNotIn("[:4]", source)
        self.assertNotIn("[:5]", source)
        self.assertNotIn("[:6]", source)
        self.assertNotIn("[:160]", source)
        self.assertNotIn("[:180]", source)
        self.assertNotIn("CronRunLog.started_at)).limit", source)

    def test_dashboard_ledger_uses_real_ticker_rows_and_lifecycle_fields(self):
        source = Path("dashboard/app.py").read_text()
        ledger_start = source.index("def _compact_ledger")
        render_start = source.index("def render_dashboard")
        function_source = source[ledger_start:render_start]

        self.assertIn('rows = _ledger_rows_from_tickers(ledger.get("tickers") or {})', function_source)
        self.assertIn('"target_builder_target": lifecycle.get("target_builder_target")', function_source)
        self.assertIn('"diagnostic_llm_target": lifecycle.get("diagnostic_llm_target")', function_source)
        self.assertIn('"validated_advisory_delta": lifecycle.get("validated_advisory_delta")', function_source)
        self.assertIn('"advisory_validator_result": advisory.get("validator_result")', function_source)

    def test_dashboard_reads_stage_telemetry_from_agent_step_log(self):
        source = Path("dashboard/app.py").read_text()

        self.assertIn("AgentStepLog", source)
        self.assertIn("def _latest_stage_metrics", source)
        self.assertIn('"prompt_tokens": (row.token_usage or {}).get("prompt_tokens")', source)
        self.assertIn("Pipeline Stage Telemetry", source)


if __name__ == "__main__":
    unittest.main()
