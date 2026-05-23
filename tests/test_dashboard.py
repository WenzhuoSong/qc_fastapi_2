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
        self.assertIn('"ticker_role": policy.get("ticker_role")', function_source)
        self.assertIn('"policy_cap_applied": policy.get("policy_cap_applied")', function_source)
        self.assertIn('"qc_status": raw.get("qc_status")', function_source)
        self.assertIn('"entered_via_hedge_path": hedge_path.get("entered_via_hedge_path")', function_source)

    def test_dashboard_reads_stage_telemetry_from_agent_step_log(self):
        source = Path("dashboard/app.py").read_text()
        pipeline_source = Path("services/pipeline.py").read_text()

        self.assertIn("AgentStepLog", source)
        self.assertIn("def _latest_stage_metrics", source)
        self.assertIn('"prompt_tokens": (row.token_usage or {}).get("prompt_tokens")', source)
        self.assertIn("Pipeline Stage Telemetry", source)
        self.assertIn('"6c_pc_eval"', pipeline_source)
        self.assertIn('"6cb_final_policy_cap"', pipeline_source)
        self.assertNotIn('"6c_portfolio_construction_evaluation"', pipeline_source)
        self.assertNotIn('"6cb_final_execution_policy_cap"', pipeline_source)

    def test_dashboard_surfaces_portfolio_construction_evaluation(self):
        source = Path("dashboard/app.py").read_text()

        self.assertIn("portfolio_construction_evaluation", source)
        self.assertIn("def _compact_portfolio_construction_evaluation", source)
        self.assertIn("Portfolio Construction Evaluation", source)
        self.assertIn("portfolio_construction_readiness", source)
        self.assertIn("Portfolio Construction Readiness", source)
        self.assertIn("portfolio_construction_promotion_gate", source)
        self.assertIn("Portfolio Construction Promotion Gate", source)

    def test_dashboard_surfaces_data_quality_audit_trend(self):
        source = Path("dashboard/app.py").read_text()

        self.assertIn("DATA_QUALITY_AUDIT_NAME", source)
        self.assertIn("def _data_quality_audit_trend", source)
        self.assertIn("to_regclass('public.data_quality_audit')", source)
        self.assertIn("Data Quality Audit Trend", source)
        self.assertIn("def _render_data_quality_audit", source)
        self.assertIn('"unit_risk_count"', source)
        self.assertIn('"high_drift_classes"', source)

    def test_qc_yfinance_audit_cron_writes_audit_and_cron_telemetry(self):
        source = Path("cron/qc_yfinance_feature_audit.py").read_text()

        self.assertIn('audit_cron_run("qc_yfinance_feature_audit")', source)
        self.assertIn("run_audit(", source)
        self.assertIn("write_db=True", source)
        self.assertIn("audit.add_rows(1)", source)
        self.assertIn("unit_risk_count", source)
        self.assertIn("QC_YFINANCE_AUDIT_LOOKBACK_DAYS", source)


if __name__ == "__main__":
    unittest.main()
