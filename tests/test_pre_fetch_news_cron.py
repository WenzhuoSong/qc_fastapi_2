import unittest
from pathlib import Path


class PreFetchNewsCronContractTests(unittest.TestCase):
    def test_pre_fetch_news_is_audited_and_documented_as_24_7(self):
        source = Path("cron/pre_fetch_news.py").read_text()
        readme = Path("README.md").read_text()

        self.assertIn("from services.cron_audit import audit_cron_run", source)
        self.assertIn('audit_cron_run("pre_fetch_news")', source)
        self.assertIn("audit.add_rows(result.get(\"total_new\"))", source)
        self.assertIn("audit.set_summary(**result)", source)
        self.assertIn("50 */2 * * *", source)
        self.assertIn("24/7", source)
        self.assertIn("50 */2 * * *", readme)
        self.assertIn("24/7 multi-source news", readme)

    def test_operational_health_treats_news_as_real_time_event_stream(self):
        source = Path("services/operational_health.py").read_text()
        start = source.index('"news_cache":', source.index("checks = {"))
        end = source.index('"memory_write":', start)
        news_block = source[start:end]

        self.assertIn("news_cache_freshness_check(", news_block)
        self.assertNotIn("_trading_day_freshness_check(", news_block)
        self.assertIn("NEWS_CRON_SCHEDULE = \"50 */2 * * * UTC\"", source)
        self.assertIn("NEWS_CRON_ALLOWED_MISSED_RUNS = 2", source)

    def test_hourly_analysis_requires_news_ready_before_pipeline(self):
        source = Path("cron/hourly_analysis.py").read_text()

        self.assertIn("from services.trading_analysis_gate import evaluate_trading_analysis_gate", source)
        self.assertIn("evaluate_trading_analysis_gate()", source)
        self.assertIn("trading_analysis_gate", source)
        self.assertLess(source.index("evaluate_trading_analysis_gate()"), source.index("result = await run_full_pipeline"))

    def test_dynamic_scheduler_uses_same_trading_analysis_gate(self):
        source = Path("services/dynamic_scheduler.py").read_text()

        self.assertIn("from services.trading_analysis_gate import evaluate_trading_analysis_gate", source)
        self.assertIn("evaluate_trading_analysis_gate()", source)
        self.assertIn("trading_analysis_gate", source)
        self.assertLess(source.index("evaluate_trading_analysis_gate()"), source.index("result = await run_full_pipeline"))

    def test_pipeline_step_logs_news_evidence_path(self):
        source = Path("services/pipeline.py").read_text()

        self.assertIn("def _news_evidence_audit_summary", source)
        self.assertIn("def _news_context_audit_summary", source)
        self.assertIn('"news_evidence_summary": news_evidence_summary', source)
        self.assertIn('"news_context_summary": news_context_summary', source)
        self.assertIn("Trading-analysis entrypoints require a fresh news cache", source)

    def test_market_brief_reuses_shared_news_freshness_contract(self):
        source = Path("services/market_brief.py").read_text()

        self.assertIn("from services.operational_health import news_cache_freshness_check", source)
        self.assertIn("_attach_news_freshness", source)
        self.assertNotIn("older than 4 hours considered stale", source)


if __name__ == "__main__":
    unittest.main()
