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

        self.assertIn("_news_cache_freshness_check(", news_block)
        self.assertNotIn("_trading_day_freshness_check(", news_block)
        self.assertIn("NEWS_CRON_SCHEDULE = \"50 */2 * * * UTC\"", source)
        self.assertIn("NEWS_CRON_ALLOWED_MISSED_RUNS = 2", source)

    def test_hourly_analysis_requires_news_ready_before_pipeline(self):
        source = Path("cron/hourly_analysis.py").read_text()

        self.assertIn("from services.operational_health import build_operational_health_snapshot", source)
        self.assertIn("news_cache_not_ready", source)
        self.assertIn('news_check.get("state") != "ok"', source)
        self.assertLess(source.index("news_cache_not_ready"), source.index("result = await run_full_pipeline"))

    def test_pipeline_step_logs_news_evidence_path(self):
        source = Path("services/pipeline.py").read_text()

        self.assertIn("def _news_evidence_audit_summary", source)
        self.assertIn("def _news_context_audit_summary", source)
        self.assertIn('"news_evidence_summary": news_evidence_summary', source)
        self.assertIn('"news_context_summary": news_context_summary', source)
        self.assertIn("hourly analysis cron requires a fresh news cache", source)


if __name__ == "__main__":
    unittest.main()
