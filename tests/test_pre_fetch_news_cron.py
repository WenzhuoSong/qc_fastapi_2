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

if __name__ == "__main__":
    unittest.main()
