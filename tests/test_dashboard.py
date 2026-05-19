import unittest
from pathlib import Path


class DashboardTests(unittest.TestCase):
    def test_displayed_dashboard_content_is_not_truncated(self):
        source = Path("dashboard/app.py").read_text()
        governance_start = source.index("def _compact_governance")
        ledger_start = source.index("def _compact_ledger")
        function_source = source[governance_start:ledger_start]

        self.assertIn('"position_explanations": _sort_by_current_weight(portfolio.get("position_explanations") or [])', function_source)
        self.assertIn("def _sort_by_current_weight", function_source)
        self.assertNotIn('"position_explanations": (portfolio.get("position_explanations") or [])[:5]', function_source)
        self.assertNotIn("[:4]", source)
        self.assertNotIn("[:5]", source)
        self.assertNotIn("[:6]", source)
        self.assertNotIn("[:160]", source)
        self.assertNotIn("[:180]", source)
        self.assertNotIn("CronRunLog.started_at)).limit", source)


if __name__ == "__main__":
    unittest.main()
