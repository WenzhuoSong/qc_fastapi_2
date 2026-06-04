import importlib
import sys
import unittest
from unittest.mock import patch


def _load_reporter_module():
    sqlalchemy = type(sys)("sqlalchemy")
    sqlalchemy.select = lambda *args, **kwargs: None
    sqlalchemy.desc = lambda value: value

    models = type(sys)("db.models")
    models.PortfolioTimeseries = type("PortfolioTimeseries", (), {})
    models.ExecutionLog = type("ExecutionLog", (), {})

    session = type(sys)("db.session")
    session.AsyncSessionLocal = object

    notify_tools = type(sys)("tools.notify_tools")
    notify_tools.tool_send_telegram = None

    execution_log_store = type(sys)("services.execution_log_store")
    execution_log_store.summarize_execution_activity_rows = lambda rows: {
        "command_count": 0,
        "gross_turnover": 0.0,
        "ordinary_command_count": 0,
        "risk_reduce_command_count": 0,
        "risk_reduce_gross_turnover": 0.0,
    }

    with patch.dict(
        "sys.modules",
        {
            "sqlalchemy": sqlalchemy,
            "db": type(sys)("db"),
            "db.models": models,
            "db.session": session,
            "tools.notify_tools": notify_tools,
            "services.execution_log_store": execution_log_store,
        },
    ):
        return importlib.import_module("agents.reporter")


reporter = _load_reporter_module()


class ReporterDailyReportTests(unittest.TestCase):
    def test_daily_report_separates_cap_usage_from_execution_log_rows(self):
        text = reporter._format_daily_report({
            "total_value": 133572.0,
            "daily_pnl_pct": -0.0009,
            "drawdown": 0.0034,
            "regime_label": "trending_bull",
            "win_rate_30d": 0.62,
            "commands_used_today": 2,
            "gross_turnover_today": 0.42,
            "ordinary_commands_today": 1,
            "risk_reduce_commands_today": 1,
            "risk_reduce_turnover_today": 0.05,
            "execution_log_rows_today": 9,
            "preflight_blocked_today": 5,
        })

        self.assertIn("Commands used for cap  2 (ordinary 1, risk-reduce 1)", text)
        self.assertIn("Turnover used for cap  42.00%", text)
        self.assertIn("Risk-reduce turnover  5.00%", text)
        self.assertIn("Execution log rows  9 (5 preflight blocked)", text)
        self.assertNotIn("Executions today", text)

    def test_preflight_blocked_row_detection_uses_not_sent_rejected_reason(self):
        row = type(
            "Row",
            (),
            {
                "status": "rejected",
                "qc_status": "not_sent",
                "command_payload": {"reason": "blocked_by_command_preflight"},
            },
        )()

        self.assertTrue(reporter._is_preflight_blocked_row(row))

    def test_qc_rejected_row_is_not_preflight_blocked(self):
        row = type(
            "Row",
            (),
            {
                "status": "rejected",
                "qc_status": "rejected",
                "command_payload": {"reason": "qc_rejected"},
            },
        )()

        self.assertFalse(reporter._is_preflight_blocked_row(row))


if __name__ == "__main__":
    unittest.main()
