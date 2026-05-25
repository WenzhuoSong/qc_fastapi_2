import importlib
import sys
import unittest
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch


def _load_cron_audit_run():
    sqlalchemy = type(sys)("sqlalchemy")
    sqlalchemy.select = lambda *args, **kwargs: None

    models = type(sys)("db.models")
    models.CronRunLog = type("CronRunLog", (), {})

    session = type(sys)("db.session")
    session.AsyncSessionLocal = object

    with patch.dict(
        "sys.modules",
        {
            "sqlalchemy": sqlalchemy,
            "db": type(sys)("db"),
            "db.models": models,
            "db.session": session,
        },
    ):
        module = importlib.import_module("services.cron_audit")
        return module.CronAuditRun


CronAuditRun = _load_cron_audit_run()


class CronAuditRunTest(unittest.TestCase):
    def test_rows_summary_and_skip_status(self):
        audit = CronAuditRun("playground_analysis")

        audit.add_rows(3)
        audit.add_rows("2")
        audit.add_rows("bad")
        audit.set_summary(status="ok", none_value=None)
        audit.mark_skipped("disabled")

        self.assertEqual(audit.rows_written, 5)
        self.assertEqual(audit.summary["status"], "ok")
        self.assertNotIn("none_value", audit.summary)
        self.assertEqual(audit.status, "skipped")
        self.assertEqual(audit.summary["skip_reason"], "disabled")

    def test_summary_values_are_json_safe(self):
        audit = CronAuditRun("hourly_analysis")

        audit.set_summary(
            decimal_value=Decimal("1.25"),
            dt=datetime(2026, 5, 25, 9, 3),
            day=date(2026, 5, 25),
            tags={"b", "a"},
            nested={"x": Decimal("2.5")},
        )

        self.assertEqual(audit.summary["decimal_value"], 1.25)
        self.assertEqual(audit.summary["dt"], "2026-05-25T09:03:00")
        self.assertEqual(audit.summary["day"], "2026-05-25")
        self.assertEqual(audit.summary["tags"], ["a", "b"])
        self.assertEqual(audit.summary["nested"], {"x": 2.5})


if __name__ == "__main__":
    unittest.main()
