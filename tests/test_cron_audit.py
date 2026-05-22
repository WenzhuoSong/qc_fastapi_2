import importlib
import sys
import unittest
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


if __name__ == "__main__":
    unittest.main()
