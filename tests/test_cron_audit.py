import sys
import types
import unittest


def _install_import_stubs() -> None:
    sqlalchemy = types.ModuleType("sqlalchemy")
    sqlalchemy.select = lambda *args, **kwargs: None
    sys.modules["sqlalchemy"] = sqlalchemy

    sys.modules.setdefault("db", types.ModuleType("db"))

    models = types.ModuleType("db.models")
    models.CronRunLog = type("CronRunLog", (), {})
    sys.modules["db.models"] = models

    session = types.ModuleType("db.session")
    session.AsyncSessionLocal = object
    sys.modules["db.session"] = session


_install_import_stubs()
sys.modules.pop("services.cron_audit", None)

from services.cron_audit import CronAuditRun  # noqa: E402

for module_name in ("sqlalchemy", "db.models", "db.session"):
    sys.modules.pop(module_name, None)


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
