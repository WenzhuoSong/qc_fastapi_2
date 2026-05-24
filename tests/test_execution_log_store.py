import importlib
import sys
import unittest
from unittest.mock import patch


def _load_utcnow_db_naive():
    sqlalchemy = type(sys)("sqlalchemy")
    sqlalchemy.select = lambda *args, **kwargs: None
    sqlalchemy.update = lambda *args, **kwargs: None
    sqlalchemy.desc = lambda value: value

    models = type(sys)("db.models")
    models.ExecutionLog = type("ExecutionLog", (), {})
    models.CommandLifecycleEvent = type("CommandLifecycleEvent", (), {})

    session = type(sys)("db.session")
    session.AsyncSessionLocal = object

    lifecycle = type(sys)("services.command_lifecycle")
    lifecycle.append_command_lifecycle_event = None

    with patch.dict(
        "sys.modules",
        {
            "sqlalchemy": sqlalchemy,
            "db": type(sys)("db"),
            "db.models": models,
            "db.session": session,
            "services.command_lifecycle": lifecycle,
        },
    ):
        return importlib.import_module("services.execution_log_store")._utcnow_db_naive


_utcnow_db_naive = _load_utcnow_db_naive()


class ExecutionLogStoreTests(unittest.TestCase):
    def test_qc_ack_timestamp_is_naive_for_db_column(self):
        value = _utcnow_db_naive()

        self.assertIsNone(value.tzinfo)


if __name__ == "__main__":
    unittest.main()
