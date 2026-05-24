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
        return importlib.import_module("services.execution_log_store")._utcnow_db_naive


_utcnow_db_naive = _load_utcnow_db_naive()


class ExecutionLogStoreTests(unittest.TestCase):
    def test_qc_ack_timestamp_is_naive_for_db_column(self):
        value = _utcnow_db_naive()

        self.assertIsNone(value.tzinfo)


if __name__ == "__main__":
    unittest.main()
