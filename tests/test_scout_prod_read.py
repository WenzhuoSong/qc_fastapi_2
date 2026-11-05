import os
import unittest
from unittest import mock

from scout import db
from scout.config import get_scout_settings
from scout.jobs import prod_read


class ProdReadTests(unittest.TestCase):
    def tearDown(self):
        get_scout_settings.cache_clear()

    def test_lock_down_makes_sessions_read_only_and_blanks_keys(self):
        env = {"OPENAI_API_KEY": "sk-x", "SCOUT_TG_BOT_TOKEN": "t", "TG_BOT_TOKEN": "t",
               "DATABASE_URL": "postgresql://u@internal:5432/d", "DATABASE_PUBLIC_URL": "postgresql://u@public:5432/d"}
        with mock.patch.dict(os.environ, env, clear=False):
            prod_read.lock_down()
            s = get_scout_settings()
            self.assertTrue(s.scout_db_read_only)
            self.assertEqual(s.openai_api_key, "")
            self.assertEqual(s.bot_token, "")
            self.assertEqual(db.connect_args()["server_settings"], {"default_transaction_read_only": "on"})
            self.assertEqual(s.database_url, "postgresql://u@public:5432/d")

    def test_normal_runs_are_unchanged(self):
        with mock.patch.dict(os.environ, {"SCOUT_DB_READ_ONLY": "0"}, clear=False):
            get_scout_settings.cache_clear()
            self.assertNotIn("server_settings", db.connect_args())

    def test_refuses_a_writable_connection(self):
        async def fake(*_a, **_k):
            return {"default_transaction_read_only": "off"}

        engine = mock.MagicMock()
        engine.dispose = mock.AsyncMock()
        with mock.patch.object(db, "fetch_one", fake), mock.patch.object(db, "get_engine", mock.MagicMock(return_value=engine)):
            import asyncio

            with self.assertRaises(SystemExit):
                asyncio.run(prod_read.check())


if __name__ == "__main__":
    unittest.main()
