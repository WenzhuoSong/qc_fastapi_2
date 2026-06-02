import asyncio
import importlib
import sys
import types
import unittest
from unittest.mock import patch


def _load_notify_tools(posts: list[dict]):
    class FakeResponse:
        status_code = 200
        text = "ok"

    class FakeAsyncClient:
        def __init__(self, timeout=10):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json):
            posts.append({"url": url, "json": dict(json)})
            return FakeResponse()

    fake_httpx = types.SimpleNamespace(AsyncClient=FakeAsyncClient, Response=FakeResponse)
    fake_settings = types.SimpleNamespace(tg_bot_token="token", tg_chat_id="chat")
    fake_config = types.SimpleNamespace(get_settings=lambda: fake_settings)

    sys.modules.pop("tools.notify_tools", None)
    with patch.dict(sys.modules, {"httpx": fake_httpx, "config": fake_config}):
        return importlib.import_module("tools.notify_tools")


class NotifyToolsTest(unittest.TestCase):
    def test_split_telegram_text_keeps_chunks_under_safe_limit(self):
        posts: list[dict] = []
        module = _load_notify_tools(posts)

        chunks = module._split_telegram_text("line\n" * 2000)

        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(chunk) <= module.TELEGRAM_SAFE_MESSAGE_CHARS for chunk in chunks))

    def test_tool_send_telegram_splits_long_messages(self):
        posts: list[dict] = []
        module = _load_notify_tools(posts)

        result = asyncio.run(module.tool_send_telegram({"text": "line\n" * 2000, "parse_mode": ""}))

        self.assertTrue(result["sent"])
        self.assertGreater(result["parts"], 1)
        self.assertEqual(len(posts), result["parts"])
        self.assertTrue(all(len(row["json"]["text"]) <= module.TELEGRAM_MAX_MESSAGE_CHARS for row in posts))


if __name__ == "__main__":
    unittest.main()
