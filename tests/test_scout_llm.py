import asyncio
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("OPENAI_API_KEY", "test-only")

from scout import agent, thesis
from scout.config import get_scout_settings
from tests import test_scout_core

EV = test_scout_core.ThesisGuardTests.EV


def _parsed(text_thesis: str, confidence: str = "medium") -> thesis.Thesis:
    return thesis.Thesis(
        headline="NVDA 趋势突破", thesis=text_thesis, catalysts=[], counter_evidence=["大盘震荡"],
        key_risk="跌破 176.8", confidence=confidence, confidence_reason="历史 143 次",
    )


def _parse_response(parsed):
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(parsed=parsed))])


class FakeClient:
    def __init__(self, parse_results=None, create_results=None):
        self.beta = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(
            parse=AsyncMock(side_effect=[_parse_response(p) for p in (parse_results or [])]))))
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=AsyncMock(side_effect=create_results or [])))


class ThesisLLMTests(unittest.TestCase):
    def setUp(self):
        get_scout_settings.cache_clear()

    def run_with(self, client):
        with patch("openai.AsyncOpenAI", return_value=client):
            return asyncio.run(thesis.write_thesis(EV))

    def test_clean_llm_output_is_used(self):
        t, source = self.run_with(FakeClient([_parsed("20 日上涨 5.2%，相对强度 93%。")]))
        self.assertEqual(source, "llm")

    def test_invented_number_retries_then_succeeds(self):
        client = FakeClient([_parsed("目标看到 250。"), _parsed("相对强度 93%。")])
        t, source = self.run_with(client)
        self.assertEqual(source, "llm")
        retry_messages = client.beta.chat.completions.parse.call_args_list[1].kwargs["messages"]
        self.assertIn("250", retry_messages[-1]["content"])

    def test_persistent_invention_falls_back_to_template(self):
        t, source = self.run_with(FakeClient([_parsed("目标 250。"), _parsed("目标 260。")]))
        self.assertEqual(source, "template")

    def test_high_confidence_downgraded_with_small_sample(self):
        ev = {**EV, "base_rate": {**EV["base_rate"], "n": 12}}
        allowed_text = "历史 12 次。"
        client = FakeClient([thesis.Thesis(
            headline="x", thesis=allowed_text, catalysts=[], counter_evidence=["y"], key_risk="z",
            confidence="high", confidence_reason="历史 12 次",
        )])
        with patch("openai.AsyncOpenAI", return_value=client):
            t, source = asyncio.run(thesis.write_thesis(ev))
        self.assertEqual((source, t.confidence), ("llm", "medium"))


class AgentLoopTests(unittest.TestCase):
    def setUp(self):
        get_scout_settings.cache_clear()

    def test_tool_call_then_answer_and_history_saved(self):
        first = {"id": "r1", "output": [{"type": "function_call", "name": "get_snapshot", "call_id": "c1",
                                         "arguments": '{"ticker": "NVDA"}'}]}
        final = {"id": "r2", "output": [{"type": "message", "content": [{"type": "output_text", "text": "NVDA 收于 210.96 <b>"}]}]}
        sent = []

        async def fake_create(body, timeout=None):
            sent.append(body)
            return first if len(sent) == 1 else final

        trace = []
        with patch("scout.responses.create", fake_create), \
             patch.object(agent, "get_scout_settings", return_value=SimpleNamespace(
                 openai_api_key="k", scout_agent_model="m", scout_agent_reasoning="low", scout_agent_timeout_s=5)), \
             patch.object(agent.db, "fetch_all", AsyncMock(return_value=[])), \
             patch.object(agent.db, "execute_many", AsyncMock()) as saved, \
             patch.object(agent, "call_tool", AsyncMock(return_value='{"close": 210.96}')) as tool:
            out = asyncio.run(agent.answer("NVDA 怎么样", channel="telegram", trace=trace))
        tool.assert_awaited_once_with("get_snapshot", {"ticker": "NVDA"})
        self.assertEqual(out, "NVDA 收于 210.96 &lt;b&gt;")   # escaped for Telegram HTML
        self.assertEqual(len(saved.await_args.args[1]), 2)
        self.assertEqual(sent[1]["previous_response_id"], "r1")              # the tool output continues the same response
        self.assertEqual(sent[1]["input"], [{"type": "function_call_output", "call_id": "c1", "output": '{"close": 210.96}'}])
        self.assertIn("instructions", sent[1])                                # not carried over by previous_response_id
        self.assertEqual(trace[0]["tool"], "get_snapshot")

    def test_every_tool_has_a_handler(self):
        self.assertEqual(sorted(t["name"] for t in agent.TOOLS), sorted(agent.HANDLERS))

if __name__ == "__main__":
    unittest.main()
