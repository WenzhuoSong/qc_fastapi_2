import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from scout import llm_usage as lu


class CostTests(unittest.TestCase):
    def test_longest_model_prefix_and_cached_rate(self):
        self.assertEqual(lu.price("gpt-4o-mini-2024-07-18"), lu.PRICES["gpt-4o-mini"])
        self.assertEqual(lu.price("gpt-4o-2024-08-06"), lu.PRICES["gpt-4o"])
        self.assertAlmostEqual(lu.cost("gpt-4o", 1_000_000, 400_000, 100_000), (600_000 * 2.5 + 400_000 * 1.25 + 100_000 * 10) / 1e6)
        self.assertIsNone(lu.cost("unknown-model", 10, 0, 10))


class RecordTests(unittest.TestCase):
    def test_sdk_usage_is_recorded_under_the_current_job(self):
        resp = SimpleNamespace(model="gpt-4o-2024-08-06", usage=SimpleNamespace(
            prompt_tokens=1000, completion_tokens=200, prompt_tokens_details=SimpleNamespace(cached_tokens=100)))
        with patch.object(lu.db, "execute", AsyncMock()) as ex:
            token = lu.JOB.set("eod")
            asyncio.run(lu.record_sdk(resp, "insight", "gpt-4o"))
            lu.JOB.reset(token)
        args = ex.await_args.args[1]
        self.assertEqual((args["j"], args["p"], args["i"], args["c"], args["o"]), ("eod", "insight", 1000, 100, 200))

    def test_a_logging_failure_never_breaks_the_call(self):
        with patch.object(lu.db, "execute", AsyncMock(side_effect=RuntimeError("db down"))):
            asyncio.run(lu.record_responses({"model": "gpt-6.1-sol", "usage": {"input_tokens": 5, "output_tokens": 1}}, "agent", "m"))


if __name__ == "__main__":
    unittest.main()
