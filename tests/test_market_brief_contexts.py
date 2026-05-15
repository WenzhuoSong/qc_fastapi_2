import unittest
import sys
import types

from services.market_brief_contexts import build_memory_context, build_scenario_context


class MarketBriefContextsTests(unittest.IsolatedAsyncioTestCase):
    async def test_scenario_context_skips_low_equity_exposure(self):
        result = await build_scenario_context({"CASH": 0.95, "SPY": 0.05})

        self.assertIsNone(result)

    async def test_memory_context_degrades_on_failure(self):
        async def failing_context():
            raise RuntimeError("boom")

        stub = types.ModuleType("services.context_assembler")
        stub.assemble_memory_context = failing_context
        previous = sys.modules.get("services.context_assembler")
        sys.modules["services.context_assembler"] = stub
        try:
            result = await build_memory_context()
        finally:
            if previous is not None:
                sys.modules["services.context_assembler"] = previous
            else:
                sys.modules.pop("services.context_assembler", None)

        self.assertFalse(result["has_memory"])
        self.assertIn("memory context unavailable", result["data_gaps"][0])


if __name__ == "__main__":
    unittest.main()
