import unittest
from unittest.mock import AsyncMock, patch

from scout import pipeline


def off(n):
    return [{"ticker": f"T{i}"} for i in range(n)]


class SettleClosesTests(unittest.IsolatedAsyncioTestCase):
    def test_a_fifth_of_the_sample_and_at_least_three(self):
        self.assertTrue(pipeline.closes_unsettled(30, off(6)))
        self.assertFalse(pipeline.closes_unsettled(30, off(5)))
        self.assertFalse(pipeline.closes_unsettled(10, off(2)))           # 20 % but only two stocks

    async def _run(self, offsets):
        with patch.object(pipeline, "price_sample", AsyncMock(return_value=["SPY"])), \
             patch("scout.research_data.price_offsets", AsyncMock(side_effect=offsets)), \
             patch.object(pipeline, "prepare", AsyncMock()) as prep:
            return await pipeline.settle_closes(wait_s=0), prep

    async def test_refetches_until_the_closes_agree(self):
        r, prep = await self._run([(30, off(20)), (30, off(1))])
        self.assertTrue(r["settled"])
        self.assertEqual([t["off"] for t in r["tries"]], [20, 1])
        self.assertEqual(prep.await_count, 1)
        self.assertIsNone(pipeline.unsettled_text(r))

    async def test_goes_on_after_the_last_retry_and_says_so(self):
        r, prep = await self._run([(30, off(20))] * (pipeline.PRICE_RETRIES + 1))
        self.assertFalse(r["settled"])
        self.assertEqual(prep.await_count, pipeline.PRICE_RETRIES)
        self.assertIn("抽查 30 只，20 只", pipeline.unsettled_text(r))


if __name__ == "__main__":
    unittest.main()
