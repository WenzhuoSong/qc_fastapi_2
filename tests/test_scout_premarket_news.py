import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from scout import premarket


class NewsSinceTests(unittest.TestCase):
    def query(self, hours_back: float) -> str:
        now = 1_800_000_000
        with patch.object(premarket, "time_now", return_value=now), \
             patch.object(premarket.db, "fetch_all", AsyncMock(return_value=[])) as q:
            asyncio.run(premarket._news_since(["MU"], int(now - hours_back * 3600)))
        return q.await_args.args[0]

    def test_a_monday_brief_reads_the_archive_too(self):
        self.assertIn("ticker_news_archive", self.query(65))           # Friday close → Monday pre-market
        self.assertNotIn("ticker_news_archive", self.query(17))        # an ordinary night


if __name__ == "__main__":
    unittest.main()
