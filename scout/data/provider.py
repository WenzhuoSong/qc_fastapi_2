"""Market data provider contract. Upper layers never name a vendor."""
from __future__ import annotations

from datetime import date
from typing import Protocol

import pandas as pd

BAR_COLUMNS = ["open", "high", "low", "close", "volume"]


class MarketDataProvider(Protocol):
    name: str

    async def daily_bars(
        self, tickers: list[str], start: date, end: date | None = None
    ) -> dict[str, pd.DataFrame]:
        """Split/dividend-adjusted daily bars keyed by ticker.

        Each frame is indexed by `datetime.date`, has BAR_COLUMNS, is sorted
        ascending and contains no NaN closes. Tickers without data are omitted.
        """
        ...


def get_provider() -> MarketDataProvider:
    from scout.data.yahoo import YahooProvider

    return YahooProvider()
