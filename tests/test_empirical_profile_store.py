import sys
import types
import unittest
from datetime import date, timedelta

from services.empirical_profile_store import (
    build_empirical_profiles_from_feature_store,
    collect_empirical_profile_tickers,
)


class EmpiricalProfileStoreTest(unittest.IsolatedAsyncioTestCase):
    def test_collects_profile_tickers_from_current_universe(self):
        tickers = collect_empirical_profile_tickers(
            brief={
                "current_weights": {"QQQ": 0.1, "CASH": 0.2},
                "holdings": [{"ticker": "SOXL"}],
            },
            quant_baseline={
                "base_weights": {"SPY": 0.5},
                "selected_tickers": ["XLK"],
            },
            playground_bundle={
                "consensus_weights": {"TLT": 0.1, "CASH": 0.9},
                "strategies": [{"selected_tickers": ["XSD", "QQQ"]}],
            },
        )

        self.assertEqual(tickers, ["QQQ", "SOXL", "SPY", "XLK", "TLT", "XSD"])

    async def test_builds_profiles_from_feature_store_rows(self):
        async def fake_get_market_daily_feature_rows(db, tickers, start_date=None, source=None, limit=None):
            rows = []
            start = date.today() - timedelta(days=69)
            spy = 100.0
            qqq = 80.0
            for i in range(70):
                dt = start + timedelta(days=i)
                spy_ret = (0.001 + (i % 3) * 0.0001) if i else None
                qqq_ret = (0.002 + (i % 3) * 0.0002) if i else None
                if spy_ret is not None:
                    spy *= 1 + spy_ret
                    qqq *= 1 + qqq_ret
                rows.extend([
                    {
                        "trading_date": dt,
                        "ticker": "SPY",
                        "source": "yfinance",
                        "adj_close_price": spy,
                        "return_1d": spy_ret,
                    },
                    {
                        "trading_date": dt,
                        "ticker": "QQQ",
                        "source": "yfinance",
                        "adj_close_price": qqq,
                        "return_1d": qqq_ret,
                    },
                ])
            return rows

        stub = types.ModuleType("services.market_feature_store")
        stub.get_market_daily_feature_rows = fake_get_market_daily_feature_rows
        previous = sys.modules.get("services.market_feature_store")
        sys.modules["services.market_feature_store"] = stub
        try:
            profiles = await build_empirical_profiles_from_feature_store(
                db=object(),
                tickers=["QQQ"],
                lookback_days=70,
            )
        finally:
            if previous is not None:
                sys.modules["services.market_feature_store"] = previous
            else:
                sys.modules.pop("services.market_feature_store", None)

        self.assertIn("QQQ", profiles)
        self.assertEqual(profiles["QQQ"]["samples"], 69)
        self.assertEqual(profiles["QQQ"]["data_quality"], "fresh")
        self.assertIsNotNone(profiles["QQQ"]["benchmark_correlation"])


if __name__ == "__main__":
    unittest.main()
