import unittest
from datetime import date, timedelta

from services.empirical_profiles import build_empirical_profiles


class EmpiricalProfilesTest(unittest.TestCase):
    def test_builds_profiles_from_feature_rows(self):
        start = date.today() - timedelta(days=69)
        rows = []
        spy_price = 100.0
        qqq_price = 80.0
        xlk_price = 60.0
        for i in range(70):
            dt = start + timedelta(days=i)
            base_ret = 0.001 + ((i % 5) * 0.0001)
            spy_ret = base_ret if i else None
            qqq_ret = base_ret * 2 if i else None
            xlk_ret = base_ret * 1.8 if i else None
            if spy_ret is not None:
                spy_price *= 1 + spy_ret
                qqq_price *= 1 + qqq_ret
                xlk_price *= 1 + xlk_ret
            rows.extend([
                {
                    "trading_date": dt,
                    "ticker": "SPY",
                    "source": "yfinance",
                    "adj_close_price": spy_price,
                    "return_1d": spy_ret,
                },
                {
                    "trading_date": dt,
                    "ticker": "QQQ",
                    "source": "yfinance",
                    "adj_close_price": qqq_price,
                    "return_1d": qqq_ret,
                },
                {
                    "trading_date": dt,
                    "ticker": "XLK",
                    "source": "yfinance",
                    "adj_close_price": xlk_price,
                    "return_1d": xlk_ret,
                },
            ])

        profiles = build_empirical_profiles(
            rows,
            tickers=["QQQ"],
            lookback_days=70,
            benchmark_ticker="SPY",
        )

        qqq = profiles["QQQ"]
        self.assertEqual(qqq["source"], "yfinance")
        self.assertEqual(qqq["samples"], 69)
        self.assertEqual(qqq["data_quality"], "fresh")
        self.assertGreater(qqq["avg_return"], 0.002)
        self.assertEqual(qqq["max_drawdown"], 0.0)
        self.assertIn("XLK", qqq["correlation_top"])
        self.assertIsNotNone(qqq["benchmark_correlation"])

    def test_missing_rows_return_missing_profile(self):
        profiles = build_empirical_profiles([], tickers=["QQQ"], lookback_days=70)

        self.assertEqual(profiles["QQQ"]["samples"], 0)
        self.assertEqual(profiles["QQQ"]["data_quality"], "missing")


if __name__ == "__main__":
    unittest.main()
