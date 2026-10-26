import unittest
from datetime import date

from scout.analysis import scout_view, trend_state

BASE = {
    "close": 110.0, "sma50": 100.0, "sma200": 90.0, "rs_pct": 0.92, "sector_rs_pct": 0.8,
    "ret_1d": 0.02, "market_ret_1d": 0.005, "idio_ret_1d": 0.015, "idio_ret_20d": 0.08, "beta_60": 1.2,
    "zscore_20": 0.5, "dist_52w_high": -0.03, "vol_20": 0.35, "max_dd_60": -0.08, "atr_pct": 0.025,
    "vol_ratio": 1.1, "rsi2": 55.0,
}


class ScoutViewTests(unittest.TestCase):
    def test_trend_states(self):
        self.assertEqual(trend_state(BASE)[1], "up")
        self.assertEqual(trend_state({**BASE, "close": 80, "sma50": 85})[1], "down")
        self.assertEqual(trend_state({})[0], "数据不足")

    def test_strong_stock_headline_and_lines(self):
        headline, lines = scout_view(BASE, is_stock=True, next_earnings=date(2026, 10, 30),
                                     latest_opp=None, today=date(2026, 10, 26))
        self.assertIn("强势股", headline)
        labels = [l.label for l in lines]
        self.assertEqual(labels[:2], ["趋势", "大盘 vs 个股"])
        self.assertIn("相对强度", labels)
        event = next(l for l in lines if l.label == "事件")
        self.assertEqual(event.tone, "warn")          # earnings within a week

    def test_near_trigger_and_etf_has_no_rs(self):
        _, lines = scout_view({**BASE, "rsi2": 18.0}, is_stock=True, next_earnings=None,
                              latest_opp=None, today=date(2026, 10, 26))
        self.assertIn("强势回调", next(l for l in lines if l.label == "规则").text)
        _, etf_lines = scout_view(BASE, is_stock=False, next_earnings=None, latest_opp=None, today=date(2026, 10, 26))
        self.assertNotIn("相对强度", [l.label for l in etf_lines])

    def test_active_opportunity_is_reported(self):
        opp = {"status": "new", "primary_detector": "volume_shock", "asof": date(2026, 10, 23), "score": 0.7,
               "evidence": {"setup": {"label": "放量异动"}}}
        _, lines = scout_view(BASE, is_stock=True, next_earnings=None, latest_opp=opp, today=date(2026, 10, 26))
        self.assertIn("放量异动", next(l for l in lines if l.label == "规则").text)

    def test_empty_snapshot(self):
        self.assertEqual(scout_view({}, is_stock=True, next_earnings=None, latest_opp=None, today=date.today()),
                         ("还没有特征数据", []))


if __name__ == "__main__":
    unittest.main()
