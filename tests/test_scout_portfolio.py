import unittest
from datetime import date

from scout import portfolio
from scout.context import event_flags
from scout.premarket import holding_label
from scout.telegram import outcome_text

SET = {"account_size": 100_000, "risk_pct": 0.01, "max_position_pct": 0.20}


class SizingTests(unittest.TestCase):
    def test_fixed_fractional(self):
        sz = portfolio.size(100.0, 95.0, SET)
        self.assertEqual(sz["shares"], 200)                     # $1,000 risk / $5 per share
        self.assertAlmostEqual(sz["pct_of_account"], 0.20)
        self.assertFalse(sz["capped"])

    def test_cap_by_position_size(self):
        sz = portfolio.size(100.0, 99.0, SET)                   # 1,000 shares would be 100% of the account
        self.assertEqual(sz["shares"], 200)
        self.assertTrue(sz["capped"])
        self.assertIn("已按单只仓位上限截断", portfolio.size_text(sz))

    def test_unconfigured_and_invalid(self):
        sz = portfolio.size(100.0, 95.0, {**SET, "account_size": None})
        self.assertEqual(sz["shares_per_1k"], 200)
        self.assertIn("每承担 $1,000 风险约 200 股", portfolio.size_text(sz))
        self.assertIsNone(portfolio.size(100.0, 101.0, SET))


class JournalTests(unittest.TestCase):
    OPEN = {"ticker": "VLO", "entry_price": 400.0, "shares": 10, "stop": 380.0, "last_close": 420.0, "closed_on": None}

    def test_open_trade_metrics(self):
        m = portfolio.metrics(self.OPEN)
        self.assertEqual(m["pnl"], 200.0)
        self.assertAlmostEqual(m["r"], 1.0)
        self.assertAlmostEqual(m["open_risk"], 400.0)
        self.assertAlmostEqual(m["to_stop"], 380 / 420 - 1)

    def test_closed_summary(self):
        closed = [{**self.OPEN, "closed_on": date(2026, 10, 30), "exit_price": 440.0},
                  {**self.OPEN, "closed_on": date(2026, 10, 30), "exit_price": 380.0}]
        s = portfolio.journal_summary(closed)
        self.assertEqual((s["n"], s["win_rate"], s["total_pnl"], s["avg_r"]), (2, 0.5, 200.0, 0.5))
        self.assertEqual(portfolio.journal_summary([{**self.OPEN, "last_close": None}]), {"n": 0})

    def test_no_price_means_no_pnl(self):
        self.assertNotIn("pnl", portfolio.metrics({**self.OPEN, "last_close": None}))

    def test_concentration(self):
        items = [{"ticker": t, "sector": "金融", "industry": "保险"} for t in ("TRV", "RGA")] + \
                [{"ticker": "FDS", "sector": "金融", "industry": "资本市场"}, {"ticker": "VLO", "sector": "能源"}]
        warn = "\n".join(portfolio.concentration(items))
        self.assertIn("板块「金融」同时有 3 个（FDS、RGA、TRV）", warn)
        self.assertIn("细分行业「保险」同时有 2 个", warn)
        self.assertNotIn("能源", warn)

    def test_trades_table_renders(self):
        from scout.web.views import trades_table

        html = trades_table([{**self.OPEN, "id": 1, "opened_on": date(2026, 10, 26), "last_date": date(2026, 10, 26)}])
        self.assertIn("+200", html)
        self.assertIn("+1.0R", html)
        self.assertIn("/trades/1/close", html)


class OutcomeAndEventTests(unittest.TestCase):
    def test_outcome_text(self):
        br = {"n": 1062, "target_rate": 0.18, "stop_rate": 0.34, "trail_rate": 0.0, "median_return": 0.004,
              "cost_bps": 10.0, "avg_days": 7.6}
        text = outcome_text(br)
        self.assertIn("达标 18% · 止损 34% · 到期离场 48%", text)
        self.assertIn("扣费收益中位数 +0.3%", text)
        self.assertIn("平均持有 8 天", text)
        self.assertIsNone(outcome_text({}))

    def test_event_flags(self):
        news = [{"headline": "Instacart reportedly discussed selling itself with DoorDash and Uber"},
                {"headline": "Company announces $500 million public offering"},
                {"headline": "Strong quarter lifts shares"}]
        kinds = [f["type"] for f in event_flags(news)]
        self.assertEqual(kinds, ["并购/出售", "增发/融资"])
        self.assertEqual(event_flags([{"headline": "Shares rally on guidance"}]), [])

    def test_holding_label(self):
        t = {"entry_price": 100.0, "stop": 95.0, "target": 110.0}
        self.assertIn("跌破止损（你的止损 95.00）", holding_label(94.0, t))
        self.assertIn("较你的成本 +2.0%，距你的止损 7.4%", holding_label(102.0, t))
        self.assertIn("已到目标 1 附近", holding_label(111.0, t))


if __name__ == "__main__":
    unittest.main()
