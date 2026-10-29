import unittest
from datetime import date, timedelta

import numpy as np

from scout import exit_redeploy as er
from scout import exit_vs_hold as ev


class MarketFilterTests(unittest.TestCase):
    def test_risk_off_after_a_month_end_close_below_the_ten_month_mean(self):
        dates, close = [], []
        d, price = date(2020, 1, 1), 100.0
        for m in range(14):
            for k in range(20):
                dates.append(d + timedelta(days=31 * m + k))
                close.append(price + m if m < 12 else 80.0)    # rising, then a drop in month 13
        off = er.market_off(np.array(close), dates)
        first_end = 10 * 20 - 1                                 # the tenth month end
        self.assertFalse(off[:first_end].any())
        self.assertFalse(off[12 * 20 - 1])                      # month 12 ended above its mean
        self.assertTrue(off[13 * 20 - 1])                       # month 13 ended below
        self.assertFalse(off[12 * 20 + 5])                      # within month 13 the old state holds


class VariantTests(unittest.TestCase):
    def test_sale_proceeds_follow_the_destination_after_one_cost(self):
        w = ev.WINDOW
        s = 10
        v = np.linspace(1.0, 0.9, w)
        base = {"hold": (np.linspace(1.0, 1.2, w), w), "plan": (v, 5), "atr10": (v, w), "trail20": (v, w)}
        growth = {k: np.full(200, 0.01) for k in [(d, f) for d in er.DESTS for f in er.FILTERS]}
        rf = np.zeros(200)
        got = er.variants(base, s, False, rf, growth)
        path, days, inv = got["plan|pool|none"]
        np.testing.assert_allclose(path[:5], v[:5])
        self.assertAlmostEqual(path[-1], v[4] * (1 - ev.COST) * 1.01 ** (w - 5))
        np.testing.assert_allclose(got["atr10|pool|none"][0], v)            # never sold: the stock itself
        self.assertTrue(inv)

    def test_no_buy_while_the_market_is_risk_off(self):
        w = ev.WINDOW
        base = {k: (np.full(w, 1.1), w) for k in ("hold", "plan", "atr10", "trail20")}
        growth = {k: np.zeros(200) for k in [(d, f) for d in er.DESTS for f in er.FILTERS]}
        rf = np.full(200, 0.0001)
        got = er.variants(base, 10, True, rf, growth)
        self.assertFalse(got["hold_market"][2])
        self.assertTrue(got["plan|pool|none"][2])                           # unfiltered variants still buy
        self.assertAlmostEqual(got["plan|picks|market"][0][-1] * (1 - ev.COST), 1.0001 ** w)


class DecisionTests(unittest.TestCase):
    def test_a_variant_behind_holding_in_a_half_is_not_picked(self):
        def pol(s1, s2, dd, w5, t1, t2, s=1.0):
            return {"portfolio": {"sharpe": s, "max_dd": dd}, "portfolio_first": {"sharpe": s1},
                    "portfolio_second": {"sharpe": s2}, "trades": {"worst5": w5},
                    "vs_hold_first": {"t": t1}, "vs_hold_second": {"t": t2}}
        pols = {"hold": pol(1.0, 1.0, -0.4, -0.3, None, None), "hold_market": pol(1.0, 1.0, -0.4, -0.3, 0, 0)}
        for k in er.VARIANTS:
            pols[k] = pol(0.5, 0.5, -0.4, -0.3, 0, 0)
        pols["atr10|pool|market"] = pol(1.2, 1.1, -0.2, -0.2, 0.5, -2.5, 1.3)
        pols["trail20|picks|market"] = pol(1.1, 1.05, -0.25, -0.2, 0.1, -1.0, 1.2)
        d = er.decide(pols)
        self.assertEqual(d["candidates"], ["trail20|picks|market"])
        self.assertEqual(d["pick"], "trail20|picks|market")


class SeriesTests(unittest.TestCase):
    def test_pool_return_uses_the_previous_days_pool(self):
        from scout import rankers
        from tests.test_scout_plan_exit import _panel

        p = _panel(n=420)
        rf = np.full(len(p.dates), 0.0001)
        off, g = er.series(p, {}, rf)
        a, stocks = rankers.arrays(p)
        t = 300
        uni = rankers._universe(a, t - 1)
        expect = np.nanmean(a["close"][t, uni] / a["close"][t - 1, uni] - 1)
        self.assertAlmostEqual(g[("pool", "none")][t], expect)
        self.assertEqual(g[("picks", "none")][t], rf[t])                    # no angle data: no picks, T-bills
        self.assertEqual(g[("pool", "market_sector")][t], rf[t])            # no sector ETFs: nothing strong


if __name__ == "__main__":
    unittest.main()
