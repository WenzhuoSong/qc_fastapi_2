import unittest
from datetime import date

import numpy as np
import pandas as pd

from scout import exit_vs_hold as ev
from tests.test_scout_plan_exit import _panel


def _arrays(close, open_=None):
    close = np.asarray(close, dtype=float)
    return {"close": close, "open": close if open_ is None else np.asarray(open_, dtype=float),
            "high": close * 1.01, "low": close * 0.99}


class PathTests(unittest.TestCase):
    def test_cash_after_a_sale_earns_the_bill_rate_until_the_window_ends(self):
        a = _arrays(np.full(100, 10.0))
        rf = np.full(100, 0.001)
        v = ev.path(a, 5, 10.0, (3, 11.0), rf)
        self.assertEqual(len(v), ev.WINDOW)
        np.testing.assert_allclose(v[:3], 1.0)
        self.assertAlmostEqual(v[3], 1.1)
        self.assertAlmostEqual(v[-1], 1.1 * 1.001 ** (ev.WINDOW - 4))

    def test_close_based_stop_sells_at_the_next_open(self):
        close = np.full(100, 10.0)
        close[12] = 7.5                          # closes below 80 % of the high close
        open_ = close.copy()
        open_[13] = 7.0
        a = _arrays(close, open_)
        top = np.maximum.accumulate(close[5:5 + ev.WINDOW])
        self.assertEqual(ev._trail(a, 5, lambda k: 0.8 * top[k - 5]), (8, 7.0))
        self.assertIsNone(ev._trail(_arrays(np.full(100, 10.0)), 5, lambda k: 8.0))

    def test_every_policy_covers_the_same_buys(self):
        p = _panel()
        a = ev.stock_arrays(p, "AAA")
        got = ev.trades(a, 301, np.zeros(len(p.dates)))
        self.assertEqual(set(got), set(ev.POLICIES))
        np.testing.assert_allclose(got["hold"][0], a["close"][301:301 + ev.WINDOW] / a["open"][301])
        self.assertTrue(all(len(v) == ev.WINDOW and 1 <= d <= ev.WINDOW for v, d in got.values()))


class PortfolioTests(unittest.TestCase):
    def test_sleeves_roll_cohorts_back_to_back(self):
        n = 400
        days = list(range(20, 20 + ev.EVERY * 9, ev.EVERY))
        means = {i: {"hold": np.linspace(1.0, 1.1, ev.WINDOW)} for i in days}
        nav = ev.portfolio(means, n, "hold")
        self.assertEqual(nav.index[0], days[ev.SLEEVES - 1] + 1)      # all sleeves invested
        self.assertTrue(np.isfinite(nav).all())
        self.assertGreater(nav.iloc[-1], nav.iloc[0])

    def test_decision_needs_every_condition(self):
        def pol(s1, s2, dd, w5, s=1.0):
            return {"portfolio": {"sharpe": s, "max_dd": dd}, "portfolio_first": {"sharpe": s1},
                    "portfolio_second": {"sharpe": s2}, "trades": {"worst5": w5}}
        pols = {"hold": pol(1.0, 1.0, -0.30, -0.20), "plan": pol(1.1, 0.9, -0.10, -0.10),
                "plan_wide": pol(1.0, 1.0, -0.26, -0.10), "atr10": pol(1.1, 1.2, -0.20, -0.15, 1.15),
                "chandelier": pol(1.2, 1.1, -0.24, -0.12, 1.2), "trail20": pol(1.3, 1.3, -0.20, -0.25, 1.4)}
        d = ev.decide(pols)
        self.assertEqual(sorted(d["candidates"]), ["atr10", "chandelier"])
        self.assertEqual(d["pick"], "chandelier")


class RateTests(unittest.TestCase):
    def test_rate_is_the_previous_days_yield(self):
        dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]
        rf = ev.rf_daily(pd.Index(dates), [(date(2026, 1, 5), 5.04), (date(2026, 1, 7), 2.52)])
        np.testing.assert_allclose(rf, [0.0, 5.04 / 100 / 252, 5.04 / 100 / 252])


if __name__ == "__main__":
    unittest.main()
