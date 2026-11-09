import unittest

import numpy as np
import pandas as pd

from scout import playbook_history as ph
from scout import playbook_history_v3 as v3


def hist(close: np.ndarray, high=None, low=None, opn=None) -> ph.Hist:
    """A synthetic history: business days around the window start, one column per stock (no ETFs)."""
    dates = pd.bdate_range("2008-05-01", periods=len(close))
    n, k = close.shape
    high = close * 1.01 if high is None else high
    low = close * 0.99 if low is None else low
    opn = close.copy() if opn is None else opn
    weeks = np.arange(0, n, 5)
    names = [f"S{j}" for j in range(k)]
    return ph.Hist(dates=dates, start=0, days={}, names=names, n_stocks=k, o=opn, h=high, l=low, c=close,
                   v=np.ones_like(close), high_max=np.fmax.accumulate(high, axis=0), week_ends=weeks,
                   members={int(w): names for w in weeks}, sp500={}, sectors={}, caps=np.ones_like(close) * 1e9,
                   fin={}, insiders=pd.DataFrame(), earnings={}, macro={}, rf=np.zeros(n))


def ver_all(H, key, buy_on: dict[int, list[int]], sell_on: dict[int, list[int]] | None = None) -> dict:
    sell_on = sell_on or {}
    every = np.arange(H.n_stocks, dtype=np.int16)
    return {d: {"stocks": every, "v": {key: {"buy": np.array(buy_on.get(d, []), dtype=np.int16),
                                              "sell": np.array(sell_on.get(d, []), dtype=np.int16)}}}
            for d in range(len(H.dates))}


class V3Tests(unittest.TestCase):
    def setUp(self):
        self.n = 60
        self.close = np.full((self.n, 2), 100.0)

    def test_window_starts_on_the_registered_day(self):
        H = hist(self.close)
        self.assertEqual(H.dates[v3.window(H)], v3.START)

    def test_daily_stop_sells_at_the_stop_or_a_lower_open(self):
        H = hist(self.close.copy())
        s = v3.window(H)
        H.c[s + 4:, 0] = 90.0
        H.l[s + 4:, 0] = 89.0
        H.o[s + 4:, 0] = 95.0
        r = v3.sim_slots(H, ver_all(H, "minervini", {s: [0]}), "minervini", s, v3.flat_cost(0.0), 8, "pct8")
        # 1/8 bought at 100, stopped at 92 (open 95 above the stop): 1 − 1/8 × 8 %
        self.assertAlmostEqual(r["nav"][-1], 1 - 0.125 * 0.08, places=9)
        self.assertEqual(r["trades"], 2)

    def test_stop_not_checked_on_the_fill_day(self):
        H = hist(self.close.copy())
        s = v3.window(H)
        H.l[s + 1, 0] = 80.0                                  # the fill day's low under the stop
        r = v3.sim_slots(H, ver_all(H, "minervini", {s: [0]}), "minervini", s, v3.flat_cost(0.0), 8, "pct8")
        self.assertEqual(r["trades"], 1)

    def test_tranches_form_at_once_then_one_acts_each_month(self):
        H = hist(np.full((120, 2), 100.0))
        s = v3.window(H)
        ver = {d: {"stocks": np.arange(2, dtype=np.int16),
                   "v": {"magic_formula": {"buy": np.array([0] if d < s + 30 else [1], dtype=np.int16), "sell": np.array([], dtype=np.int16)}}}
               for d in range(len(H.dates))}
        r = v3.sim_tranches(H, ver, "magic_formula", s, v3.flat_cost(0.0), 12, "rebuild", 30)
        ends = v3.month_ends(H, s)
        late = [i for i in ends if s + 30 <= i < len(H.dates) - 1]      # the last row's group has no next open
        # 12 buys at the start, then each later month end swaps one tranche (sell + buy)
        self.assertEqual(r["trades"], 12 + 2 * len(late))

    def test_turtle_stop_order_fills_at_the_level_or_a_gap_open(self):
        n = 150
        H = hist(np.full((n, 1), 100.0), high=np.full((n, 1), 101.0), low=np.full((n, 1), 99.0))
        H.dates = pd.bdate_range("2008-02-01", periods=n)
        s = v3.window(H)
        H.h[s - 10, 0] = 102.0                                 # the 55-day level: 102, above the usual highs
        H.h[s + 2, 0], H.o[s + 2, 0], H.c[s + 2:, 0] = 104.0, 100.5, 103.0
        pre = v3.precompute(H)
        ver = ver_all(H, "turtle_s2", {})
        r = v3.sim_turtle(H, ver, s, v3.flat_cost(0.0), 2, "stop", pre)
        n = pre["N"][s + 1, 0]
        units = 0.01 / n                                       # shares per 1 % of equity at nav 1
        self.assertAlmostEqual(r["nav"][s + 2], 1 + units * (103.0 - 102.0), places=9)
        H.o[s + 2, 0] = 103.5                                  # gap above the level: filled at the open
        r = v3.sim_turtle(H, ver, s, v3.flat_cost(0.0), 2, "stop", v3.precompute(H))
        self.assertAlmostEqual(r["nav"][s + 2], 1 + units * (103.0 - 103.5), places=9)

    def test_elder_buy_stop_risk_sizing_and_cap(self):
        H = hist(self.close.copy())
        s = v3.window(H)
        H.h[s + 1, 0], H.o[s + 1, 0], H.c[s + 1:, 0], H.l[s + 1:, 0] = 102.0, 100.0, 101.0, 100.0
        r = v3.sim_elder(H, ver_all(H, "elder", {s: [0]}), s, v3.flat_cost(0.0))
        # buy-stop at the signal day's high 101; stop = 99 (the two lows); 2 % risk → 1.01 of equity, capped at 1/5
        self.assertEqual(r["trades"], 1)
        self.assertAlmostEqual(r["nav"][s + 1], 1.0, places=9)               # bought at 101, closed at 101
        H.c[s + 2:, 0] = 111.0
        r = v3.sim_elder(H, ver_all(H, "elder", {s: [0]}), s, v3.flat_cost(0.0))
        self.assertAlmostEqual(r["nav"][s + 2], 1 + 0.2 / 101 * 10, places=9)

    def test_small_cost_refuses_untradeable_buys_and_prices_sells(self):
        H = hist(self.close)
        H.large = {int(w): {"S0"} for w in H.week_ends}
        month = H.dates.to_period("M")
        f = v3.small_cost(H, {("S1", month[10]): {"main": 0.004}})
        self.assertEqual(f(10, 0), v3.COST)
        self.assertEqual(f(10, 1), 0.004)
        self.assertIsNone(f(40, 1))
        self.assertEqual(f(40, 1, sell=True), 0.004)          # its last estimate
        self.assertEqual(v3.small_cost(H, {("S1", month[10]): {"main": 0.004}}, high=True)(10, 1), 0.016)

    def test_every_playbook_has_a_protocol_or_is_kept_as_v2(self):
        keys = {p.key for p in ph.books()}
        mapped = (set(v3.ANNUAL_REBUILD) | set(v3.ANNUAL_REVIEW) | set(v3.QUARTERLY_REVIEW) | set(v3.FUNDAMENTAL)
                  | set(v3.PRICE_RANKERS) | set(v3.CONCENTRATED) | set(v3.ALLOC_TRANCHES) | set(v3.AS_V2)
                  | {"weinstein", "turtle_s1", "turtle_s2", "clenow", "elder", "connors_rsi2"})
        rest = {p.key for p in ph.books() if p.school in ("大盘择时", "资产配置")} - mapped
        self.assertEqual(keys - mapped - rest, set())


if __name__ == "__main__":
    unittest.main()
