import unittest
from datetime import date

import numpy as np
import pandas as pd

from scout import playbook_history_v3 as v3
from scout import playbook_history_v4 as v4
from scout.playbooks import value as va
from scout.playbooks.base import BUY
from scout.playbooks.data import fundamentals
from tests.test_scout_playbook_history_v3 import hist, ver_all


def breakout(n=120, k=1):
    """k stocks flat at 100 (high 101, low 99), then breaking above the 20-day high on the window's second session."""
    H = hist(np.full((n, k), 100.0), high=np.full((n, k), 101.0), low=np.full((n, k), 99.0))
    H.dates = pd.bdate_range("2008-02-01", periods=n)
    s = v3.window(H)
    H.h[s - 10, :] = 102.0                                          # the entry level: 102, above the usual highs
    H.c[s + 1, :] = 102.0                                           # the evening before at the level: a unit is 1/12 exactly
    H.h[s + 2, :], H.c[s + 2:, :] = 104.0, 103.0
    H.h[s + 3:, :], H.l[s + 3:, :] = 102.5, 102.5                    # no pyramiding, no exit afterwards
    return H, s


class TurtleTests(unittest.TestCase):
    def test_a_median_unit_is_a_twelfth_of_the_account(self):
        H, s = breakout()
        pre = v3.precompute(H)
        r = v4.sim_turtle(H, ver_all(H, "turtle_s1", {}), s, v3.flat_cost(0.0), 2, "stop", pre)
        n = pre["N"][s + 1, 0]
        units = (n / H.c[s + 1, 0]) / 12 / n                        # shares per unit at nav 1
        self.assertAlmostEqual(r["nav"][s + 2], 1 + units * (103.0 - 102.0), places=9)

    def test_twelve_units_in_all_and_six_a_sector(self):
        H, s = breakout(k=20)
        pre = v3.precompute(H)
        r = v4.sim_turtle(H, ver_all(H, "turtle_s2", {}), s, v3.flat_cost(0.0), 2, "stop", pre)
        self.assertEqual(r["trades"], 12)
        H.sectors = {t: "Energy" for t in H.names}
        r = v4.sim_turtle(H, ver_all(H, "turtle_s2", {}), s, v3.flat_cost(0.0), 2, "stop", pre)
        self.assertEqual(r["trades"], 6)
        r = v4.sim_turtle(H, ver_all(H, "turtle_s2", {}), s, v3.flat_cost(0.0), 2, "stop", pre, tape=True)
        self.assertEqual(r["trades"], 20)                           # the tape: every breakout


class ElderTests(unittest.TestCase):
    def test_risk_is_scaled_to_a_twentieth_and_capped_at_a_tenth(self):
        H = hist(np.full((60, 1), 100.0))
        s = v3.window(H)
        H.h[s + 1, 0], H.o[s + 1, 0], H.c[s + 2:, 0], H.l[s + 1:, 0] = 102.0, 100.0, 111.0, 100.0
        r = v4.sim_elder(H, ver_all(H, "elder", {s: [0]}), s, v3.flat_cost(0.0))
        # median two-day stop distance 1 % → risk 0.05 %; stop 99 from a fill at 101 → 0.0005 / 2 × 101 = 2.5 % of equity
        self.assertAlmostEqual(r["nav"][s + 2], 1 + 0.0005 / 2 * 10, places=9)
        H.l[s - 1:s + 1, 0], H.l[s + 1:, 0] = 100.95, 101.0          # a stop 0.05 under the fill: capped at 1/10
        r = v4.sim_elder(H, ver_all(H, "elder", {s: [0]}), s, v3.flat_cost(0.0))
        self.assertAlmostEqual(r["nav"][s + 2], 1 + 0.10 / 101 * 10, places=9)


class EventTests(unittest.TestCase):
    def test_insider_signal_is_the_first_day_and_not_again_within_six_months(self):
        H = hist(np.full((400, 1), 100.0))
        s = v3.window(H)
        on = {d: [0] for d in range(s + 2, s + 40)} | {d: [0] for d in range(s + 60, s + 70)} | {d: [0] for d in range(s + 200, s + 210)}
        e = v4.insider_entries(H, ver_all(H, "insider", on), s)
        self.assertEqual(sorted(e), [s + 2, s + 200])

    def test_pead_takes_the_top_tenth_of_the_last_63_sessions(self):
        ev = [(d, d % 7, float(d % 10)) for d in range(100, 140)]       # reactions 0..9, 40 events
        e = v4.pead_entries(ev)
        self.assertTrue(all(d % 10 >= 8 for d in e))                    # the top tenth of what is known
        self.assertNotIn(109, e)                                       # fewer than 20 known by then
        self.assertIn(119, e)

    def test_signals_buy_a_fiftieth_skip_when_cash_runs_out_and_sell_after_the_hold(self):
        H = hist(np.full((200, 60), 100.0))
        s = v3.window(H)
        entries = {s: list(range(60))}
        r = v4.sim_signals(H, ver_all(H, "insider", {}), "insider", s, v3.flat_cost(0.0), entries, hold=10, sells=False)
        self.assertEqual((r["signals"], r["taken"]), (60, 50))
        self.assertEqual(r["trades"], 100)                              # 50 bought, 50 sold after 10 sessions
        self.assertEqual(len(r["log"]), 50)
        self.assertTrue(all(z - a == 10 for _, a, z, _, _ in r["log"]))


class CanSlimTests(unittest.TestCase):
    def _hist(self):
        n = 300
        H = hist(np.full((n, 1), 100.0))
        H.dates = pd.bdate_range("2008-02-01", periods=n)              # 50 sessions of volume before the window
        s = v3.window(H)
        d = s + 2
        H.h[d - 30:d, 0], H.l[d - 30:d, 0], H.c[d - 30:d, 0] = 100.0, 92.0, 96.0
        H.c[d, 0], H.h[d, 0], H.v[d, 0] = 102.0, 102.5, 2.0            # breakout above the base on twice the volume
        H.o[d + 1:, 0], H.c[d + 1:, 0], H.h[d + 1:, 0], H.l[d + 1:, 0] = 102.0, 102.0, 103.0, 101.0
        return H, s, d

    def test_flat_base_breakout(self):
        H, s, d = self._hist()
        self.assertEqual(v4.flat_base(H, d, 0), 100.0)
        H.v[d, 0] = 1.2
        self.assertIsNone(v4.flat_base(H, d, 0))                       # not enough volume

    def test_profit_target_from_the_pivot_and_the_eight_week_rule(self):
        H, s, d = self._hist()
        buy = {i: [0] for i in range(s, len(H.dates))}
        H.h[d + 30, 0] = 121.0                                          # +21 % from the pivot after 3 weeks: sold at 120
        r = v4.sim_canslim(H, ver_all(H, "can_slim", buy), s, v3.flat_cost(0.0))
        self.assertEqual(r["log"][0][2], d + 30)
        self.assertAlmostEqual(r["log"][0][4] / r["log"][0][3], 120 / 102, places=9)
        H.h[d + 5, 0] = 121.0                                           # within 15 sessions: held, never targeted again
        r = v4.sim_canslim(H, ver_all(H, "can_slim", buy), s, v3.flat_cost(0.0))
        self.assertEqual(r["trades"], 1)

    def test_institutions_must_rise(self):
        H, s, d = self._hist()
        buy = {i: [0] for i in range(s, len(H.dates))}
        q = lambda a, b: {"S0": [(pd.Timestamp("2007-12-31"), a), (pd.Timestamp("2008-03-31"), b)]}  # noqa: E731
        self.assertEqual(v4.sim_canslim(H, ver_all(H, "can_slim", buy), s, v3.flat_cost(0.0), inst=q(10, 9))["trades"], 0)
        self.assertEqual(v4.sim_canslim(H, ver_all(H, "can_slim", buy), s, v3.flat_cost(0.0), inst=q(9, 10))["trades"], 1)


class ThrustTests(unittest.TestCase):
    def test_holds_spy_252_sessions_from_the_signal(self):
        n = 400
        H = hist(np.full((n, 1), 100.0))
        H.names, H.n_stocks = ["S0", "SPY"], 1
        for a in ("o", "h", "l", "c", "v"):
            setattr(H, a, np.hstack([getattr(H, a), getattr(H, a)[:, :1]]))
        s = v3.window(H)
        ver = {d: {"stocks": np.array([0], dtype=np.int16),
                   "v": {"breadth_thrust": {"etf": "SPY", "action": "买入" if s + 3 <= d <= s + 10 else "持有"}}} for d in range(n)}
        r = v4.sim_thrust(H, ver, s, v3.flat_cost(0.0))
        self.assertEqual(r["trades"], 2)
        self.assertEqual(len(r["signals"]), 1)


class TradeStatsTests(unittest.TestCase):
    def test_returns_and_comparisons(self):
        H = hist(np.full((100, 1), 100.0))
        H.names, H.n_stocks = ["S0", "SPY"], 1
        H.c = np.hstack([H.c, H.c[:, :1]])
        s = v3.window(H)
        st = v4.trade_stats(H, [(0, s + 1, s + 5, 1.0, 1.1)], np.ones(len(H.dates)), s)
        self.assertEqual(st["n"], 1)
        self.assertAlmostEqual(st["avg_ret"], 0.1)
        self.assertAlmostEqual(st["vs_spy"]["mean"], 0.1)
        self.assertAlmostEqual(st["vs_hold"]["mean"], 0.1)            # a flat stock: holding it earns nothing


class FullFieldTests(unittest.TestCase):
    def _fin(self):
        q = pd.date_range("2006-03-31", "2018-12-31", freq="QE").date
        rows = lambda v: [(e, e, "quarter", v, e) for e in q]  # noqa: E731
        fin = {("A", m): rows(v) for m, v in {"net_income": 10.0, "eps_diluted": 1.0, "dividends": 1.0, "ebitda": 20.0,
                                             "revenue": 100.0, "operating_income": 12.0, "total_assets": 400.0, "equity": 200.0,
                                             "current_assets": 150.0, "current_liabilities": 50.0, "ppe_net": 100.0,
                                             "cash": 20.0}.items()}
        return fin

    def test_full_fields_only_when_asked(self):
        asof = date(2019, 1, 31)
        self.assertNotIn("current_assets", fundamentals([], pd.Series(dtype=float), {}, asof, fin=self._fin(), tickers=["A"]))
        f = fundamentals([], pd.Series(dtype=float), {}, asof, fin=self._fin(), tickers=["A"], full=True)
        self.assertEqual(f.at["A", "current_assets"], 150.0)
        self.assertEqual(f.at["A", "ebitda"], 80.0)
        self.assertEqual(len(f.at["A", "eps_years"]), 11)
        self.assertEqual(f.at["A", "eps_years"][10], 4.0)

    def test_greenblatt_capital_and_graham_with_full_fields(self):
        from tests.test_scout_playbooks import _ctx

        f = fundamentals([], pd.Series(dtype=float), {}, date(2019, 1, 31), fin=self._fin(), tickers=["A"], full=True)
        f["mcap"], f["ev"] = 3e9, 3e9
        f.loc["A", ["total_assets", "equity", "net_income"]] = [4e9, 3e9, 4e8]
        idx = pd.bdate_range("2018-06-01", periods=130)
        frame = pd.DataFrame({"A": np.full(130, 0.2)}, index=idx)
        ctx = _ctx(pd.DataFrame({"SPY": np.full(130, 100.0), "A": np.full(130, 50.0)}, index=idx), fund=f,
                   f={"vol_63": frame, "beta": frame})
        r = va.ratios(ctx)
        # EBIT 48 / ((150 − 20 − (50 − 0)) + 100) = 48 / 180
        self.assertAlmostEqual(r.at["A", "roc"], 48 / 180)
        self.assertAlmostEqual(r.at["A", "cr"], 3.0)
        v = va.graham(ctx)                                              # flat EPS: growth 0 < 1/3, not a buy
        self.assertNotEqual(getattr(v.get("A"), "action", None), BUY)


if __name__ == "__main__":
    unittest.main()
