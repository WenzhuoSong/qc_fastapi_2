import unittest

import numpy as np
import pandas as pd

from scout import strong_hold as sh


def _d(c: np.ndarray, last=None) -> dict:
    n, k = c.shape
    return {"o": c.copy(), "c": c.copy(), "last_day": np.array(last if last is not None else [n - 1] * k),
            "member": np.ones((n, k), dtype=bool), "dates": pd.bdate_range("2001-01-02", periods=n), "names": [f"S{j}" for j in range(k)]}


class TradeTests(unittest.TestCase):
    def test_held_to_the_windows_last_close_when_the_stop_never_triggers(self):
        c = np.linspace(100, 150, 30)[:, None]
        r = sh.trade(_d(c), 0, 0, 20)
        self.assertAlmostEqual(r["ret"], c[20, 0] / c[1, 0] - 1 - sh.COST)
        self.assertEqual((r["days"], r["stopped"]), (20, False))

    def test_a_close_below_80_percent_of_the_high_sells_at_the_next_open(self):
        c = np.full((30, 1), 100.0)
        c[5:8, 0] = 120.0
        c[8:, 0] = 90.0                                               # 90 < 0.8 × 120 = 96
        r = sh.trade(_d(c), 0, 0, 20)
        self.assertAlmostEqual(r["ret"], 90.0 / 100.0 - 1 - sh.COST)
        self.assertEqual((r["days"], r["stopped"]), (8, True))
        self.assertAlmostEqual(r["hold"], 90.0 / 100.0 - 1 - sh.COST)

    def test_a_delisting_settles_without_counting_as_a_stop(self):
        c = np.full((30, 1), 100.0)
        c[11:, 0] = 70.0                                              # settled price after the last trading day 10
        r = sh.trade(_d(c, last=[10]), 0, 0, 20)
        self.assertAlmostEqual(r["ret"], 0.7 - 1 - sh.COST)
        self.assertFalse(r["stopped"])

    def test_no_trade_without_the_full_window(self):
        self.assertIsNone(sh.trade(_d(np.full((10, 1), 100.0)), 0, 0, 20))


class SelectionTests(unittest.TestCase):
    def test_picks_meet_every_strength_condition_and_the_control_draws_from_the_rest(self):
        n, k = 30, 4
        c = np.full((n, k), 100.0)
        c[:, 0] = np.linspace(100, 140, n)                            # the pick rises
        d = _d(c)
        ones = np.ones((n, k))
        f = {"rs_pct": ones * np.array([0.9, 0.9, 0.5, 0.95]), "close": ones * 100, "sma200": ones * np.array([80, 80, 80, 120]),
             "sma50": ones * 90, "ret_63d": ones * np.array([0.2, 0.05, 0.2, 0.2])}
        self.assertEqual([sh.strong(f, 0, j) for j in range(k)], [True, False, False, False])
        r = sh.formation(d, f, 0, np.random.default_rng(1), 20, sh.RS_MIN)
        self.assertEqual((r["n"], r["pool"]), (1, 4))
        self.assertAlmostEqual(r["ret"], c[20, 0] / c[1, 0] - 1 - sh.COST)
        self.assertAlmostEqual(r["control"], -sh.COST)                # flat stocks


class DecideTests(unittest.TestCase):
    def _rows(self, edge_of):
        return {"rows": [{"date": pd.Timestamp(y, m, 15), "n": 5, "ret": 0.1 + edge_of(y, m), "control": 0.1, "hold_same": 0.1,
                          "control_hold": 0.1, "spy": 0.08, "trades": [{"ret": 0.1, "hold": 0.1, "days": 100, "stopped": False}]}
                         for y in range(2000, 2020) for m in range(1, 13)], "filter_off": 0}

    def test_passes_with_a_steady_edge(self):
        dec = sh.decide(sh.evaluate(self._rows(lambda y, m: 0.02 + 0.002 * (m % 3))))
        self.assertTrue(dec["passed"], dec)

    def test_fails_when_the_edge_lives_only_in_2008_09(self):
        dec = sh.decide(sh.evaluate(self._rows(lambda y, m: (0.5 if y in (2008, 2009) else -0.01) + 0.001 * (m % 3))))
        self.assertFalse(dec["checks"]["ex_2008_09"])
        self.assertFalse(dec["passed"])


if __name__ == "__main__":
    unittest.main()
