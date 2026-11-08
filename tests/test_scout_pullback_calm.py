import unittest

import numpy as np
import pandas as pd

from scout import pullback_calm as pc


class PullbackCalmTests(unittest.TestCase):
    def test_cooldown_keeps_the_first_signal_per_stock_in_ten_sessions(self):
        hits = np.zeros((30, 2), dtype=bool)
        hits[[0, 5, 10, 25], 0] = True
        hits[[3], 1] = True
        self.assertEqual(pc.cooldown(hits), [(0, 0), (3, 1), (10, 0), (25, 0)])

    def test_mask_follows_the_rule_conditions(self):
        f = {"rs_pct": np.array([[0.9, 0.9]]), "close": np.array([[100.0, 100.0]]), "sma200": np.array([[80.0, 80.0]]),
             "sma50": np.array([[98.0, 98.0]]), "ret_63d": np.array([[0.2, 0.2]]), "rsi2": np.array([[5.0, 15.0]]),
             "sma20": np.array([[102.0, 102.0]]), "vol_ratio": np.array([[1.0, 1.0]])}
        self.assertEqual(pc.mask(f, np.array([[True, True]])).tolist(), [[True, False]])      # RSI(2) 15 fails

    def test_place_fills_and_exits_with_the_rules_bracket(self):
        n = 20
        c = np.full((n, 1), 100.0)
        d = {"o": c.copy(), "h": c + 1, "l": c - 1, "c": c.copy()}
        d["l"][5, 0] = 80.0                                         # a deep low after the fill: stopped
        f = {"close": c, "atr14": np.full((n, 1), 2.0), "low_5": np.full((n, 1), 98.0), "sma50": np.full((n, 1), 97.0),
             "high_20": np.full((n, 1), 110.0)}
        r = pc.place(d, f, 0, 0)
        self.assertEqual((r["fill"], r["state"]), (1, "stopped"))
        self.assertLess(r["ret"], 0)

    def test_decide_needs_every_check(self):
        rows = []
        for m in range(1, 13):
            for y, inside in ((2001, True), (2010, True), (2001, False), (2010, False)):
                rows.append({"date": pd.Timestamp(y, m, 15), "in": inside, "filter_on": True, "calm": inside, "state": "expired",
                             "ret": 0.01, "select": (0.02 + 0.001 * m) if inside else 0.0, "hold": 0.005})
        dec = pc.decide(pc.evaluate(rows))
        self.assertTrue(dec["passed"], dec)
        for r in rows:
            r["hold"] = -0.01
        self.assertFalse(pc.decide(pc.evaluate(rows))["passed"])


if __name__ == "__main__":
    unittest.main()
