import unittest

import numpy as np
import pandas as pd

from scout import knowledge, sectors
from scout.features import build_panel


def _panel(n=260, seed=3):
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2025-06-02", periods=n).date
    specs = {"SPY": 0.0005, "XLE": 0.002, "XLK": -0.001, "AAA": 0.002, "BBB": 0.0015, "CCC": -0.001, "DDD": -0.002}
    bars = {}
    for t, drift in specs.items():
        close = 100 * np.exp(np.cumsum(rng.normal(drift, 0.01, n)))
        bars[t] = pd.DataFrame({"open": close, "high": close * 1.01, "low": close * 0.99, "close": close,
                                "volume": rng.integers(2_000_000, 3_000_000, n).astype(float)}, index=dates)
    inst = {"SPY": {"kind": "etf"}, "XLE": {"kind": "etf"}, "XLK": {"kind": "etf"},
            "AAA": {"kind": "stock", "sector": "Energy"}, "BBB": {"kind": "stock", "sector": "Energy"},
            "CCC": {"kind": "stock", "sector": "Information Technology"},
            "DDD": {"kind": "stock", "sector": "Information Technology"}}
    return build_panel(bars, inst)


class SectorTests(unittest.TestCase):
    def test_quadrants(self):
        self.assertEqual(sectors.quadrant(103, 102), "leading")
        self.assertEqual(sectors.quadrant(103, 98), "weakening")
        self.assertEqual(sectors.quadrant(97, 98), "lagging")
        self.assertEqual(sectors.quadrant(97, 101), "improving")

    def test_compute_ranks_strong_sector_hotter(self):
        p = _panel()
        rows = {r["key"]: r for r in sectors.compute(p, p.dates[-1]) if r["kind"] == "sector"}
        self.assertEqual(set(rows), {"XLE", "XLK"})
        self.assertGreater(rows["XLE"]["heat"], rows["XLK"]["heat"])
        self.assertGreater(rows["XLE"]["rel_20"], rows["XLK"]["rel_20"])
        for r in rows.values():
            self.assertEqual(r["members"], 2)
            self.assertTrue(0 <= r["pct_above_50"] <= 1)
            self.assertEqual(len(r["tail"]), sectors.TAIL_POINTS)
            self.assertIn(r["heat_label"], ("热", "温", "冷"))
            self.assertTrue(-1 <= r["cmf_20"] <= 1)

    def test_sector_metrics_are_in_the_dictionary(self):
        for key in ("rel_20", "rs_ratio", "rs_momentum", "pct_above_50", "net_new_highs", "cmf_20",
                    "rel_dollar_vol", "dollar_vol_share", "heat", "updown_vol_20"):
            self.assertIn(key, knowledge.features(), key)
        self.assertEqual(knowledge.fmt("cmf_20", 0.123), "+0.12")
        self.assertEqual(knowledge.fmt("rs_ratio", 103.04), "103.0")


if __name__ == "__main__":
    unittest.main()
