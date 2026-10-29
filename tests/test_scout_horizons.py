import unittest
from datetime import date, timedelta

import numpy as np

from scout import horizons


class ProfileTests(unittest.TestCase):
    def test_edges_per_horizon_and_a_fixed_split(self):
        start = date(2020, 1, 1)
        rows = [{"h": h, "date": start + timedelta(days=15 * k), "ret": 0.01 * h / 21 + (0.002 if k % 2 else -0.002), "control": 0.0}
                for k in range(80) for h in horizons.HORIZONS]
        prof = horizons.profile(rows)
        self.assertEqual(set(prof["horizons"]), set(horizons.HORIZONS))
        self.assertAlmostEqual(prof["horizons"][21]["all"]["edge"], 0.01, places=4)
        self.assertAlmostEqual(prof["horizons"][252]["all"]["edge"], 0.12, places=4)

    def test_overlap_correction_shrinks_t_for_persistent_monthly_errors(self):
        rng = np.random.default_rng(1)
        start = date(2015, 1, 1)
        noise = np.convolve(rng.normal(0, 0.01, 130), np.ones(12) / 12, mode="same")    # overlapping 12-month holdings
        dates = [start + timedelta(days=30 * k) for k in range(120)]
        diffs = list(0.002 + noise[:120])
        from scout.gate import monthly_t

        _, t_plain, _ = monthly_t(dates, diffs)
        self.assertLess(abs(horizons.nw_t(dates, diffs, 252)), abs(t_plain))


if __name__ == "__main__":
    unittest.main()
