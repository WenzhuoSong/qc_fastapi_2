import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

from scout import sharadar_research as sr
from scout.ml import tilt


class TiltTests(unittest.TestCase):
    def test_targets_are_long_only_tilts_over_the_members(self):
        w, b = tilt.targets(np.array([0, 2, 3]), np.array([-1.0, 0.0, 1.0]), 5)
        self.assertAlmostEqual(w.sum(), 1.0)
        np.testing.assert_allclose(w, [0, 0, 1 / 3, 2 / 3, 0])
        np.testing.assert_allclose(b, [1 / 3, 0, 1 / 3, 1 / 3, 0])

    def test_band_trades_only_large_gaps_and_exits(self):
        w = np.array([0.25, 0.25, 0.25, 0.25, 0.0])
        target = np.array([0.30, 0.20, 0.50, 0.0, 0.0])            # n = 3 members: band = 0.5 / 3
        new = tilt.rebalance(w, target, 3, True)
        self.assertEqual(new[3], 0.0)                               # left the universe: sold
        self.assertGreater(new[2], new[0])                          # 0.25 → 0.50 is outside the band
        self.assertAlmostEqual(new[0] / new[1], 1.0)                # small gaps untouched
        self.assertAlmostEqual(new.sum(), 1.0)

    def test_costs_and_benchmark(self):
        R = np.zeros((6, 2))
        R[2:, 0] = 0.01
        sim = tilt.simulate(R, [(0, np.array([0, 1]), np.array([1.0, -1.0]))], 5, False)
        np.testing.assert_allclose(sim["port"], [0.01] * 4)          # all in stock 0 from the close of day 1
        np.testing.assert_allclose(sim["bench"], [0.005, 0.005 * 1.01 / 1.005, *sim["bench"][2:]])
        sim = tilt.simulate(np.zeros((4, 2)), [(0, np.array([0, 1]), np.array([0.0, 0.0])),
                                              (1, np.array([0, 1]), np.array([1.0, -1.0]))], 3, False)
        self.assertAlmostEqual(sim["port"][0], -tilt.ONE_WAY * 1.0)  # moved 0.5 / 0.5 → 1 / 0: turnover 1
        self.assertEqual(sim["turnover"], [1.0, 1.0])

    def test_delisting_haircut_lands_after_the_last_close(self):
        dates = pd.bdate_range("2005-01-03", periods=5)
        c = np.array([[10.0], [11.0], [np.nan], [12.1], [np.nan]])
        p = sr.Prices(dates, ["A"], c, c, c, c, c, np.ones_like(c), np.array([-0.3]))
        r = tilt.daily_returns(p)[:, 0]
        np.testing.assert_allclose(r, [0, 0.1, 0, 0.1, -0.3])         # halt made up on resumption

    def test_confirmation_runs_once(self):
        with tempfile.TemporaryDirectory() as d:
            lock = Path(d) / "tilt.json"
            lock.write_text("{}")
            with mock.patch.object(tilt, "RESULT", lock), mock.patch.object(tilt, "load") as load:
                with self.assertRaises(SystemExit):
                    tilt.run_confirmation()
                load.assert_not_called()


if __name__ == "__main__":
    unittest.main()
