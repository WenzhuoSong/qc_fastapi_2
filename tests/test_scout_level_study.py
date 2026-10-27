import unittest

import numpy as np

from scout import level_study as ls


class FillTests(unittest.TestCase):
    O = np.array([105.0, 103.0, 99.0, 101.0])
    H = np.array([106.0, 104.0, 100.5, 102.0])
    L = np.array([104.0, 101.5, 98.0, 100.0])

    def test_pullback_fills_when_price_trades_into_the_zone(self):
        # day 1 only dips to 101.5; day 2 gaps down to 99 and fills at the zone floor, as the replay convention does
        self.assertEqual(ls.fill_limit(100.0, 101.0, self.O, self.H, self.L, 1, 4), (2, 100.0))
        self.assertIsNone(ls.fill_limit(100.0, 101.0, self.O, self.H, self.L, 0, 1))

    def test_gap_into_the_zone_fills_at_the_open(self):
        self.assertEqual(ls.fill_limit(98.5, 99.5, self.O, self.H, self.L, 2, 4), (2, 99.0))

    def test_breakout_needs_a_close_in_the_zone_on_volume(self):
        c = np.array([99.0, 101.0, 101.5, 104.0])
        vr = np.array([2.0, 1.2, 1.8, 3.0])
        self.assertEqual(ls.fill_close_confirmed(100.0, 102.0, c, vr, 0, 4), (2, 101.5))          # day 1 lacks volume
        self.assertIsNone(ls.fill_close_confirmed(100.0, 102.0, c, vr, 3, 4))                      # extended: not chased


if __name__ == "__main__":
    unittest.main()
