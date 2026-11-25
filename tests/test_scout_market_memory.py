import unittest
from datetime import date

import pandas as pd

from scout import market_memory as mm


class AnchorTests(unittest.TestCase):
    def test_symbols_from_evidence_keys(self):
        self.assertEqual(mm.anchors_of(["move.SMH", "move.TNX", "global.N225", "global.000001SS", "contrib", "style.RSP_SPY", "move.SMH"]),
                         ["SMH", "^TNX", "^N225", "000001.SS"])


class SinceTests(unittest.TestCase):
    def test_from_the_close_before_the_start(self):
        s = pd.Series([100.0, 90.0, 81.0], index=[date(2026, 11, 4), date(2026, 11, 5), date(2026, 11, 6)])
        self.assertAlmostEqual(mm.since(s, date(2026, 11, 5), date(2026, 11, 6)), -0.19)
        self.assertAlmostEqual(mm.since(s, date(2026, 11, 5), date(2026, 11, 6), level=True), -19.0)
        self.assertIsNone(mm.since(s, date(2026, 11, 4), date(2026, 11, 6)))       # nothing before the start
        self.assertIsNone(mm.since(None, date(2026, 11, 5), date(2026, 11, 6)))


if __name__ == "__main__":
    unittest.main()
