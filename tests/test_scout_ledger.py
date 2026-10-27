import unittest
from datetime import date

import pandas as pd

from scout import ledger


class LedgerTests(unittest.TestCase):
    def test_hit_rules(self):
        self.assertTrue(ledger.hit(1, 0.02, 20))
        self.assertFalse(ledger.hit(1, -0.01, 20))
        self.assertTrue(ledger.hit(-1, -0.05, 20))
        self.assertTrue(ledger.hit(0, 0.02, 20))           # inside ±3 %
        self.assertFalse(ledger.hit(0, 0.02, 5))           # outside ±1.5 %
        self.assertIsNone(ledger.hit(1, None, 20))

    def test_overall_stance(self):
        self.assertEqual(ledger.overall_stance(["偏多", "偏多", "偏空", "不适用"]), 1)
        self.assertEqual(ledger.overall_stance(["偏多", "偏空"]), 0)
        self.assertEqual(ledger.overall_stance(["偏空", "中性", "不适用"]), -1)

    def test_forward_uses_sessions_not_calendar_days(self):
        idx = [date(2026, 10, d) for d in (19, 20, 21, 22, 23, 26, 27)]
        closes = pd.Series([100, 101, 102, 103, 104, 110, 111], index=idx)
        end, ret = ledger.forward(closes, date(2026, 10, 24), 1)          # Saturday → Friday's close is the base
        self.assertEqual(end, date(2026, 10, 26))
        self.assertAlmostEqual(ret, 110 / 104 - 1)
        self.assertIsNone(ledger.forward(closes, date(2026, 10, 26), 5))  # not elapsed yet


if __name__ == "__main__":
    unittest.main()
