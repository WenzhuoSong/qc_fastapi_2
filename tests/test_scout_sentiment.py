import unittest
from datetime import date

import numpy as np
import pandas as pd

from scout import sentiment as snt
from scout.data import sentiment as src


class SourceTests(unittest.TestCase):
    def test_cboe_ratios(self):
        data = {"ratios": [{"name": "TOTAL PUT/CALL RATIO", "value": "0.95"}, {"name": "EQUITY PUT/CALL RATIO", "value": "0.71"},
                           {"name": "INDEX PUT/CALL RATIO", "value": "1.12"}, {"name": "VIX PUT/CALL RATIO", "value": "0.4"},
                           {"name": "EQUITY PUT/CALL RATIO", "value": "n/a"}]}
        self.assertEqual(src.parse_cboe(data), {"total": 0.95, "equity": 0.71, "index": 1.12})


class ReadingTests(unittest.TestCase):
    def test_position_and_tone(self):
        h = pd.Series(np.arange(100, dtype=float))
        self.assertAlmostEqual(snt.position(h, 94.0), 0.95)
        self.assertEqual(snt.tone(0.95, fear_high=True), "偏恐慌")
        self.assertEqual(snt.tone(0.95, fear_high=False), "偏乐观")
        self.assertEqual(snt.tone(0.05, fear_high=False), "偏恐慌")
        self.assertEqual(snt.tone(0.5, fear_high=True), "正常范围")
        self.assertEqual(snt.tone(float("nan"), True), "历史不足")

    def test_item_wording(self):
        it = snt.item("snt.vix", "VIX", "30.0", 0.93, date(2006, 7, 17), True, "m")
        self.assertEqual(it["历史位置"], "高于 2006 年以来 93% 的时间")
        self.assertEqual(it["读数"], "偏恐慌")

    def test_breadth_counts_only_liquid_stocks(self):
        idx = pd.bdate_range("2024-01-01", periods=300).date
        up = np.linspace(10, 40, 300)
        close = pd.DataFrame({"A": up, "B": up[::-1], "C": up}, index=idx)
        liquid = pd.DataFrame(True, index=idx, columns=close.columns)
        liquid["C"] = False
        net, above = snt.breadth_series(close, liquid)
        self.assertAlmostEqual(net.iloc[-1], 0.0)                     # A at a new high, B at a new low, C ignored
        self.assertAlmostEqual(above.iloc[-1], 0.5)


if __name__ == "__main__":
    unittest.main()
