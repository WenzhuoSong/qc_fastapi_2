import unittest
from datetime import date

import numpy as np
import pandas as pd

from scout import market_day


def series(rets: list[float], start: str = "2000-01-03") -> pd.Series:
    idx = [d.date() for d in pd.bdate_range(start, periods=len(rets) + 1)]
    return pd.Series(100 * np.cumprod([1.0, *[1 + r for r in rets]]), index=idx)


class MarketDayTests(unittest.TestCase):
    def test_trigger(self):
        self.assertTrue(market_day.triggered(-0.044, 0.01))
        self.assertTrue(market_day.triggered(0.021, 0.008))       # 2.6 sigmas
        self.assertFalse(market_day.triggered(-0.015, 0.01))
        self.assertFalse(market_day.triggered(None, 0.01))
        self.assertFalse(market_day.triggered(float("nan"), 0.01))

    def test_history_counts_same_direction_and_what_followed(self):
        rets = [0.001] * 300
        rets[100] = -0.05              # one past drop of 5 %, then a steady drift up
        rets[200] = -0.041             # and one of 4.1 %
        rets[250] = 0.06               # an up day: not counted for a drop
        rets.append(-0.044)            # today
        h = market_day.history(series(rets))
        self.assertAlmostEqual(h["ret"], -0.044, places=6)
        self.assertEqual(h["threshold"], 0.04)
        self.assertEqual(h["n"], 2)
        self.assertEqual(h["rank"], 2)        # only the 5 % drop was larger
        self.assertEqual(h["episodes"], 2)
        a = h["after"]["21"]
        self.assertEqual(a["n"], 2)
        self.assertEqual(a["up"], 1.0)

    def test_episodes_merge_clustered_days(self):
        rets = [0.001] * 300
        for i in (100, 105, 110, 200):
            rets[i] = -0.05
        rets.append(-0.05)
        h = market_day.history(series(rets))
        self.assertEqual(h["n"], 4)
        self.assertEqual(h["episodes"], 2)

    def test_breadth_sectors_holdings(self):
        b = market_day.breadth(pd.Series([-0.01, -0.06, 0.02, np.nan]), up=False)
        self.assertEqual(b["n"], 3)
        self.assertAlmostEqual(b["with"], 2 / 3)
        self.assertAlmostEqual(b["big"], 1 / 3)
        rows = [{"kind": "sector", "name": n, "ret_1d": r} for n, r in (("科技", -0.08), ("公用", -0.007), ("能源", -0.03), ("金融", -0.02))]
        s = market_day.sector_moves(rows + [{"kind": "theme", "name": "半导体", "ret_1d": -0.11}], up=False)
        self.assertEqual([n for n, _ in s["lead"]], ["科技", "能源", "金融"])
        self.assertEqual(s["least"][0][0], "公用")
        hd = market_day.holdings_day([{"value": 9_000, "ret_1d": -0.1, "market_ret_1d": -0.05},
                                      {"value": 0, "ret_1d": 0.1, "market_ret_1d": 0.0}], 100_000)
        self.assertAlmostEqual(hd["pnl"], -1_000)
        self.assertAlmostEqual(hd["market"], -500)
        self.assertAlmostEqual(hd["pct"], -0.01)
        self.assertIsNone(market_day.holdings_day([], 1.0))

    def test_cause_masks_numbers_of_unopened_pages(self):
        facts = [{"id": 1, "status": "verified", "subject": "MACRO", "claim": "芯片出口限制扩大，半导体股领跌 11%",
                  "outlet": "Reuters", "published": date(2026, 11, 5), "url": "u1"},
                 {"id": 2, "status": "unreachable", "subject": "MACRO", "claim": "美债收益率升至 5.1%",
                  "outlet": "WSJ", "published": date(2026, 11, 5), "url": "u2"},
                 {"id": 3, "status": "mismatch", "subject": "MACRO", "claim": "x", "outlet": "y", "published": None, "url": "u3"},
                 {"id": 4, "status": "verified", "subject": "NVDA", "claim": "y", "outlet": "y", "published": None, "url": "u4"},
                 {"id": 5, "status": "verified", "subject": "MACRO", "claim": "旧闻", "outlet": "y",
                  "published": date(2026, 10, 1), "url": "u5"}]
        out = market_day.cause_lines(facts, date(2026, 11, 5))
        self.assertEqual([c["id"] for c in out], [1, 2])
        self.assertIn("11%", out[0]["claim"])
        self.assertEqual(out[1]["claim"], "有报道称：美债收益率升至 …%")
        self.assertIn("未核对", out[1]["source"])

    def test_lines(self):
        d = {"date": "2026-11-05", "ret": -0.044, "sigma": 0.011, "up": False, "qqq": -0.065,
             "history": {"since": "1993-01-29", "ret": -0.044, "sigma": 0.011, "sigmas": 4.0, "rank": 37, "threshold": 0.04,
                         "n": 47, "episodes": 14, "years": [[2008, 16], [2020, 9], [2009, 6]], "last": "2025-04-10",
                         "after": {"5": {"n": 47, "up": 0.6, "median": 0.014, "p10": -0.062},
                                   "21": {"n": 47, "up": 0.66, "median": 0.034, "p10": -0.112}}},
             "vix": [18.0, 27.0], "breadth": {"n": 900, "with": 0.92, "big": 0.31},
             "sectors": {"lead": [["信息技术", -0.08]], "least": [["公用事业", -0.007]]},
             "holdings": {"pnl": -9_870, "market": -6_500, "pct": -0.04},
             "cause": [{"claim": "芯片出口限制扩大", "source": "Reuters 2026-11-05"}]}
        text = "\n".join(market_day.lines(d))
        self.assertIn("⚡ 大跌日：SPY -4.4%，QQQ -6.5%；约 4.0 倍日常波动", text)
        self.assertIn("1993 年以来第 37 大的单日跌幅", text)
        self.assertIn("单日跌 4.0% 以上共 47 天，分属 14 段行情，最多的年份：2008（16 天）", text)
        self.assertIn("1 个月：66% 的时候上涨，中位数 +3.4%，十次里较差的一次 -11.2%（47 次）", text)
        self.assertIn("92% 下跌，31% 跌超 5%", text)
        self.assertIn("领跌：信息技术 -8.0%", text)
        self.assertIn("最抗跌：公用事业 -0.7%", text)
        self.assertIn("VIX 18.0 → 27.0（+50%）", text)
        self.assertIn("持仓今天合计 -9,870（占账户 -4.0%），其中按 β 算的大盘部分约 -6,500", text)
        self.assertIn("β 差 0.3 × SPY -4.4% ≈ 1.3 个百分点", text)
        self.assertIn("原因（据外部报道（未经核实））：", text)
        self.assertIn("不是预测", text)
        self.assertEqual(market_day.lines(None), [])
        d.update(cause=[], history=None)
        self.assertIn("外部报道里没有找到可用的说法", "\n".join(market_day.lines(d)))


if __name__ == "__main__":
    unittest.main()
