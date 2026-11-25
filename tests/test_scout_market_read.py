import unittest
from datetime import date

import numpy as np
import pandas as pd

from scout import market_read as mr


def pack(**over):
    p = {"data_date": "2026-11-24",
         "moves": [{"key": "move.SPY", "名称": "标普 500（SPY）", "含义": "大盘", "当日": "-1.8%", "5 日": "-2.0%", "20 日": "-1.0%",
                    "日常波动倍数": "-2.4", "超出日常波动": True},
                   {"key": "move.TLT", "名称": "长期美债（TLT）", "含义": "", "当日": "+0.2%", "5 日": "+0.1%", "20 日": "-3.1%",
                    "日常波动倍数": "+0.3", "超出日常波动": False}],
         "breadth": {"key": "breadth", "上涨股票比例": "21%"},
         "contributions": {"key": "contrib", "最大 10 家合计贡献": "-1.10 个百分点", "最大 10 家占指数涨跌": "61%"},
         "sectors": {"key": "sectors", "领先": [], "落后": []}, "macro_data": [], "odds": [], "earnings": [], "news": [
             {"key": "news.1", "标题": "Stocks slide"}], "causes": [], "holdings": [], "filter": None, "yesterday": None,
         "sentiment": [{"key": "snt.pc", "名称": "个股期权看跌 / 看涨比（10 日平均）", "数值": "0.71（2026-11-24）",
                        "历史位置": "高于 2019 年以来 55% 的时间", "读数": "正常范围", "含义": ""}],
         "_strong": ["contrib", "move.SPY"]}
    p.update(over)
    return p


def read(**over):
    base = dict(headline="标普 500 当日 -1.8%，大型科技股拖累", what_happened="SPY 当日 -1.8%，约 -2.4 倍日常波动；上涨股票只有 21%。",
                drivers=[mr.Driver(title="大公司拖累", explanation="最大 10 家合计贡献 -1.10 个百分点，占 61%", evidence=["contrib"], strength="强")],
                media_vs_data="新闻说股市下滑，数据支持。", story_status="无前一天", story="没有昨天的解读。",
                for_you="持仓照计划持有。", would_change="若普通股票表现更差，就不只是大公司的问题。",
                mood="个股期权看跌 / 看涨比 0.71，正常范围。")
    base.update(over)
    return mr.Read(**base)


class MoveTests(unittest.TestCase):
    def test_day_in_its_own_sigmas(self):
        idx = pd.bdate_range("2026-01-01", periods=100).date
        rng = np.random.default_rng(1)
        s = pd.Series(100 * np.cumprod(1 + rng.normal(0, 0.01, 100)), index=idx)
        s.iloc[-1] = s.iloc[-2] * 0.95
        m = mr.sigma_move(s, idx[-1])
        self.assertAlmostEqual(m["day"], -0.05)
        self.assertLess(m["sigmas"], -3)
        self.assertTrue(mr.move_item("move.X", "X", "", m)["超出日常波动"])

    def test_yield_changes_are_levels(self):
        idx = pd.bdate_range("2026-01-01", periods=80).date
        s = pd.Series(np.linspace(4.0, 4.4, 80), index=idx)
        m = mr.sigma_move(s, idx[-1], level=True)
        self.assertAlmostEqual(m["d20"], 0.4 * 20 / 79, places=6)
        self.assertEqual(mr.move_item("move.TNX", "10 年", "", m, level=True)["最新"], "4.40")


class ContributionTests(unittest.TestCase):
    def test_weights_times_returns(self):
        caps = pd.Series({f"S{i}": 1.0 for i in range(120)} | {"BIG": 120.0})
        rets = pd.Series({f"S{i}": 0.0 for i in range(120)} | {"BIG": -0.02})
        c = mr.contributions(rets, caps, -0.01)
        self.assertEqual(c["估算合计"], "-1.00 个百分点")
        self.assertEqual(c["拖累最多"][0]["代码"], "BIG")
        self.assertEqual(c["拖累最多"][0]["占指数涨跌"], "100%")
        self.assertEqual(c["_strong"], ["BIG"])

    def test_too_few_members(self):
        self.assertIsNone(mr.contributions(pd.Series({"A": 0.01}), pd.Series({"A": 1.0}), 0.01))


class ValidationTests(unittest.TestCase):
    def test_clean_read_passes(self):
        self.assertEqual(mr.validate(read(), pack()), [])

    def test_strong_needs_strong_evidence(self):
        r = read(drivers=[mr.Driver(title="债市", explanation="TLT 当日 +0.2%", evidence=["move.TLT"], strength="强")])
        self.assertTrue(any("标为「强」" in x for x in mr.validate(r, pack())))

    def test_numbers_keys_forecasts_and_story(self):
        r = read(headline="标普 500 当日 -2.7%，将继续下跌",
                 drivers=[mr.Driver(title="x", explanation="y", evidence=["news.9"], strength="弱")], story_status="延续")
        issues = mr.validate(r, pack())
        for part in ("2.7", "news.9", "将继续", "无前一天"):
            self.assertTrue(any(part in x for x in issues), part)

    def test_fallback_is_valid(self):
        p = pack()
        self.assertEqual(mr.validate(mr.fallback(p), p), [])


class TextTests(unittest.TestCase):
    def test_short_lines_escape_and_cap_drivers(self):
        rec = {"read": read(drivers=[mr.Driver(title=f"<d{i}>", explanation="e", evidence=["contrib"], strength="中")
                                     for i in range(4)]).model_dump()}
        lines = mr.short_lines(rec)
        self.assertTrue(lines[0].startswith("<b>【市场解读】</b>"))
        self.assertEqual(sum(x.startswith("• ") for x in lines), 3)
        self.assertIn("&lt;d0&gt;", lines[1])
        self.assertEqual(mr.short_lines(None), [])

    def test_full_text_says_it_is_not_a_forecast(self):
        self.assertIn("不是预测", mr.render_text(read(), date(2026, 11, 24), "llm"))


if __name__ == "__main__":
    unittest.main()
