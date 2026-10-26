import unittest
from datetime import date

import pandas as pd

from scout import premarket
from scout.context import parse_economic_calendar
from scout.data.yahoo import quote_from
from scout.premarket import PremarketBrief, Point, TickerNote

OPP = {"status": "new", "entry_lo": 100.0, "entry_hi": 104.0, "stop": 95.0, "current_stop": None, "target1": 115.0}

PACK = {
    "session": "2026-10-27", "weekday": "周二", "prev_session": "2026-10-26",
    "overnight": [{"key": "q.ES=F", "名称": "标普期货", "最新": "7684.25", "较前收": "+0.3%", "时间（美东）": "10-27 08:55"},
                  {"key": "q.^TNX", "名称": "10 年美债收益率", "最新": "4.99%", "说明": "今天尚未交易，为上一交易日报价"}],
    "opportunities": [{"key": "opp.NVDA", "代码": "NVDA", "规则": "trend_pullback", "状态": "待触发", "入场区间": "100.00–104.00",
                       "止损": "95.00", "目标 1": "115.00", "盘前价": "102.00", "盘前较前收": "+1.0%",
                       "位置判断": "在入场区间内（100.00–104.00）"}],
    "watchlist_moves": [],
    "earnings_reported": [{"key": "er.MSFT", "代码": "MSFT", "名称": "Microsoft", "身份": "大市值", "时间": "昨日盘后",
                           "EPS 预期": "2.90", "EPS 实际": "3.10", "EPS 超预期幅度": "+6.9%", "盘前价": "540.00", "盘前较前收": "+3.2%"}],
    "earnings_today": [{"key": "et.GOOGL", "代码": "GOOGL", "名称": "Alphabet", "身份": "大市值", "时间": "盘后", "EPS 预期": "2.30"}],
    "earnings_week": [],
    "economic_today": [{"key": "econ_1", "事件": "谘商会消费者信心", "日期": "2026-10-27", "美东时间": "10:00",
                        "北京时间": "10-27 22:00", "重要性": "中", "预期": "80.5", "前值": "81.9"}],
    "economic_week": [],
    "news": [{"key": "news_1", "标题": "Microsoft beats", "来源": "Reuters", "时间（美东）": "10-26 16:30",
              "相关股票": ["MSFT"], "情绪": "positive"}],
    "macro_news": [],
    "last_focus": None,
}


def brief(**kw) -> PremarketBrief:
    base = dict(headline="期货小幅走高，关注微软财报后表现", overnight="标普期货 +0.3%。",
                points=[Point(text="MSFT 盘后 EPS 超预期 +6.9%，盘前 +3.2%", evidence=["er.MSFT"]),
                        Point(text="10:00 公布消费者信心，预期 80.5", evidence=["econ_1"])],
                tickers=[TickerNote(ticker="NVDA", note="在入场区间内，开盘后看能否守住区间", evidence=["opp.NVDA"])],
                risks=["今晚 GOOGL 盘后财报"])
    base.update(kw)
    return PremarketBrief(**base)


class PositionLabelTests(unittest.TestCase):
    def test_pending_opportunity(self):
        self.assertIn("跌破止损", premarket.position_label(94.0, OPP))
        self.assertIn("低于入场区间", premarket.position_label(98.0, OPP))
        self.assertIn("在入场区间内", premarket.position_label(102.0, OPP))
        self.assertIn("追高风险", premarket.position_label(108.0, OPP))
        self.assertIn("已高于目标 1", premarket.position_label(116.0, OPP))

    def test_triggered_uses_current_stop(self):
        o = {**OPP, "status": "triggered", "current_stop": 101.0}
        self.assertIn("跌破止损 101.00", premarket.position_label(100.5, o))
        self.assertIn("距止损", premarket.position_label(105.0, o))

    def test_eps_surprise(self):
        self.assertAlmostEqual(premarket.eps_surprise(3.1, 2.9), 0.2 / 2.9)
        self.assertAlmostEqual(premarket.eps_surprise(-0.5, -1.0), 0.5)
        self.assertIsNone(premarket.eps_surprise(None, 2.9))
        self.assertIsNone(premarket.eps_surprise(1.0, 0))


class QuoteTests(unittest.TestCase):
    def test_change_vs_previous_session_close(self):
        idx = pd.DatetimeIndex(["2026-10-27 08:50", "2026-10-27 08:55"]).tz_localize("America/New_York")
        daily = pd.Series([100.0, 102.0, 103.0], index=pd.to_datetime(["2026-10-23", "2026-10-26", "2026-10-27"]))
        q = quote_from(pd.Series([103.5, 104.04], index=idx), daily, date(2026, 10, 27))
        self.assertEqual(q["prev_close"], 102.0)
        self.assertAlmostEqual(q["change"], 0.02)
        self.assertFalse(q["stale"])
        self.assertEqual(q["time_et"], "10-27 08:55")

    def test_stale_print_has_no_change(self):
        idx = pd.DatetimeIndex(["2026-10-26 14:55"]).tz_localize("America/New_York")
        q = quote_from(pd.Series([4.99], index=idx), pd.Series([4.97], index=pd.to_datetime(["2026-10-26"])), date(2026, 10, 27))
        self.assertTrue(q["stale"])
        self.assertIsNone(q["change"])

    def test_quote_rows(self):
        self.assertEqual(premarket.quote_row("ES=F", "标普期货", None)["最新"], "无数据")
        tnx = {"last": 4.99, "prev_close": 4.97, "change": 0.004, "time_et": "10-27 08:55", "stale": False}
        self.assertEqual(premarket.quote_row("^TNX", "10 年美债收益率", tnx)["较前收"], "+2.0bp")


class EconomicCalendarTests(unittest.TestCase):
    def test_parse_filters_country_and_impact(self):
        items = [
            {"title": "CB Consumer Confidence", "country": "USD", "date": "2026-10-27T10:00:00-04:00", "impact": "Medium",
             "forecast": "80.5", "previous": "81.9"},
            {"title": "Federal Funds Rate", "country": "USD", "date": "2026-10-28T14:00:00-04:00", "impact": "High",
             "forecast": "4.00%", "previous": "4.00%"},
            {"title": "Bank Holiday", "country": "USD", "date": "2026-10-27T00:00:00-04:00", "impact": "Holiday"},
            {"title": "CPI y/y", "country": "EUR", "date": "2026-10-27T05:00:00-04:00", "impact": "High"},
        ]
        ev = parse_economic_calendar(items)
        self.assertEqual([e["title_zh"] for e in ev], ["谘商会消费者信心", "联邦基金利率决议"])
        self.assertEqual((ev[0]["time_et"], ev[0]["time_bj"], ev[0]["impact"]), ("10:00", "10-27 22:00", "中"))


class ValidationTests(unittest.TestCase):
    def test_clean_brief_passes(self):
        self.assertEqual(premarket.validate(brief(), PACK), [])

    def test_catches_numbers_keys_tickers_and_position_misreads(self):
        bad = brief(points=[Point(text="期货上涨 1.8%", evidence=["q.NQ=F"]), Point(text="见下", evidence=[])],
                    tickers=[TickerNote(ticker="NVDA", note="盘前已跌破止损", evidence=["opp.NVDA"]),
                             TickerNote(ticker="TSLA", note="关注", evidence=[])])
        issues = "\n".join(premarket.validate(bad, PACK))
        self.assertIn("1.8", issues)
        self.assertIn("q.NQ=F", issues)
        self.assertIn("TSLA", issues)
        self.assertIn("NVDA 的位置判断", issues)

    def test_point_count(self):
        self.assertTrue(any("2–5" in i for i in premarket.validate(brief(points=[Point(text="x", evidence=[])]), PACK)))


class RenderTests(unittest.TestCase):
    def test_render_has_every_section(self):
        text = premarket.render_text(brief(), PACK)
        self.assertTrue(text.startswith("【盘前简报 · 2026-10-27 周二】"))
        self.assertIn("隔夜：标普期货 +0.3% · 10 年美债收益率 4.99%（上一交易日）", text)
        self.assertIn("10:00 / 22:00 [中] 谘商会消费者信心 预期 80.5 前值 81.9", text)
        self.assertIn("MSFT 昨日盘后 EPS 3.10 vs 预期 2.90（+6.9%），盘前 +3.2%", text)
        self.assertIn("今日待公布：GOOGL（盘后）", text)
        self.assertIn("NVDA（待触发）102.00 在入场区间内", text)

    def test_fallback_is_valid_shape(self):
        fb = premarket.fallback(PACK)
        self.assertGreaterEqual(len(fb.points), 2)
        self.assertIn("+0.3%", fb.headline)

    def test_card_renders(self):
        from scout.web.views import premarket_card

        rec = {"source": "llm", "model": "m", "brief": brief().model_dump(), "pack": PACK}
        html = premarket_card(rec, current=True)
        self.assertIn("盘前简报 · 2026-10-27", html)
        self.assertIn("谘商会消费者信心", html)
        self.assertIn("<details", premarket_card(rec, current=False))


if __name__ == "__main__":
    unittest.main()
