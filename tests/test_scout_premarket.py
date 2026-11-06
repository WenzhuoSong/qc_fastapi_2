import unittest
from datetime import date

import pandas as pd

from scout import premarket
from scout.context import parse_economic_calendar
from scout.data.yahoo import quote_from
from scout import holding_notes as hn
from scout.premarket import Notes, WatchNote

OPP = {"status": "new", "entry_lo": 100.0, "entry_hi": 104.0, "stop": 95.0, "current_stop": None, "target1": 115.0}

POS = {"key": "pos.HUM", "代码": "HUM", "盘前价": "380.00", "盘前较前收": "-7.7%", "持仓盈亏": "-8.4%（-1,745 美元）",
       "止损": "334.30（距盘前价 -12.0%）", "盘前拆分": "按 β 0.60 × 标普期货 -0.5% ≈ -0.3%，其余 -7.4% 是它自己的",
       "日常波动": "±2.1%（按 63 日波动）", "计划提示": "按计划持有", "买入理由": "强势回调中的买入机会",
       "财报": "HUM 今日盘前 EPS -1.10 vs 预期 -1.00（-9.5%），营收高于预期（Yahoo +1.2%）；盘前 -7.7%",
       "news": [{"index": 0, "标题": "Humana cuts 2027 outlook", "情绪（新闻库）": "negative"}]}

PACK = {
    "version": 2, "session": "2026-10-27", "weekday": "周二", "prev_session": "2026-10-26",
    "overnight": [{"key": "q.ES=F", "名称": "标普期货", "最新": "7684.25", "较前收": "+0.3%", "时间（美东）": "10-27 08:55",
                   "相对日常波动": "约 0.3 倍（SPY 日常波动 ±1.0%）"},
                  {"key": "q.^TNX", "名称": "10 年美债收益率", "最新": "4.99%", "说明": "今天尚未交易，为上一交易日报价"}],
    "positions": [POS],
    "opportunities": [{"key": "opp.NVDA", "代码": "NVDA", "规则": "trend_pullback", "入场区间": "100.00–104.00",
                       "止损": "95.00", "目标 1": "115.00", "盘前价": "102.00", "盘前较前收": "+1.0%",
                       "位置判断": "在入场区间内（100.00–104.00）"}],
    "watchlist_moves": [{"key": "pm.VST", "代码": "VST", "盘前价": "150.00", "盘前较前收": "-9.4%", "相对日常波动": "约 3.1 倍"}],
    "earnings_reported": [{"key": "er.MSFT", "代码": "MSFT", "名称": "Microsoft", "身份": "大市值", "时间": "昨日盘后",
                           "EPS 预期": "2.90", "EPS 实际": "3.10", "EPS 超预期幅度": "+6.9%", "盘前价": "540.00", "盘前较前收": "+3.2%",
                           "财报前期权定价": "±5.0%（到 10-31 到期）"}],
    "earnings_today": [{"key": "et.GOOGL", "代码": "GOOGL", "名称": "Alphabet", "身份": "持仓同板块（MU）", "时间": "盘后", "EPS 预期": "2.30"}],
    "earnings_week": [],
    "economic_today": [{"key": "econ_1", "事件": "非农就业人数", "日期": "2026-10-27", "美东时间": "08:30",
                        "北京时间": "10-27 20:30", "重要性": "高", "预期": "120K", "前值": "95K", "已公布": "已公布（在本简报之前），结果见宏观新闻"}],
    "economic_week": [],
    "news": [{"key": "news_1", "标题": "Microsoft beats", "来源": "Reuters", "时间（美东）": "10-26 16:30",
              "相关股票": ["MSFT"], "情绪": "positive"}],
    "macro_news": [], "calendar": [], "prediction": [],
    "key_items": ["你的持仓 HUM 今日盘前发布财报：EPS -1.10 vs 预期 -1.00（-9.5%）"],
}


def notes(**kw) -> Notes:
    base = dict(headline="期货小幅走高，HUM 财报后盘前下跌", overnight="标普期货 +0.3%，非农数据已公布。",
                holdings=[hn.HoldingNote(ticker="HUM", status="财报 EPS 低于预期，盘前 -7.7%，主要是它自己的下跌，离止损还有 -12.0%。",
                                         news=[hn.NewsNote(index=0, zh="Humana 下调 2027 年展望", impact="利空", vs_reason="无关",
                                                           why="展望下调影响盈利预期")])],
                watch=[WatchNote(ticker="NVDA", note="在入场区间内，开盘后看能否守住区间"),
                       WatchNote(ticker="MSFT", note="微软（MSFT）财报超预期，盘前 +3.2%")])
    base.update(kw)
    return Notes(**base)


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
    def test_clean_notes_pass(self):
        self.assertEqual(premarket.validate(notes(), PACK), [])

    def test_catches_numbers_tickers_positions_and_actions(self):
        bad = notes(headline="期货上涨 1.8%，建议买入", watch=[WatchNote(ticker="NVDA", note="盘前已跌破止损"),
                                                         WatchNote(ticker="TSLA", note="关注"), WatchNote(ticker="HUM", note="x")])
        issues = "\n".join(premarket.validate(bad, PACK))
        self.assertIn("1.8", issues)
        self.assertIn("TSLA", issues)
        self.assertIn("NVDA 的位置判断", issues)
        self.assertIn("watch 不要放持仓（HUM）", issues)
        self.assertIn("建议买入", issues)

    def test_holdings_use_the_digest_checks(self):
        n = notes()
        n.holdings[0].status = "财报后有望反弹，盘前 -7.7%。"
        issues = "\n".join(premarket.validate(n, PACK))
        self.assertIn("HUM 不要预测涨跌：有望反弹", issues)
        n.holdings = []
        self.assertIn("持仓不一致", "\n".join(premarket.validate(n, PACK)))

    def test_revenue_verdict(self):
        v = premarket.revenue_verdict
        self.assertEqual(v(114.477e9, 113.27e9, "Yahoo"), "高于预期（Yahoo +1.1%）")          # Apple 2026-11-02
        self.assertEqual(v(3.742e9, 3.744e9, "Yahoo"), "基本持平（Yahoo -0.1%）")             # Shopify 2026-11-03
        self.assertTrue(v(90.0, 100.0, "Finnhub").startswith("低于预期"))
        self.assertIsNone(v(None, 100.0, "Yahoo"))
        self.assertIsNone(v(90.0, None, "Yahoo"))

    def test_consensus_prefers_yahoo_and_distrusts_late_finnhub(self):
        late = {"eps_estimate": 2.03, "revenue_estimate": 115.66e9}
        self.assertEqual(premarket.consensus(late, {"eps": 1.99, "revenue": 113.27e9}), ("Yahoo", 1.99, 113.27e9))
        self.assertIsNone(premarket.consensus(late, {}))
        self.assertEqual(premarket.consensus({**late, "estimates_before_report": True}, {"eps": None, "revenue": None}),
                         ("Finnhub", 2.03, 115.66e9))

    def test_revenue_claims_must_match_the_verdict(self):
        rows = [{"代码": "AAPL", "名称": "Apple Inc", "营收对比预期": "高于预期（Yahoo +0.9%）"},
                {"代码": "SHOP", "名称": "Shopify Inc", "营收对比预期": "各家预期口径不一，不能下结论（Finnhub -2.1%，Yahoo -0.1%）"},
                {"代码": "PLTR", "名称": "Palantir"}]
        bad = premarket.revenue_claim_issues(["Apple 财报超预期但营收不及预期，盘前下跌。", "SHOP 营收不及预期；PLTR 营收超预期"], rows)
        self.assertEqual([i.split(" ")[0] for i in bad], ["AAPL", "SHOP", "PLTR"])
        self.assertEqual(premarket.revenue_claim_issues(["Apple 营收高于预期，毛利率指引偏低", "SHOP 盘前下跌"], rows), [])

    def test_eps_claims_follow_the_surprise_and_name_the_company(self):
        rows = [{"代码": "INTC", "名称": "Intel Corp", "EPS 超预期幅度": "+30.1%"},
                {"代码": "AMZN", "名称": "Amazon.com Inc", "EPS 超预期幅度": "-5.5%"}]
        bad = premarket.eps_claim_issues(["亚马逊和英特尔财报不及预期，导致纳指期货大跌。", "英特尔（INTC）财报不及预期"], rows)
        self.assertTrue(bad[0].startswith("说财报超预期"))
        self.assertTrue(bad[1].startswith("INTC"))
        self.assertEqual(premarket.eps_claim_issues(["亚马逊（AMZN）财报不及预期；英特尔（INTC）财报超预期但指引偏弱"], rows), [])



class HoldingTests(unittest.TestCase):
    TRADE = {"ticker": "MU", "name": "Micron", "entry_price": 900.0, "shares": 25, "stop": 786.58, "last_close": 760.0,
             "plan": {"policy": "trail_20", "stop": 786.58, "stop_basis": "close"}}

    def test_record_at_the_premarket_price(self):
        q = {"last": 741.0, "change": -0.025, "stale": False}
        h = premarket.holding_record({**self.TRADE, "plan": {}}, q, {"beta": 1.6, "vol_63": 0.0318 * 252 ** 0.5}, -0.005)
        row = h["row"]
        self.assertEqual(row["盘前价"], "741.00")
        self.assertEqual(row["盘前拆分"], "按 β 1.60 × 标普期货 -0.5% ≈ -0.8%，其余 -1.7% 是它自己的")
        self.assertEqual(row["日常波动"], "±3.2%（按 63 日波动）")
        self.assertIn("距盘前价 +6.2%", row["止损"])
        self.assertEqual(row["买入理由"], hn.NO_REASON)
        self.assertTrue(h["below"])
        self.assertAlmostEqual(h["m"]["pnl"], (741 - 900) * 25)

    def test_no_quote_keeps_the_close(self):
        h = premarket.holding_record({**self.TRADE, "plan": {}}, None, {}, None)
        self.assertEqual(h["row"]["盘前"], "暂无成交")
        self.assertNotIn("盘前拆分", h["row"])
        self.assertFalse(h["below"])

    def test_key_items_order(self):
        base = {"m": {"to_stop": -0.12}, "price": 380.0, "chg": -0.077, "sd": 0.021, "stop": 334.3, "below": False,
                "row": {"盘前拆分": "x"}}
        hs = [{**base, "ticker": "MU", "step": "⚠️ 回落止损已触发：按计划开盘卖出全部 25 股", "m": {}, "chg": None},
              {**base, "ticker": "HUM", "step": "", "reported": PACK["positions"][0] | {"时间": "今日盘前", "EPS 实际": "-1.10",
                                                                                  "代码": "HUM"}},
              {**base, "ticker": "PBF", "step": "", "chg": -0.06, "sd": 0.025,
               "preview": {"days": 0, "options": {"move": 0.08}, "gaps": {"max_down": -0.11}}}]
        es = {"change": -0.02, "sigmas": 1.9}
        keys = premarket.key_items(hs, es, PACK["economic_today"], [{"代码": "COP", "身份": "持仓同板块（PBF）", "盘前较前收": "-6.0%"}])
        self.assertEqual(len(keys), 5)
        self.assertTrue(keys[0].startswith("MU：回落止损已触发"))
        self.assertTrue(keys[1].startswith("你的持仓 HUM 今日盘前发布财报：EPS -1.10"))
        self.assertIn("止损在 -12.0%", keys[1])
        self.assertEqual(keys[2], "今天的高重要性数据：北京时间 20:30 非农就业人数（预期 120K），已公布。已公布的结果已经反映在盘前价格里")
        self.assertEqual(keys[3], "PBF 今天发财报，期权定价 ±8.0%，过去最大跌幅 -11.0%，止损在 -12.0%")
        self.assertEqual(keys[4], "标普期货 -2.0%，约 1.9 倍日常波动")
        self.assertEqual(premarket.key_items([], None, [], []), [])

    def test_near_stop_and_unreported_numbers(self):
        h = {"ticker": "SHOP", "m": {"to_stop": -0.071}, "price": 143.75, "chg": None, "sd": 0.038, "stop": 133.51, "below": False,
             "step": "", "row": {}}
        self.assertEqual(premarket.key_items([h], None, [], []),
                         ["SHOP -7.1% 离止损的距离不到 2 倍日常波动，平常的波动就可能碰到（止损按收盘判断）"])
        e = {"代码": "HUM", "时间": "今日盘前", "EPS 实际": "数据待更新", "EPS 预期": "-1.00", "盘前较前收": "-3.0%"}
        self.assertEqual(premarket.earn_summary(e), "HUM 今日盘前 已发布，结果数据还没更新（EPS 预期 -1.00）；盘前 -3.0%")


class RenderTests(unittest.TestCase):
    def test_render_has_every_section(self):
        n = notes()
        text = premarket.render_text(PACK, {"headline": n.headline, "overnight": n.overnight, "watch": [w.model_dump() for w in n.watch]},
                                     {"HUM": n.holdings[0].model_dump()})
        self.assertTrue(text.startswith("【盘前简报 · 2026-10-27 周二】期货小幅走高"))
        self.assertLess(text.index("今天开盘前最重要的"), text.index("一、我的持仓"))
        self.assertLess(text.index("一、我的持仓"), text.index("二、隔夜市场"))
        self.assertIn("• HUM 盘前 380.00（-7.7%） · 持仓 -8.4%（-1,745 美元） · 止损 334.30（距盘前价 -12.0%）", text)
        self.assertIn("💬 财报 EPS 低于预期", text)
        self.assertIn("📈 按 β 0.60 × 标普期货 -0.5%", text)
        self.assertIn("（日常波动 ±2.1%）", text)
        self.assertIn("📰 🔴 Humana 下调 2027 年展望（利空：展望下调影响盈利预期）", text)
        self.assertIn("标普期货的变化约 0.3 倍", text)
        self.assertIn("· 20:30 / 08:30 [高] 非农就业人数 预期 120K 前值 95K（已公布）", text)
        self.assertIn("· 今天待公布财报：GOOGL 盘后（持仓同板块（MU））", text)
        self.assertIn("· MSFT 昨日盘后 EPS 3.10 vs 预期 2.90（+6.9%）；盘前 +3.2%，财报前期权定价 ±5.0%（到 10-31 到期）", text)
        self.assertIn("· NVDA 盘前 102.00（+1.0%）在入场区间内", text)
        self.assertIn("VST -9.4%（约 3.1 倍）", text)
        self.assertIn("💬 NVDA：在入场区间内", text)
        self.assertIn("术语：β", text)

    def test_rule_version_without_notes(self):
        text = premarket.render_text(PACK, {"headline": premarket.fallback_headline(PACK), "overnight": "", "watch": []}, {})
        self.assertIn("】标普期货 +0.3%", text)
        self.assertIn("📰 🔴 Humana cuts 2027 outlook", text)

    def test_card_renders(self):
        from scout.web.views import premarket_card

        rec = {"source": "llm", "model": "m", "brief": {"headline": "h"}, "pack": PACK, "content": "line 1\n一、我的持仓\n• HUM"}
        html = premarket_card(rec, current=True)
        self.assertIn("盘前简报 · 2026-10-27", html)
        self.assertIn("一、我的持仓<br>• HUM", html)
        self.assertIn("<details", premarket_card(rec, current=False))


class PremarketWindowTests(unittest.TestCase):
    def test_one_of_the_two_utc_firings_runs_in_each_season(self):
        from datetime import datetime, timezone

        from scout.premarket import due

        utc = timezone.utc
        self.assertFalse(due(datetime(2026, 11, 4, 13, 0, tzinfo=utc)))     # winter: 8:00 New York
        self.assertTrue(due(datetime(2026, 11, 4, 14, 0, tzinfo=utc)))      # winter: 9:00
        self.assertTrue(due(datetime(2026, 7, 1, 13, 0, tzinfo=utc)))       # summer: 9:00
        self.assertFalse(due(datetime(2026, 7, 1, 14, 0, tzinfo=utc)))      # summer: 10:00
        self.assertTrue(due(datetime(2026, 11, 4, 14, 20, tzinfo=utc)))     # a late start still runs


if __name__ == "__main__":
    unittest.main()
