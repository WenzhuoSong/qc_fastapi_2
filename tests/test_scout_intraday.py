import json
import unittest
from datetime import date, datetime
from unittest import mock
from zoneinfo import ZoneInfo

from scout import intraday, market_filter

OPP = {"id": "o1", "ticker": "VLO", "status": "new", "asof": date(2026, 10, 23), "primary_detector": "breakout_retest",
       "entry_lo": 410.75, "entry_hi": 423.54, "stop": 383.88, "current_stop": None, "target1": 467.05}
TRADE = {"id": 1, "ticker": "PBF", "name": "PBF Energy", "entry_price": 75.76, "shares": 300, "stop": 70.0, "last_close": 78.0,
         "sold_shares": 0, "sold_value": 0, "closed_on": None, "plan": {"policy": "trail20", "stop": 60.6}}
QUOTE = {"last": 74.1, "change": -0.05, "time_et": "11-05 10:15", "stale": False}
FEAT = {"vol_63": 0.40, "beta": 0.8}
ET = ZoneInfo("America/New_York")


class SessionCountTests(unittest.TestCase):
    def test_counts_trading_days_only(self):
        self.assertEqual(intraday.sessions_since(date(2026, 10, 23), date(2026, 10, 26)), 1)   # Fri → Mon
        self.assertEqual(intraday.sessions_since(date(2026, 10, 23), date(2026, 10, 28)), 3)


class OpportunityEventTests(unittest.TestCase):
    def test_entry_zone_and_void_inside_the_window_only(self):
        today = date(2026, 10, 27)
        self.assertEqual(intraday.opportunity_events(OPP, 415.0, today, 3, live=True)[0][0], "opp_zone")
        self.assertEqual(intraday.opportunity_events(OPP, 415.0, today, 3, live=False), [])
        self.assertEqual(intraday.opportunity_events(OPP, 415.0, date(2026, 10, 30), 3, live=True), [])   # window over
        self.assertEqual(intraday.opportunity_events(OPP, 380.0, today, 3, live=True)[0][0], "opp_void")
        self.assertEqual(intraday.opportunity_events(OPP, 440.0, today, 3, live=True), [])

    def test_triggered_cards_are_left_to_the_close(self):
        o = {**OPP, "status": "triggered", "current_stop": 400.0}
        self.assertEqual(intraday.opportunity_events(o, 399.0, date(2026, 10, 27), 3, True), [])


class ThresholdTests(unittest.TestCase):
    def test_hold_bar_and_levels(self):
        sd = 0.66 / 252 ** 0.5                       # MU: 1.5 sigmas is above 5 %, so the bar is 5 %
        self.assertAlmostEqual(intraday.hold_bar(sd), 0.05)
        self.assertAlmostEqual(intraday.hold_bar(0.005), 0.02)          # never below 2 %
        self.assertEqual(intraday.level(-0.141, 0.05, 3), 2)
        self.assertEqual(intraday.level(-0.30, 0.05, 3), 3)
        self.assertEqual(intraday.level(0.049, 0.05, 3), 0)
        self.assertEqual(intraday.level(-0.10, 0.05, 3), 2)             # exactly two bars

    def test_market_bar_is_the_big_day_bar(self):
        self.assertAlmostEqual(intraday.market_bar(0.008), 0.016)       # 2 sigmas below 2.5 %
        self.assertAlmostEqual(intraday.market_bar(0.02), 0.025)
        self.assertAlmostEqual(intraday.market_bar(None), 0.025)

    def test_every_crossed_level_is_claimed_and_the_top_fresh_one_reported(self):
        rows = intraday._levels("hold_move:MU:2026-11-05", -0.11, 2, {}, "MU")
        self.assertEqual([r["key"] for r in rows], ["hold_move:MU:2026-11-05:down1", "hold_move:MU:2026-11-05:down2"])
        self.assertEqual(intraday._top(rows, "hold_move")["MU"]["payload"]["level"], 2)
        self.assertEqual(intraday._top(rows[:1], "hold_move")["MU"]["payload"]["level"], 1)

    def test_verdict(self):
        self.assertEqual(intraday.move_verdict(-0.05, 1.0, -0.01, [-0.04, -0.045]), "行业")
        self.assertEqual(intraday.move_verdict(-0.05, 2.0, -0.02, [0.01]), "大盘")
        self.assertEqual(intraday.move_verdict(-0.05, 1.0, -0.01, [0.0]), "公司自身")
        self.assertEqual(intraday.move_verdict(-0.05, None, -0.04, []), "公司自身")      # no beta, no market verdict


class PositionTests(unittest.TestCase):
    def test_moves_pnl_and_plan_line(self):
        p = intraday.position(TRADE, QUOTE, FEAT)
        self.assertAlmostEqual(p["today"], 300 * 74.1 * -0.05 / 0.95)
        self.assertAlmostEqual(p["pnl"], (74.1 - 75.76) * 300)
        self.assertFalse(p["below"])
        self.assertIn("距止损 70.00 还有 5.5%", p["plan"])
        self.assertIn("盘中不需要操作", p["plan"])

    def test_below_the_stop_says_the_close_decides(self):
        p = intraday.position(TRADE, {**QUOTE, "last": 69.5}, FEAT)
        self.assertTrue(p["below"])
        self.assertIn("收盘仍低于 70.00，下一个交易日开盘卖出全部", p["plan"])

    def test_partial_sells_count(self):
        p = intraday.position({**TRADE, "sold_shares": 100, "sold_value": 8000}, QUOTE, FEAT)
        self.assertAlmostEqual(p["today"], 200 * 74.1 * -0.05 / 0.95)

    def test_core_holding(self):
        p = intraday.position({**TRADE, "ticker": "SPY", "stop": None, "plan": {"policy": "core_filter"}}, QUOTE, FEAT)
        self.assertTrue(p["core"])
        self.assertFalse(p["below"])


class NewsFilterTests(unittest.TestCase):
    def row(self, **kw):
        return {"ticker": "MU", "headline": "Micron freezes Taiwan plant", "llm_summary": "", "sentiment": "negative",
                "is_hard_event": False, **kw}

    def test_holdings_need_a_sentiment_and_no_question(self):
        held, none = {"MU"}, set()
        self.assertEqual(intraday.news_tag(self.row(), held, none, none), "持仓")
        self.assertIsNone(intraday.news_tag(self.row(sentiment="neutral"), held, none, none))
        self.assertIsNone(intraday.news_tag(self.row(headline="Is the sharp drop a buying opportunity?"), held, none, none))
        self.assertEqual(intraday.news_tag(self.row(sentiment="neutral", is_hard_event=True), held, none, none), "持仓")

    def test_watchlist_needs_a_hard_event(self):
        w = {"MU"}
        self.assertIsNone(intraday.news_tag(self.row(), set(), set(), w))
        self.assertEqual(intraday.news_tag(self.row(is_hard_event=True), set(), set(), w), "自选")
        self.assertEqual(intraday.news_tag(self.row(is_hard_event=True), set(), {"MU"}, w), "机会")

    def test_numbers_in_a_translation(self):
        self.assertTrue(intraday.numbers_ok("美光暂停 45 亿美元台湾投资", "Micron pauses $4.5 billion Taiwan bet"))
        self.assertTrue(intraday.numbers_ok("美光股价下跌 11.8%", "Micron Stocks Fall 11.8%"))
        self.assertFalse(intraday.numbers_ok("美光股价下跌 12.8%", "Micron Stocks Fall 11.8%"))
        self.assertTrue(intraday.numbers_ok("美光冻结台湾工厂", "Micron freezes Taiwan plant"))


class TriageTests(unittest.IsolatedAsyncioTestCase):
    def items(self):
        return [{"ticker": "MU", "payload": {"headline": "Micron pauses $4.5 billion Taiwan bet"}},
                {"ticker": "MU", "payload": {"headline": "3 reasons to buy Micron"}}]

    async def test_kinds_and_checked_translations(self):
        out = {"items": [{"id": "0", "kind": "事件", "zh": "美光暂停 45 亿美元台湾投资"},
                         {"id": "1", "kind": "评论", "zh": "买入美光的 4 个理由"}]}
        items = self.items()
        with mock.patch("scout.responses.create", mock.AsyncMock(return_value={})), \
                mock.patch("scout.responses.output_text", return_value=json.dumps(out)):
            self.assertTrue(await intraday.triage(items))
        self.assertEqual(items[0]["payload"]["kind"], "事件")
        self.assertEqual(items[0]["payload"]["zh"], "美光暂停 45 亿美元台湾投资")
        self.assertEqual(items[1]["payload"]["kind"], "评论")
        self.assertNotIn("zh", items[1]["payload"])                    # 4 is not in the original

    async def test_failure_leaves_the_rule_filter(self):
        items = self.items()
        with mock.patch("scout.responses.create", mock.AsyncMock(side_effect=RuntimeError("down"))):
            self.assertFalse(await intraday.triage(items))
        self.assertNotIn("kind", items[0]["payload"])


class FilterBarTests(unittest.TestCase):
    def test_month_end_bar_is_the_mean_of_the_previous_nine_month_ends(self):
        dates, closes = [], []
        for i, m in enumerate(range(1, 11)):            # month ends Jan .. Oct 2026, closes 100 .. 109
            dates.append(date(2026, m, 28 if m == 2 else 27))
            closes.append(100.0 + i)
        dates.append(date(2026, 11, 3))
        closes.append(120.0)
        with mock.patch.object(market_filter, "_month_end", lambda ds, t: ds[t].month != 11):
            bar = market_filter.month_end_bar(dates, closes, date(2026, 11, 5))
        self.assertAlmostEqual(bar, sum(range(101, 110)) / 9)          # Feb .. Oct
        c = bar - 0.01                                                 # a month-end close just below turns the filter off
        self.assertLess(c, (sum(range(101, 110)) + c) / 10)


MARKET = {"spy": -0.042, "spy_price": 734.25, "time_et": "11-05 15:45", "sigmas": 5.0, "level": 2,
          "indices": [["标普 500", -0.042], ["纳指 100", -0.059]], "vix": [28.2, 0.70],
          "least": [["信息技术", "XLK", -0.072]], "lead": [["公用事业", "XLU", -0.008]], "today": -8497.0, "today_pct": -0.034,
          "beta_part": -7000.0, "betas_missing": [], "filter_bar": 729.89, "filter_gap": 0.006}
HOLD = {"ticker": "MU", "price": 683.4, "change": -0.141, "time_et": "11-05 15:45", "sigmas": 3.4, "level": 2, "today": -2796.0,
        "pnl": -5731.0, "pnl_pct": -0.251, "stop": 786.58, "below": True, "plan": "止损按收盘判断：收盘仍低于 786.58，下一个交易日开盘卖出全部",
        "verdict": "行业", "peers": {"SNDK": -0.185}, "peer_source": "同行业", "etf": "XLK", "etf_change": -0.072, "live": [],
        "spy": -0.042, "beta": 1.6, "news": [{"headline": "Micron <freezes> plant", "zh": "美光冻结台湾工厂", "source": "Yahoo", "flags": []}]}


class RenderTests(unittest.TestCase):
    def test_sections_in_order_and_escaped(self):
        out = {"market": MARKET, "holdings": [HOLD],
               "buys": [{"kind": "opp_zone", "ticker": "CRWD", "payload": {"rule": "突破回踩", "text": "进入入场区间 234.83–245.30",
                                                                           "price": 240.0, "time_et": "11-05 10:00", "size": "建议 20 股"}}],
               "other_news": [{"ticker": "MRVL", "tag": "自选", "headline": "Marvell <buys> Lumentum", "source": "Yahoo", "flags": ["并购/出售"]}]}
        text = intraday.render(out, datetime(2026, 11, 5, 20, 47, tzinfo=ZoneInfo("UTC")))
        self.assertTrue(text.startswith("⏱ <b>盘中提醒</b> 15:47 美东"))
        self.assertLess(text.index("【大盘】"), text.index("【我的持仓】"))
        self.assertLess(text.index("【我的持仓】"), text.index("【机会与自选】"))
        for part in ("标普 500 -4.2%（734.25，15:45，约 5.0 倍日常波动，跌幅继续扩大）", "VIX 28.2（+70%）",
                     "跌得最多：信息技术 XLK -7.2%", "你的持仓今天 −$8,497（账户 -3.4%）", "收在 729.89 以下才转为避险，现在高出它 0.6%",
                     "🔻 <b>MU</b> -14.1%", "跌幅继续扩大；今天 −$2,796", "⚠️ 现价已低于系统止损 786.58", "SPY -4.2% × β 1.60 ≈ -6.7%",
                     "主要是整个行业在动", "📰 美光冻结台湾工厂（Yahoo）", "👉 止损按收盘判断", "🟦 <b>CRWD</b> 突破回踩",
                     "📐 建议 20 股", "<b>MRVL</b> [自选] ⚡并购/出售 Marvell &lt;buys&gt; Lumentum"):
            self.assertIn(part, text)

    def test_nothing_to_say(self):
        self.assertIsNone(intraday.render({"market": None, "holdings": [], "buys": [], "other_news": []}, datetime.now(ET)))

    def test_news_only_holding_has_no_comparison_line(self):
        h = {k: v for k, v in HOLD.items() if k not in ("verdict", "peers", "etf", "etf_change", "live", "spy")}
        lines = intraday.holding_lines({**h, "level": 0, "below": False})
        self.assertFalse(any("主要是" in x for x in lines))
        self.assertTrue(lines[-1].startswith("  👉"))


if __name__ == "__main__":
    unittest.main()
