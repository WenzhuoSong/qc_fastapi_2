import unittest
from datetime import date

from scout import holding_notes as hn

ASOF = date(2026, 11, 5)


def item(tk, price, stop, pnl_pct=-0.1, pnl=-1000, remaining=25, **kw):
    m = {"price": price, "remaining": remaining, "pnl": pnl, "pnl_pct": pnl_pct, "to_stop": stop / price - 1}
    return {"ticker": tk, "m": m, "stop": stop, "step": kw.pop("step", ""), "core": False, "trade": kw.pop("trade", {"ticker": tk}),
            "news": kw.pop("news", []), **kw}


class PreviewTests(unittest.TestCase):
    def test_options_move_takes_first_expiry_after_report(self):
        snap = {"quality": "ok", "expiries": {
            "front": {"expiry": "2026-11-06", "straddle_move": 0.05},          # before the report: excluded
            "m1": {"expiry": "2026-11-13", "straddle_move": 0.0908},
            "m2": {"expiry": "2027-01-15", "straddle_move": 0.174}}}
        self.assertEqual(hn.options_move(snap, date(2026, 11, 7)), {"expiry": "2026-11-13", "move": 0.0908})
        self.assertIsNone(hn.options_move({"quality": "thin"}, ASOF))
        self.assertIsNone(hn.options_move(None, ASOF))

    def test_preview_lines(self):
        p = {"date": "2026-11-06", "days": 1, "consensus": {"source": "Yahoo", "eps": -1.00467, "revenue": 40_889_933_520},
             "options": {"expiry": "2026-11-13", "move": 0.0908},
             "gaps": {"n": 19, "big": 3, "big_up": 0, "big_down": 3, "median_abs": 0.051, "max_up": 0.091, "max_down": -0.113},
             "to_stop": -0.188}
        text = "\n".join(hn.preview_lines(p))
        self.assertIn("财报 2026-11-06（1 天后）前瞻", text)
        self.assertIn("每股收益 -1.00 美元，营收 408.9 亿美元（Yahoo", text)
        self.assertIn("到 11-13 到期，市场定价的涨跌约 ±9.1%（包含财报这一次）", text)
        self.assertIn("过去 19 次财报前后两日：3 次涨跌超过 10%", text)
        self.assertIn("期权定价和过去最大跌幅都没到", text)
        p["to_stop"] = -0.08
        self.assertIn("已经够得着它", "\n".join(hn.preview_lines(p)))
        p.update(consensus=None, options=None, gaps=None)
        self.assertIn("没有报告前的分析师预期数据", "\n".join(hn.preview_lines(p)))


class RiskAndKeyTests(unittest.TestCase):
    def test_risk_lines(self):
        r = {"invested": 100_000, "account": 250_000, "beta_usd": 150_000,
             "vol": {"sessions": 63, "month_usd": 9_000, "month_pct_account": 0.036,
                     "contrib": [{"ticker": "MU", "share": 0.41}, {"ticker": "SHOP", "share": 0.22}]},
             "sectors": [("信息技术", 0.4), ("大盘指数", 0.35)]}
        text = "\n".join(hn.risk_lines(r))
        self.assertIn("投入 100,000（账户 40%）", text)
        self.assertIn("相当于 150,000 的大盘仓位，SPY 跌 5% 时持仓约 -7,500（账户 -3.0%）", text)
        self.assertIn("一个月的正常波动约 ±9,000（账户 ±3.6%）；风险来源：MU 41%、SHOP 22%", text)
        self.assertIn("按市值：信息技术 40%、大盘指数 35%", text)
        self.assertEqual(hn.risk_lines(None), [])

    def test_key_items_order_and_cap(self):
        items = [item("MU", 656.39, 786.58, pnl_pct=-0.281, pnl=-6406),
                 item("PBF", 68.37, 62.40, sigma21=0.188),
                 item("SHOP", 143.75, 133.51, sigma21=0.158),
                 item("HUM", 411.79, 334.30, preview={"date": "2026-11-06", "days": 1, "options": {"expiry": "2026-11-13", "move": 0.0908},
                                                      "gaps": {"max_down": -0.113}})]
        md = {"up": False, "ret": -0.044, "history": {"since": "1993-01-29", "rank": 36},
              "holdings": {"pnl": -9491, "market": -7588, "pct": -0.038}}
        sat = {"over": True, "pct": 0.378, "cap_pct": 0.25, "used": 93_535, "cap": 61_862}
        keys = hn.key_items({"earnings": {}}, items, md, {"off": False}, sat, ASOF)
        self.assertEqual(len(keys), 5)
        self.assertTrue(keys[0].startswith("MU 触发回落止损：收盘 656.39 低于 786.58，按你的计划下一个交易日开盘卖出全部 25 股"))
        self.assertIn("大跌日：SPY -4.4%，1993 年以来第 36 大的单日跌幅；持仓合计 -9,491（账户 -3.8%），其中约 80% 是跟着大盘", keys[1])
        self.assertIn("HUM 11-06 发财报（1 天后），期权定价 ±9.1%，过去最大跌幅 -11.3%，止损在 -18.8%", keys[2])
        self.assertIn("PBF -8.7%、SHOP -7.1% 离止损的距离在 1 个月正常波动之内", keys[3])
        self.assertIn("卫星仓占账户 37.8%，超过你定的 25% 上限（超出约 31,673）", keys[4])
        self.assertEqual(hn.key_items({"earnings": {}}, [], None, None, None), [])

    def test_key_items_earnings_fallback_without_preview(self):
        keys = hn.key_items({"earnings": {"PBF": date(2026, 11, 7)}}, [item("PBF", 70, 56)], None, None, None, ASOF)
        self.assertEqual(keys, ["PBF 11-07 发财报（2 天后），止损在 -20.0%"])


def note(tk="HUM", status="持仓小幅亏损，财报在明天，止损离得很远。", news=None):
    return hn.HoldingNote(ticker=tk, status=status, news=news or [])


PACK = {"holdings": [
    {"代码": "HUM", "持仓盈亏": "-0.8%（-159 美元）", "止损": "334.30（距现价 -18.8%）", "计划提示": "按计划持有", "买入理由": "强势回调中的买入机会",
     "news": [{"index": 0, "标题": "Can Higher Premiums Help Humana Beat Q3 Earnings Estimates?", "摘要": "premiums up"}]},
    {"代码": "MU", "持仓盈亏": "-28.1%（-6,406 美元）", "计划提示": "⚠️ 回落止损触发：建议卖出余下 25 股", "买入理由": hn.NO_REASON,
     "news": []}]}


def good():
    return hn.Notes(holdings=[
        note(news=[hn.NewsNote(index=0, zh="文章讨论保费上调能否帮助 Humana 第三季度盈利超预期", impact="利好", vs_reason="无关",
                               why="保费上调是收入端的正面因素")]),
        note("MU", "跌破回落止损，按你的计划下一个交易日开盘卖出全部 25 股，持仓 -28.1%。")])


class ValidateTests(unittest.TestCase):
    def test_good_notes_pass(self):
        self.assertEqual(hn.validate(good(), PACK), [])

    def test_numbers_forecasts_actions_and_reason(self):
        n = good()
        n.holdings[0].status = "财报后有望反弹，可以考虑加仓，目标 450。"
        n.holdings[0].news[0].vs_reason = "没有记录理由"
        issues = "\n".join(hn.validate(n, PACK))
        self.assertIn("证据包里没有的数字：450", issues)
        self.assertIn("不要预测涨跌：有望反弹", issues)
        self.assertIn("加仓", issues)
        self.assertIn("HUM 有买入理由", issues)

    def test_double_negatives(self):
        n = good()
        n.holdings[1].status = "目前亏损 -28.1%，回落止损已触发，按你的计划下一个交易日开盘卖出全部 25 股。"
        self.assertIn("双重否定：亏损 -28.1%", "\n".join(hn.validate(n, PACK)))
        n.holdings[1].status = "目前亏损 28.1%，持仓 -28.1%，按你的计划下一个交易日开盘卖出全部 25 股。"
        self.assertEqual(hn.validate(n, PACK), [])

    def test_sell_only_where_the_plan_says_so(self):
        n = good()
        n.holdings[0].status = "建议卖出一半锁定收益。"
        self.assertIn("卖出", "\n".join(hn.validate(n, PACK)))
        self.assertEqual(hn.validate(good(), PACK), [])        # MU's plan says sell: allowed

    def test_missing_reason_and_indexes(self):
        n = good()
        n.holdings[1].news = [hn.NewsNote(index=3, zh="x", impact="利空", vs_reason="削弱", why="y")]
        issues = "\n".join(hn.validate(n, PACK))
        self.assertIn("MU 的新闻序号应为 []", issues)
        self.assertIn("MU 没有记录买入理由", issues)
        n.holdings = n.holdings[:1]
        self.assertIn("持仓不一致", "\n".join(hn.validate(n, PACK)))

    def test_accepted_keeps_the_holdings_that_pass(self):
        n = good()
        n.holdings[1].status = "将反弹到 900。"
        self.assertEqual(sorted(hn.accepted(n, PACK)), ["HUM"])

    def test_news_lines(self):
        nn = good().holdings[0].model_dump()
        self.assertEqual(hn.news_lines(nn, []), ["📰 🟢 文章讨论保费上调能否帮助 Humana 第三季度盈利超预期（利好：保费上调是收入端的正面因素）"])
        nn["news"][0]["vs_reason"] = "削弱"
        self.assertIn("（利好｜削弱你的买入理由：", hn.news_lines(nn, [])[0])
        raw = [{"headline": "Micron falls", "sentiment": "negative"}]
        self.assertEqual(hn.news_lines(None, raw), ["📰 🔴 Micron falls"])


if __name__ == "__main__":
    unittest.main()
