import unittest
from datetime import date, timedelta

import numpy as np

from scout import event_cause, events, fundamentals
from scout.event_cause import EventCause


def bars(n=120, crash_at=None, crash=-0.20, gap=-0.01, vol_mult=4.0, seed=1):
    rng = np.random.default_rng(seed)
    out, spy, close, s = [], [], 100.0, 400.0
    d = date(2026, 3, 2)
    for i in range(n):
        while d.weekday() >= 5:
            d += timedelta(days=1)
        m = rng.normal(0, 0.008)
        r = 0.4 * m + rng.normal(0, 0.015)
        o = close * (1 + rng.normal(0, 0.002))
        v = 1e6
        if i == crash_at:
            o, r, v = close * (1 + gap), crash, 1e6 * vol_mult
        close = close * (1 + r)
        s = s * (1 + m)
        out.append({"date": d, "open": o, "high": max(o, close) * 1.01, "low": min(o, close) * 0.99, "close": close, "volume": v})
        spy.append({"date": d, "open": s, "high": s, "low": s, "close": s, "volume": 1e8})
        d += timedelta(days=1)
    return out, spy


class DetectTests(unittest.TestCase):
    def test_finds_the_crash_with_timing_and_sigma(self):
        b, spy = bars(crash_at=110)
        evs = events.detect(events.frame(b, spy))
        crash = next(e for e in evs if e["date"] == b[110]["date"].isoformat())
        self.assertAlmostEqual(crash["ret"], -0.20, places=6)
        self.assertLess(crash["sigma_mult"], -5)
        self.assertEqual(crash["timing"], "盘中")
        self.assertAlmostEqual(crash["vol_ratio"], 4.0, places=1)
        self.assertEqual(crash["sessions_ago"], 9)

    def test_quiet_series_has_no_events(self):
        b, spy = bars()
        self.assertEqual([e for e in events.detect(events.frame(b, spy)) if abs(e["ret"]) >= 0.08], [])

    def test_rules(self):
        self.assertTrue(events.is_event(-0.09, -0.09, None, None))                 # big raw move
        self.assertTrue(events.is_event(-0.05, -0.05, 0.015, 1.0))                 # 3.3σ and ≥ 4%
        self.assertFalse(events.is_event(-0.03, -0.03, 0.005, 1.0))                # 6σ but under 4%
        self.assertTrue(events.is_event(0.035, 0.035, 0.02, 3.5))                  # volume shock
        self.assertEqual(events.timing(-0.12, -0.01), "隔夜跳空")
        self.assertEqual(events.timing(-0.01, -0.19), "盘中")
        self.assertEqual(events.timing(-0.06, -0.06), "隔夜与盘中")

    def test_base_rate_picks_most_specific_bucket(self):
        rates = {"跌|盘中|200 日线上方": {"n": 40}, "跌|盘中|全部": {"n": 3000}, "跌|全部|全部": {"n": 6000}}
        ev = {"ret": -0.2, "timing": "盘中"}
        self.assertEqual(events.base_rate_for(rates, ev, True)[0], "跌|盘中|200 日线上方")
        self.assertEqual(events.base_rate_for(rates, ev, False)[0], "跌|盘中|全部")
        self.assertEqual(events.base_rate_for({"跌|全部|全部": {"n": 10}}, ev, None), None)


class FundamentalsTests(unittest.TestCase):
    def test_metrics_are_converted_to_fractions(self):
        f = fundamentals.parse_metrics({"marketCapitalization": 9461.8, "peTTM": 171.5, "revenueGrowthTTMYoy": 16.02,
                                        "grossMarginTTM": 98.3, "totalDebt/totalEquityQuarterly": 0})
        self.assertAlmostEqual(f["market_cap"], 9.4618e9)
        self.assertAlmostEqual(f["rev_growth_ttm"], 0.1602)
        self.assertAlmostEqual(f["gross_margin"], 0.983)
        self.assertEqual(f["debt_equity"], 0)

    def test_recommendations_and_earnings(self):
        rec = [{"period": "2026-10-01", "strongBuy": 3, "buy": 7, "hold": 1, "sell": 0, "strongSell": 1},
               {"period": "2026-09-01", "strongBuy": 3, "buy": 7, "hold": 1, "sell": 0, "strongSell": 0},
               {"period": "2026-08-01", "strongBuy": 3, "buy": 8, "hold": 1, "sell": 0, "strongSell": 0},
               {"period": "2026-07-01", "strongBuy": 4, "buy": 8, "hold": 0, "sell": 0, "strongSell": 0}]
        r = fundamentals.parse_recommendations(rec)
        self.assertEqual(r["analyst_count"], 12)
        self.assertAlmostEqual(r["analyst_buy_share"], 10 / 12)
        self.assertAlmostEqual(r["analyst_buy_change"], 10 / 12 - 1.0)
        e = fundamentals.parse_earnings([
            {"period": "2026-06-30", "actual": 0.36, "estimate": 0.0051}, {"period": "2026-03-31", "actual": -0.3, "estimate": -0.0986},
            {"period": "2025-12-31", "actual": 0.2, "estimate": 0.306}, {"period": "2025-09-30", "actual": 0.16, "estimate": 0.1377}])
        self.assertEqual((e["eps_beat_count"], e["eps_quarters"]), (2, 4))
        self.assertNotIn("eps_surprise_last", e)          # estimate ≈ 0: no meaningful surprise

    def test_insiders_count_open_market_only(self):
        items = [{"name": "A", "change": -1000, "transactionPrice": 100, "transactionCode": "S", "transactionDate": "2026-10-10"},
                 {"name": "B", "change": 500, "transactionPrice": 90, "transactionCode": "P", "transactionDate": "2026-10-12"},
                 {"name": "C", "change": 9000, "transactionPrice": 0, "transactionCode": "A", "transactionDate": "2026-10-12"},
                 {"name": "D", "change": -50, "transactionPrice": 100, "transactionCode": "S", "transactionDate": "2026-06-01"}]
        r = fundamentals.parse_insiders(items, date(2026, 7, 29))
        self.assertEqual(r, {"insider_net_value_90d": 45000 - 100000, "insider_buyers_90d": 1, "insider_sellers_90d": 1})


class CauseValidationTests(unittest.TestCase):
    PACK = {"move": {"key": "market", "当日涨跌": "-20.1%"}, "news": [{"key": "news_1", "标题": "x", "摘要": ""}],
            "filings": [], "insiders": {"key": "insiders"}, "earnings": [],
            "web": {"key": "web", "内容": "Genmab Phase 3 succeeded", "来源": []}}
    EV = {"ret": -0.201, "sector_ret": -0.011, "spy_ret": 0.002}

    def cause(self, **kw):
        base = dict(cause_type="竞争对手/同业传导", summary="竞品三期数据成功，股价下跌 20.1%。", related_company="Genmab",
                    business_impact="Lifyorli 卵巢癌适应症的竞争格局", confidence="high", sources=["web"])
        return EventCause(**{**base, **kw})

    def test_passes(self):
        self.assertEqual(event_cause.validate(self.cause(), self.PACK, self.EV), [])

    def test_needs_sources_and_real_numbers(self):
        issues = "\n".join(event_cause.validate(self.cause(sources=["market"], summary="下跌 35%"), self.PACK, self.EV))
        self.assertIn("必须引用至少一条", issues)
        self.assertIn("35", issues)

    def test_sector_must_explain_the_move(self):
        issues = event_cause.validate(self.cause(cause_type="大盘/板块"), self.PACK, self.EV)
        self.assertTrue(any("不能归为「大盘/板块」" in i for i in issues))

    def test_fallback_without_evidence(self):
        pack = {**self.PACK, "news": [], "web": None}
        self.assertEqual(event_cause.fallback(pack, self.EV).cause_type, "无明确消息")


class InsightEventTests(unittest.TestCase):
    def test_recent_material_event_must_be_explained(self):
        from scout import knowledge
        from scout.insight import Insight, LensView, validate

        lenses = [{"id": l.id, "title": l.title, "method": "", "evidence": []} for l in knowledge.lenses()]
        ev = {"key": "event_1", "日期": "2026-10-23", "距今交易日": 1, "当日涨跌": "-20.1%", "标准差倍数": "-4.6σ",
              "个股自身": "-19.0%", "发生时段": "盘中", "量比": "2.7 倍", "原因类型": "竞争对手/同业传导", "原因": "x"}
        pack = {"lenses": lenses, "context": {"events": [ev], "driver": "个股自身", "scout_view": {"结论": "", "要点": {}}}}
        lv = [LensView(lens=l["id"], verdict="不适用", points=["-"], evidence=[]) for l in lenses]
        base = dict(headline="h", driver="个股自身", attribution="a", lenses=lv, watch=["w"], invalidation=["i"],
                    key_risk="r", confidence="low", confidence_reason="c")
        issues = "\n".join(validate(Insight(**base), pack, "neutral"))
        self.assertIn("异常事件视角必须引用 event_1", issues)
        self.assertIn("recent_event 必须解释它", issues)
        lv_ok = [LensView(lens=l["id"], verdict="偏空", points=["10-23 下跌 -20.1%"], evidence=["event_1"]) if l["id"] == "events"
                 else LensView(lens=l["id"], verdict="不适用", points=["-"], evidence=[]) for l in lenses]
        ok = Insight(**{**base, "lenses": lv_ok, "recent_event": "10-23 因竞品数据下跌 -20.1%"})
        self.assertFalse([i for i in validate(ok, pack, "neutral") if "event" in i or "recent_event" in i])

    def test_old_events_do_not_fill_recent_event(self):
        from scout import knowledge
        from scout.insight import Insight, LensView, complete_lenses

        lenses = [{"id": l.id, "title": l.title, "method": "", "evidence": []} for l in knowledge.lenses()]
        old = {"key": "event_1", "日期": "2026-08-20", "距今交易日": 46, "当日涨跌": "-9.2%", "标准差倍数": "-6.6σ"}
        pack = {"lenses": lenses, "context": {"events": [old]}}
        ins = Insight(headline="h", recent_event="8月20日下跌，未找到原因", driver="不适用", attribution="a",
                      lenses=[LensView(lens="trend_dow", verdict="中性", points=["x"], evidence=[])], watch=["w"],
                      invalidation=["i"], key_risk="r", confidence="low", confidence_reason="c")
        out = complete_lenses(ins, pack)
        self.assertIsNone(out.recent_event)
        self.assertEqual(len(out.lenses), len(lenses))       # lenses without evidence filled as 不适用


if __name__ == "__main__":
    unittest.main()
