import unittest
from datetime import date

import numpy as np
import pandas as pd

from scout import event_calendar as ec

FED_HTML = """
<h4>2026 FOMC Meetings</h4>
<div class="fomc-meeting__month"><strong>January</strong></div><div class="fomc-meeting__date">27-28</div>
<div class="fomc-meeting__month"><strong>Apr/May</strong></div><div class="fomc-meeting__date">30-1</div>
<div class="fomc-meeting__month"><strong>August</strong></div><div class="fomc-meeting__date">22 (notation vote)</div>
<div class="fomc-meeting__month"><strong>December</strong></div><div class="fomc-meeting__date">8-9*</div>
<h4>2025 FOMC Meetings</h4>
<div class="fomc-meeting__month"><strong>October</strong></div><div class="fomc-meeting__date">28-29</div>
"""


class RuleDateTests(unittest.TestCase):
    def test_election_day_is_tuesday_after_first_monday(self):
        self.assertEqual(ec.election_day(2026), date(2026, 11, 3))
        self.assertEqual(ec.election_day(2024), date(2024, 11, 5))
        self.assertEqual(ec.election_day(2022), date(2022, 11, 8))       # Nov 1 is a Tuesday: not election day

    def test_triple_witching_third_friday(self):
        self.assertEqual(ec.triple_witching(2026, 12), date(2026, 12, 18))
        self.assertEqual(ec.triple_witching(2026, 3), date(2026, 3, 20))

    def test_rule_events_in_window(self):
        evs = ec.rule_events(date(2026, 11, 1), date(2026, 12, 31))
        kinds = {(e["kind"], e["event_date"]) for e in evs}
        self.assertIn(("election", date(2026, 11, 3)), kinds)
        self.assertIn(("triple_witching", date(2026, 12, 18)), kinds)
        self.assertIn(("holiday", date(2026, 11, 26)), kinds)            # Thanksgiving
        self.assertIn(("holiday", date(2026, 12, 25)), kinds)
        self.assertFalse(any(e["kind"] == "election" for e in ec.rule_events(date(2027, 11, 1), date(2027, 11, 30))))


class SourceTests(unittest.TestCase):
    def test_parse_fomc_takes_decision_day_and_skips_notation_votes(self):
        got = ec.parse_fomc(FED_HTML)
        self.assertEqual([g["date"] for g in got], [date(2025, 10, 29), date(2026, 1, 28), date(2026, 5, 1), date(2026, 12, 9)])
        self.assertTrue(got[-1]["sep"])
        self.assertFalse(got[1]["sep"])

    def test_ff_skips_what_has_its_own_source(self):
        items = [{"date": "2026-11-03", "title": "Congressional Elections", "title_zh": "Congressional Elections", "impact": "高"},
                 {"date": "2026-12-09", "title": "FOMC Statement", "title_zh": "FOMC 声明", "impact": "高"},
                 {"date": "2026-11-06", "title": "Non-Farm Employment Change", "title_zh": "非农就业人数", "impact": "高", "time_et": "08:30"}]
        out = ec.ff_events(items, {(date(2026, 11, 3), "election"), (date(2026, 12, 9), "fomc")})
        self.assertEqual([e["title"] for e in out], ["非农就业人数"])
        self.assertEqual(out[0]["category"], "宏观数据")

    def test_web_events_need_a_cited_source_and_a_date_in_window(self):
        parsed = [{"date": "2026-11-20", "title_zh": "政府拨款到期", "category": "政治与政策", "importance": "高", "why_zh": "x",
                   "sectors": ["Industrials", "Nope"], "tickers": ["lmt"], "url": "https://a.com/x?utm_source=openai"},
                  {"date": "2026-11-21", "title_zh": "编造的会议", "category": "行业与公司", "importance": "中", "why_zh": "x",
                   "sectors": [], "tickers": [], "url": "https://made.up/y"},
                  {"date": "2027-03-01", "title_zh": "太远", "category": "地缘", "importance": "中", "why_zh": "x",
                   "sectors": [], "tickers": [], "url": "https://a.com/x"}]
        out = ec.web_events(parsed, {"https://a.com/x"}, "", date(2026, 11, 4), date(2026, 12, 9))
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["detail"]["sectors"], ["Industrials"])
        self.assertEqual(out[0]["detail"]["tickers"], ["LMT"])

    def test_same_event_across_weekly_searches(self):
        a = {"event_date": date(2026, 11, 20), "title": "美国政府拨款到期"}
        self.assertTrue(ec.same_event(a, {"event_date": date(2026, 11, 20), "title": "政府拨款到期日"}))
        self.assertFalse(ec.same_event(a, {"event_date": date(2026, 11, 21), "title": "美国政府拨款到期"}))


class ReactionTests(unittest.TestCase):
    def test_election_reacts_next_session_and_stats_compare_with_normal_days(self):
        idx = pd.bdate_range("2024-01-01", "2024-12-31")
        rng = np.random.default_rng(3)
        r = rng.normal(0, 0.005, len(idx))
        nxt = idx.get_loc(pd.Timestamp("2024-11-06"))
        r[nxt] = 0.025
        closes = pd.DataFrame({"SPY": 100 * np.cumprod(1 + r)}, index=idx)
        s = ec.reaction_stats([date(2024, 11, 5)], "election", closes)
        self.assertEqual(s["次数"], 1)
        self.assertEqual(s["每次"][0]["日期"], "2024-11-06")
        self.assertAlmostEqual(s["每次"][0]["SPY"], 0.025, places=6)
        self.assertGreater(s["波动倍数"], 3)
        same_day = ec.reaction_stats([date(2024, 11, 5)], "fomc", closes)
        self.assertEqual(same_day["每次"][0]["日期"], "2024-11-05")
        d = ec.describe(s, "election")
        self.assertEqual(d["每次"][0], {"日期": "2024-11-06", "类型": "大选", "SPY": "+2.5%"})
        self.assertEqual(d["SPY 上涨次数"], "1 / 1")


class PreviewTests(unittest.TestCase):
    def pack(self):
        return {"event": {"标题": "美联储利率决议（FOMC）", "为什么重要": "x"}, "history": {"次数": 6, "反应日": "事件当天", "SPY 反应日涨跌幅中位数（不分涨跌）": "1.20%", "SPY 平常一天涨跌幅中位数（不分涨跌）": "0.61%"},
                "holdings": [{"代码": "MU", "板块": "信息技术"}], "web": {"内容": "预计降息 25 个基点"}}

    def preview(self, **over):
        base = dict(what="w", why="y", expectations="据外部报道（未经核实），预计降息 25 个基点", history="过去 6 次，样本少，只能参考",
                    scenarios=[ec.Scenario(case="a", channel="b", sectors="c"), ec.Scenario(case="d", channel="e", sectors="f")],
                    holdings=[ec.HoldingImpact(ticker="MU", channel="没有直接关联，主要随大盘")], watch="z")
        return ec.Preview(**{**base, **over})

    def test_clean_preview_passes(self):
        self.assertEqual(ec.validate_preview(self.preview(), self.pack()), [])

    def test_preview_checks(self):
        issues = "\n".join(ec.validate_preview(self.preview(expectations="市场预计降息 50 个基点", history="过去 6 次",
                                                            scenarios=[ec.Scenario(case="a", channel="b", sectors="c")],
                                                            holdings=[]), self.pack()))
        for part in ("50", "scenarios", "holdings", "据外部报道", "样本少"):
            self.assertIn(part, issues)

    def test_fallback_is_valid(self):
        self.assertEqual(ec.validate_preview(ec.fallback_preview({**self.pack(), "web": None}), {**self.pack(), "web": None}), [])

    def test_upcoming_lines_skip_forex_factory(self):
        rows = [{"event_date": date(2026, 12, 9), "time_et": "14:00", "importance": "高", "title": "美联储利率决议（FOMC）",
                 "category": "美联储", "source": "fed"},
                {"event_date": date(2026, 12, 10), "time_et": "08:30", "importance": "高", "title": "CPI", "category": "宏观数据", "source": "ff"}]
        self.assertEqual(ec.upcoming_lines(rows, date(2026, 12, 8)), ["· 12-09 周三 14:00 [高] 美联储利率决议（FOMC）（美联储）"])


if __name__ == "__main__":
    unittest.main()
