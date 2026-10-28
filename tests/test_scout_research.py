import asyncio
import unittest
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

from scout import research

Q = [date(2025, 3, 31), date(2025, 6, 30), date(2025, 9, 30), date(2025, 12, 31),
     date(2026, 3, 31), date(2026, 6, 30)]


def series(**kw):
    return {k: list(zip(Q[-len(v):], v)) for k, v in kw.items()}


class FundamentalTrendTests(unittest.TestCase):
    def test_growth_margins_cash_and_dilution(self):
        s = series(revenue=[100, 110, 120, 130, 120, 140], gross_profit=[60, 66, 72, 78, 74, 91],
                   operating_income=[10, 12, 14, 16, 15, 28], operating_cash_flow=[15, 16, 18, 20, 18, 30],
                   capex=[5, 5, 5, 5, 5, 5], shares_diluted=[100, 100, 100, 100, 99, 98], cash=[50] * 6,
                   long_term_debt=[20] * 6)
        v, table = research.fundamental_trend(s)
        self.assertAlmostEqual(v["fin_rev_yoy"], 140 / 110 - 1)
        self.assertAlmostEqual(v["fin_rev_accel"], (140 / 110 - 1) - (120 / 100 - 1))
        self.assertAlmostEqual(v["fin_gross_margin_chg"], 0.65 - 0.60)
        self.assertAlmostEqual(v["fin_op_margin"], 0.2)
        self.assertAlmostEqual(v["fin_fcf_ttm"], 13 + 15 + 13 + 25)          # last four quarters
        self.assertAlmostEqual(v["fin_net_cash"], 30)
        self.assertAlmostEqual(v["fin_share_change"], 0.98 - 1)
        self.assertEqual(len(table), 6)

    def test_annual_filer_in_foreign_currency(self):
        s = {"revenue": [(date(2024, 12, 31), 100.0), (date(2025, 12, 31), 130.0)]}
        v, _ = research.fundamental_trend(s, unit="TWD")
        self.assertAlmostEqual(v["fin_rev_yoy"], 0.3)
        self.assertIn("年度", v["fin_period"])
        self.assertNotIn("fin_fcf_ttm", v)


class ValuationTests(unittest.TestCase):
    def test_pe_ps_and_history(self):
        s = series(revenue=[100, 110, 120, 130, 120, 140], eps_diluted=[1, 1, 1, 1, 1, 1],
                   shares_diluted=[10] * 6)
        trend, _ = research.fundamental_trend(s)
        closes = pd.Series({d: 100.0 + i * 10 for i, d in enumerate(Q)})
        v = research.valuation(150.0, s, trend, "USD", {"forwardPE": 30}, closes,
                               {"val_fwd_pe": [20, 25, 40], "val_ev_sales": []})
        self.assertAlmostEqual(v["val_pe_ttm"], 150 / 4)
        self.assertAlmostEqual(v["val_ps_ttm"], 1500 / (120 + 130 + 120 + 140))
        self.assertAlmostEqual(v["val_fwd_pe_vs_sector"], 30 / 25)
        self.assertNotIn("val_ps_pctile", v)                 # fewer than 6 quarters of history

    def test_negative_earnings_have_no_pe(self):
        s = series(revenue=[100] * 4, eps_diluted=[-1, -1, 1, 0.5], shares_diluted=[10] * 4)
        trend, _ = research.fundamental_trend(s)
        self.assertIsNone(research.valuation(10.0, s, trend, "USD", {}, None, {}).get("val_pe_ttm"))


class ExpectationsTests(unittest.TestCase):
    def test_revisions_targets_and_actions(self):
        est = {"eps_trend": {"0y": {"current": 2.12, "30daysAgo": 2.15, "90daysAgo": 0.78}},
               "eps_revisions": {"0y": {"upLast30days": 1, "downLast30days": 0}},
               "earnings_estimate": {"0y": {"avg": 2.12, "numberOfAnalysts": 4}, "+1y": {"avg": 4.2}},
               "price_targets": {"mean": 141.0, "high": 165.0, "low": 50.0}}
        now = datetime(2026, 10, 26, tzinfo=timezone.utc)
        actions = [{"action_at": now - timedelta(days=5), "firm": "A", "action": "down", "pt_action": "Lowers"},
                   {"action_at": now - timedelta(days=60), "firm": "B", "action": "up", "pt_action": "Raises"}]
        v, table = research.expectations(est, 87.12, actions, date(2026, 10, 26), {"analyst_buy_share": 0.83})
        self.assertAlmostEqual(v["est_eps_rev_30d"], 2.12 / 2.15 - 1)
        self.assertAlmostEqual(v["est_target_upside"], 141 / 87.12 - 1)
        self.assertEqual((v["est_downgrades_30d"], v["est_upgrades_30d"], v["est_pt_cuts_30d"]), (1, 0, 1))
        self.assertEqual(v["analyst_buy_share"], 0.83)
        self.assertEqual(table[0]["firm"], "A")


class EarningsOptionsMacroTests(unittest.TestCase):
    def test_earnings_reaction_over_the_report(self):
        idx = [date(2026, 7, d) for d in (28, 29, 30, 31)]
        closes = pd.Series([100.0, 101.0, 110.0, 111.0], index=idx)
        spy = pd.Series([500.0, 500.0, 505.0, 505.0], index=idx)
        r = research.earnings_reactions([date(2026, 7, 29)], closes, spy)
        self.assertAlmostEqual(r[0]["move"], 110 / 100 - 1)
        self.assertAlmostEqual(r[0]["excess"], 0.10 - 0.01)

    def test_implied_vs_history_needs_earnings_before_expiry(self):
        reactions = [{"move": m} for m in (0.10, -0.06, 0.08)]
        opt = {"opt_move_front": 0.12, "_front_expiry": "2026-11-20"}
        b = research.earnings_block(None, {"date": "2026-11-04"}, reactions, date(2026, 10, 27), opt)
        self.assertAlmostEqual(b["earn_implied_vs_hist"], 0.12 / 0.08)
        b2 = research.earnings_block(None, {"date": "2026-12-04"}, reactions, date(2026, 10, 27), opt)
        self.assertNotIn("earn_implied_vs_hist", b2)

    def test_options_block(self):
        snap = {"quality": "ok", "put_call_volume": 0.6, "unusual": [{}] * 2,
                "expiries": {"front": {"atm_iv": 0.6, "straddle_move": 0.1, "expiry": "2026-11-06"},
                             "m1": {"atm_iv": 0.5, "put_skew": 0.08}, "m2": {"atm_iv": 0.45}}}
        b = research.options_block(snap, 0.4, [0.3] * 70)
        self.assertAlmostEqual(b["opt_iv_rv_ratio"], 1.25)
        self.assertAlmostEqual(b["opt_term_slope"], -0.15)
        self.assertEqual(b["opt_iv_rank"], 1.0)
        self.assertEqual(research.options_block({**snap, "quality": "thin"}, 0.4, []), {})

    def test_macro_sensitivity_recovers_rate_exposure(self):
        rng = np.random.default_rng(0)
        idx = pd.bdate_range("2025-01-01", periods=300).date
        spy_r = rng.normal(0, 0.01, 300)
        d10 = rng.normal(0, 0.05, 300)                  # daily change in the 10y yield, percentage points
        stock_r = 1.0 * spy_r - 0.2 * d10 + rng.normal(0, 0.005, 300)
        spy = pd.Series(100 * np.cumprod(1 + spy_r), index=idx)
        stock = pd.Series(50 * np.cumprod(1 + stock_r), index=idx)
        rate = pd.Series(4 + np.cumsum(d10), index=idx)
        m = research.macro_sensitivity(stock, spy, {"DGS10": rate})
        self.assertAlmostEqual(m["macro_rate_10bp"], -0.02, places=2)
        self.assertLess(m["macro_rate_t"], -10)


class TwoStepTests(unittest.TestCase):
    def test_groups_cover_every_lens_once(self):
        from scout import knowledge
        from scout.insight import GROUPS

        ids = [i for g in GROUPS.values() for i in g]
        self.assertEqual(sorted(ids), sorted(l.id for l in knowledge.lenses()))
        self.assertEqual(len(ids), len(set(ids)))

    def test_group_without_data_needs_no_llm(self):
        from scout import knowledge
        from scout.insight import run_group

        lenses = [{"id": l.id, "title": l.title, "method": "", "evidence": []} for l in knowledge.lenses()]
        pack = {"ticker": "XLE", "name": "", "type": "ETF", "data_date": "2026-10-26", "lenses": lenses, "context": {}}
        views, source, hist = asyncio.run(run_group(None, "m", "基本面与预期", pack, "neutral"))
        self.assertEqual(source, "no_data")
        self.assertEqual({v.verdict for v in views}, {"不适用"})
        self.assertEqual(len(views), 4)


if __name__ == "__main__":
    unittest.main()


class ValidationLeniencyTests(unittest.TestCase):
    def pack(self):
        return {"lenses": [{"id": "ownership", "title": "", "method": "", "evidence": [
                    {"字段": "insider_buyers_90d", "数值": "0"}, {"字段": "insider_net_value_90d", "数值": "-$53.9M"}]}],
                "context": {"macro_now": {"10 年期美债": {"最新": "4.12"}}, "scout_view": {"趋势": "x"}}}

    def test_context_citations_resolve(self):
        from scout.insight import LensView, resolve_citations

        v = LensView(lens="ownership", verdict="中性", points=["x"],
                     evidence=["context.macro_now", "macro_now.10 年期美债", "10 年期美债", "scout_view.趋势", "bogus"])
        self.assertEqual(resolve_citations([v], self.pack())[0].evidence, ["macro_now", "scout_view", "bogus"])

    def test_dates_are_not_numbers(self):
        from scout.thesis import unsupported_numbers

        self.assertEqual(unsupported_numbers("2026年10月23日下跌，10-23 之后、10/30 前，2026 年", []), [])
        self.assertEqual(unsupported_numbers("下跌 20.1%", []), ["20.1"])

    def test_insider_selling_is_not_lost_confidence(self):
        from scout.insight import LensView, insider_overreach

        bad = LensView(lens="ownership", verdict="偏空", points=["内部人净卖出 -$53.9M，显示信心不足"], evidence=["insider_net_value_90d"])
        ok = LensView(lens="ownership", verdict="中性", points=["内部人 90 日净卖出 -$53.9M"], evidence=["insider_net_value_90d"])
        self.assertTrue(insider_overreach([bad], self.pack()))
        self.assertEqual(insider_overreach([ok], self.pack()), [])

    def test_amounts_in_chinese_units_match_the_evidence(self):
        from scout.thesis import allowed_numbers, unsupported_numbers

        allowed = allowed_numbers({"v": ["$4.5B", "$480.5B", "-$53.9M", "-$12.3B"], "成交量": 12000})
        text = "自由现金流 45 亿美元，市值约 4805 亿（0.48 万亿），内部人净卖出 5390 万美元，净负债 123 亿，成交 1.2 万手"
        self.assertEqual(unsupported_numbers(text, allowed), [])

    def test_amounts_in_chinese_units_are_still_checked(self):
        from scout.thesis import allowed_numbers, unsupported_numbers

        allowed = allowed_numbers({"v": ["$4.5B", "-$53.9M"]})
        self.assertEqual(unsupported_numbers("营收 900 亿，市值 5 万亿，回购 3 亿", allowed), ["900亿", "5万亿", "3亿"])
        self.assertEqual(unsupported_numbers("自由现金流率 4.2", allowed), ["4.2"])

    def test_lens_prompt_states_the_insider_rule_and_no_arithmetic(self):
        from scout.insight import _CONFIDENCE_CLAIMS, LENS_PROMPT

        for claim in _CONFIDENCE_CLAIMS:
            self.assertIn(claim, LENS_PROMPT)
        self.assertIn("不要改写成", LENS_PROMPT)
        self.assertIn("不算差值", LENS_PROMPT)
