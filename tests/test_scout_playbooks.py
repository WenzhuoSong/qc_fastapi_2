import unittest
from datetime import date, timedelta

import numpy as np
import pandas as pd

import scout.playbooks as pb
from scout.playbooks import allocation as al
from scout.playbooks import market as mk
from scout.playbooks import technical as te
from scout.playbooks import value as va
from scout.playbooks.base import BUY, HOLD, SCHOOLS, SELL, Ctx
from scout.playbooks.data import fundamentals


def _ctx(close: pd.DataFrame, volume: pd.DataFrame | None = None, **kw) -> Ctx:
    volume = volume if volume is not None else close * 0 + 1e6
    return Ctx(asof=close.index[-1].date(), close=close, high=close * 1.01, low=close * 0.99, open=close, volume=volume,
               f=kw.pop("f", {}), stocks=kw.pop("stocks", [c for c in close.columns if c != "SPY"]), etfs=kw.pop("etfs", []),
               sectors={}, **kw)


class RegistryTests(unittest.TestCase):
    def test_keys_are_unique_and_every_playbook_is_complete(self):
        keys = [p.key for p in pb.PLAYBOOKS]
        self.assertEqual(len(keys), len(set(keys)))
        for p in pb.PLAYBOOKS:
            self.assertIn(p.school, SCHOOLS, p.key)
            self.assertTrue(p.name and p.author and p.idea and p.evidence, p.key)
            self.assertTrue(p.fn is not None or p.missing, f"{p.key}: a playbook without a function says what is missing")

    def test_status_prefers_the_rule_then_its_relative(self):
        entries = {"rank_trend_template": "rejected", "breakout_retest_selection": "forward"}
        self.assertEqual(pb.status_of(pb.BY_KEY["minervini"], entries)["grade"], "未检验")
        self.assertEqual(pb.status_of(pb.BY_KEY["high_52w"], {"rank_high_52w": "rejected"})["grade"], "未通过")
        self.assertEqual(pb.status_of(pb.BY_KEY["faber"], {})["grade"], "有依据")

    def test_a_failing_playbook_is_reported_not_raised(self):
        ctx = _ctx(pd.DataFrame({"SPY": [1.0, 2.0], "X": [1.0, 2.0]}, index=pd.bdate_range("2026-01-01", periods=2)))
        with self.assertLogs("scout.playbooks", "WARNING"):
            self.assertIsNone(pb.apply(ctx, pb.BY_KEY["clenow"]))        # no feature frames → an error inside
        self.assertIn("clenow", ctx.cache["errors"])


class TechnicalTests(unittest.TestCase):
    def test_darvas_box_is_the_last_high_followed_by_three_lower_highs(self):
        h = np.r_[np.linspace(10, 20, 260), [21, 20.5, 20.2, 20.4, 20.0]]
        l = h - 1
        top, bottom = te.darvas_box(h, l)
        self.assertEqual(top, 21)
        self.assertAlmostEqual(bottom, 19.2)

    def test_turtle_buys_a_fresh_breakout_and_sells_a_fresh_breakdown(self):
        idx = pd.bdate_range("2025-01-01", periods=80)
        up = pd.Series(np.r_[np.full(77, 10.0), [10.5, 11, 12]], index=idx)
        down = pd.Series(np.r_[np.full(77, 10.0), [9.5, 9, 8]], index=idx)
        ctx = _ctx(pd.DataFrame({"UP": up, "DN": down, "SPY": up}))
        v = te.turtle(ctx, 20, 10)
        self.assertEqual(v["UP"].action, BUY)
        self.assertEqual(v["DN"].action, SELL)

    def test_wilcox_counts_highs_before_the_loaded_window(self):
        idx = pd.bdate_range("2025-01-01", periods=300)
        c = pd.Series(np.linspace(50, 100, 300), index=idx)
        for prior, bought in ((150.0, False), (60.0, True)):
            ctx = _ctx(pd.DataFrame({"X": c, "SPY": c}), prior_high={"X": prior})
            ctx.high = ctx.close
            v = te.wilcox(ctx).get("X")
            self.assertEqual(v is not None and v.action == BUY, bought)


class MarketTests(unittest.TestCase):
    def test_zweig_buys_four_percent_off_the_low_and_sells_four_percent_off_the_high(self):
        w = pd.Series([100, 97, 95, 99.0, 102, 105, 100.5, 99], index=pd.period_range("2026-01", periods=8, freq="W-FRI"))
        st = mk.zweig_state(pd.concat([pd.Series([100.0] * 3, index=pd.period_range("2025-12", periods=3, freq="W-FRI")), w]))
        self.assertEqual(st["state"], "sell")                          # 105 → 100.5 is −4.3 %
        self.assertAlmostEqual(st["high"], 105)

    def test_oneil_follow_through_after_a_correction(self):
        idx = pd.bdate_range("2025-06-01", periods=200)
        px = np.r_[np.linspace(100, 120, 140), np.linspace(120, 105, 20), [105.5, 106, 106.3, 108, 109], np.linspace(109, 115, 35)]
        vol = np.r_[np.full(163, 1e6), [2e6, 1e6], np.full(35, 1e6)]
        c = pd.DataFrame({"SPY": px}, index=idx)
        st = mk.oneil_state(_ctx(c, pd.DataFrame({"SPY": vol}, index=idx)), "SPY")
        self.assertEqual(st["state"], "确认上升")
        self.assertEqual(st["ftd"], idx[163].date())

    def test_oneil_correction_without_follow_through(self):
        idx = pd.bdate_range("2025-06-01", periods=180)
        px = np.r_[np.linspace(100, 120, 150), np.linspace(120, 100, 30)]
        st = mk.oneil_state(_ctx(pd.DataFrame({"SPY": px}, index=idx)), "SPY")
        self.assertEqual(st["state"], "调整中")


class AllocationTests(unittest.TestCase):
    def _months(self, rets: dict[str, float], n: int = 16) -> pd.DataFrame:
        days = pd.bdate_range("2024-01-01", periods=n * 21 + 5)
        return pd.DataFrame({t: 100 * (1 + r) ** np.arange(len(days)) for t, r in rets.items()}, index=days)

    def test_gem_holds_the_stronger_equity_when_stocks_beat_bills(self):
        ctx = _ctx(self._months({"SPY": 0.001, "EFA": 0.002, "AGG": 0.0001, "BIL": 0.0002}))
        v = al.gem(ctx)
        self.assertIn(v["EFA"].action, (BUY, HOLD))
        self.assertNotIn("SPY", [t for t, x in v.items() if x.action in (BUY, HOLD)])

    def test_gem_goes_to_bonds_when_stocks_lag_bills(self):
        v = al.gem(_ctx(self._months({"SPY": -0.001, "EFA": 0.0005, "AGG": 0.0001, "BIL": 0.0002})))
        self.assertIn(v["AGG"].action, (BUY, HOLD))

    def test_vaa_turns_defensive_when_one_offensive_asset_is_negative(self):
        v = al.vaa(_ctx(self._months({"SPY": 0.001, "EFA": 0.001, "EEM": -0.001, "AGG": 0.0002,
                                      "SHY": 0.0001, "IEF": 0.0003, "LQD": 0.0002})))
        held = [t for t, x in v.items() if x.action in (BUY, HOLD)]
        self.assertEqual(held, ["IEF"])

    def test_missing_etfs_are_reported(self):
        ctx = _ctx(self._months({"SPY": 0.001}))
        self.assertEqual(al.gem(ctx), {})
        self.assertEqual(ctx.cache["missing"]["gem"], ["EFA", "AGG", "BIL"])

    def test_13612w_weights_recent_months_most(self):
        m = pd.DataFrame({"A": [100.0] * 12 + [90.0, 100.0]})
        self.assertAlmostEqual(float(al.m13612w(m, -1)["A"]), (12 * (100 / 90 - 1) + 0 + 0 + 0) / 4)


class ValueTests(unittest.TestCase):
    def test_piotroski_counts_the_signals_it_can_see(self):
        f = pd.DataFrame({"net_income": [10, -1], "net_income_prev": [5, 1], "total_assets": [100, 100], "total_assets_prev": [100, 100],
                          "operating_cash_flow": [12, -2], "long_term_debt": [10, 30], "long_term_debt_prev": [20, 20],
                          "gross_profit": [40, 20], "revenue": [100, 100], "gross_profit_prev": [30, 30], "revenue_prev": [90, 120],
                          "shares": [100, 110], "shares_prev": [100, 100]}, index=["GOOD", "BAD"])
        ctx = _ctx(pd.DataFrame({"SPY": [1.0]}, index=pd.bdate_range("2026-01-01", periods=1)), fund=f)
        sc = va.piotroski_score(ctx)
        self.assertEqual(int(sc.at["GOOD", "score"]), 8)
        self.assertEqual(int(sc.at["BAD", "score"]), 0)


class PageTests(unittest.TestCase):
    def _res(self):
        book = {"key": "magic_formula", "name": "神奇公式", "school": "价值投资", "author": "Greenblatt", "idea": "好公司 + 便宜",
                "buy_rule": "b", "sell_rule": "s", "evidence": "ev", "version": "v", "status": {"text": "未检验", "grade": "未检验"},
                "counts": {BUY: 9, HOLD: 0, SELL: 1}, "buys": [{"ticker": f"T{i}", "name": "", "why": f"第 {i}"} for i in range(9)],
                "sells": [{"ticker": "PBF", "name": "", "why": "跌出"}], "mine": {"PBF": {"action": SELL, "why": "跌出"}}}
        missing = {**book, "key": "gem", "name": "双动量", "school": "资产配置", "missing": "缺少数据：EFA"}
        for k in ("counts", "buys", "sells", "mine"):
            missing.pop(k)
        return {"asof": "2026-11-06", "playbooks": [book, missing], "by_ticker": {"T8": [["magic_formula", BUY]]},
                "universe": {"stocks": 10, "etfs": 2}}

    def test_panel_shows_each_playbook_with_its_lists_and_missing_data(self):
        from scout.web import views

        html = views.playbooks_panel(self._res())
        self.assertIn("神奇公式", html)
        self.assertNotIn("你的持仓", html)                              # PBF is in the verdicts but not held
        self.assertIn("你的持仓", views.playbooks_panel(self._res(), held={"PBF"}))
        self.assertIn("其余 1 只", html)
        self.assertIn("缺少数据：EFA", html)
        self.assertNotIn("双动量", views.playbooks_panel(self._res(), "价值投资"))
        self.assertIn("还没有打法简报", views.playbooks_panel(None))

    def test_cards_carry_the_twenty_year_history(self):
        from scout.web import views

        hist = {"_period": ["2006-11-01", "2026-10-28"], "_benchmarks": {"SPY": {"年化": "+11.1%", "最大回撤": "-55.2%"}},
                "magic_formula": {"判断节奏": "annual", "年化": "+11.0%", "最大回撤": "-48.0%", "夏普": 0.5, "前十年年化": "+8.0%",
                                  "后十年年化": "+14.0%", "高成本年化": "+10.8%", "对比": "等权股票池", "每月超额": "+0.10%",
                                  "超额t值": 0.9, "交易笔数": 312, "起始": "2006-12-29",
                                  "同期参照年化": "+9.1%", "同期参照最大回撤": "-59.0%", "同期SPY年化": "+10.9%"},
                "gem": {"没有回测": "数据缺"}}
        html = views.playbooks_panel(self._res(), hist=hist)
        self.assertIn("年化 <b>+11.0%</b>（同期 等权股票池 +9.1%、SPY +10.9%）", html)
        self.assertIn("每年判断", html)
        self.assertIn("二十年回测：没有做，数据缺", html)
        self.assertIn("SPY 年化 +11.1%", html)
        self.assertNotIn("二十年回测", views.playbooks_panel(self._res()))

    def test_ticker_verdicts_come_from_lists_holdings_and_the_long_index(self):
        res = self._res()
        self.assertEqual([(v["book"]["key"], v["action"]) for v in pb.for_ticker(res, "PBF")], [("magic_formula", SELL)])
        self.assertEqual(pb.for_ticker(res, "T8")[0]["why"], "第 8")
        self.assertEqual(pb.for_ticker(res, "NONE"), [])


class FundamentalsTests(unittest.TestCase):
    def test_trailing_and_year_earlier_values_as_filed(self):
        rows = []
        for k in range(8):                                     # 8 quarters of net income 10, 11, …; filed 30 days later
            end = date(2025, 12, 31) - timedelta(days=91 * k)
            rows.append({"ticker": "X", "metric": "net_income", "period_end": end, "period_start": end - timedelta(days=90),
                         "filed": end + timedelta(days=30), "value": 17.0 - k})
            rows.append({"ticker": "X", "metric": "shares_diluted", "period_end": end, "period_start": end - timedelta(days=90),
                         "filed": end + timedelta(days=30), "value": 100.0})
            rows.append({"ticker": "X", "metric": "eps_diluted", "period_end": end, "period_start": end - timedelta(days=90),
                         "filed": end + timedelta(days=30), "value": (17.0 - k) / 100})
        f = fundamentals(rows, pd.Series({"X": 20.0}), {}, date(2026, 2, 15))
        self.assertAlmostEqual(f.at["X", "net_income"], 17 + 16 + 15 + 14)
        self.assertAlmostEqual(f.at["X", "net_income_prev"], 13 + 12 + 11 + 10)
        self.assertAlmostEqual(f.at["X", "mcap"], 2000.0)
        later_filing_unknown = fundamentals(rows, pd.Series({"X": 20.0}), {}, date(2026, 1, 15))
        self.assertAlmostEqual(later_filing_unknown.at["X", "net_income"], 16 + 15 + 14 + 13)


if __name__ == "__main__":
    unittest.main()
