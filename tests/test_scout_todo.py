import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

from scout import todo

TODAY = date(2026, 11, 25)


def book(key, name, grade, school="价值投资"):
    return {"key": key, "name": name, "school": school, "status": {"text": {"有依据": "前向验证中", "未检验": "未检验"}.get(grade, "否定"),
                                                                  "grade": grade}}


class StateTests(unittest.TestCase):
    def test_done_hides_for_good_and_snooze_until_its_day(self):
        self.assertTrue(todo.visible(None, TODAY))
        self.assertFalse(todo.visible({"status": "done"}, TODAY))
        self.assertFalse(todo.visible({"status": "snoozed", "snooze_until": date(2026, 11, 26)}, TODAY))
        self.assertTrue(todo.visible({"status": "snoozed", "snooze_until": TODAY}, TODAY))


class RenderTests(unittest.TestCase):
    def test_numbered_items_with_their_buttons(self):
        rows = [{"key": "sell:7:2026-11-19", "text": "PBF：回落止损 <触发>", "buttons": [("✅ 已卖出 PBF", "sd:7"), ("⏸ 明天再提醒", "tz:sell:7:2026-11-19")]},
                {"key": "cand:2026-11-25", "text": "2 个候选等你决定", "buttons": []}]
        text, kb = todo.render(rows, TODAY)
        self.assertIn("1. PBF：回落止损 &lt;触发&gt;", text)
        self.assertIn("2. 2 个候选等你决定", text)
        self.assertEqual(kb["inline_keyboard"], [[{"text": "1. ✅ 已卖出 PBF", "callback_data": "sd:7"},
                                                  {"text": "1. ⏸ 明天再提醒", "callback_data": "tz:sell:7:2026-11-19"}]])

    def test_nothing_to_do(self):
        text, kb = todo.render([], TODAY)
        self.assertIn("没有需要你处理的事", text)
        self.assertIsNone(kb)

    def test_callbacks_fit_telegram(self):
        self.assertLessEqual(max(len(b["callback_data"]) for b in todo.keyboard(10 ** 12)["inline_keyboard"][0]), 64)


class ItemTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_sell_due_comes_first_and_a_handled_one_is_gone(self):
        trade = {"id": 7, "ticker": "PBF", "opened_on": date(2026, 10, 1), "entry_price": 75.0, "shares": 300, "stop": 62.11,
                 "plan": {"policy": "trail20", "stop_hit": "2026-11-19"}}
        sat = {"used": 40000.0, "pct": 0.4, "cap_pct": 0.25, "cap": 25000.0, "room": 0.0, "over": True}

        async def fetch_all(sql, params=None):
            return []

        with patch("scout.portfolio.trades", AsyncMock(return_value=[trade])), \
             patch("scout.portfolio.metrics", return_value={"remaining": 300, "price": 61.0}), \
             patch("scout.telegram.next_step", return_value="⚠️ 回落止损触发：下一个交易日开盘卖出全部 300 股"), \
             patch("scout.position_risk.adherence", return_value={"signal": {"sessions": 4}}), \
             patch("scout.portfolio.get_settings", AsyncMock(return_value={})), \
             patch("scout.portfolio.satellite", return_value=sat), \
             patch("scout.market_filter.current", AsyncMock(return_value={"off": False})), \
             patch("scout.db.fetch_all", side_effect=fetch_all):
            rows = await todo.items(TODAY)
            self.assertEqual([r["key"] for r in rows], ["sell:7:2026-11-19", "sat:over"])
            self.assertIn("（已过 4 个交易日）", rows[0]["text"])
            self.assertEqual(rows[0]["buttons"][0][1], "sd:7")

            async def handled(sql, params=None):
                return [{"key": "sell:7:2026-11-19", "status": "done"}] if "todo_state" in sql else []

            with patch("scout.db.fetch_all", side_effect=handled):
                self.assertEqual([r["key"] for r in await todo.items(TODAY)], ["sat:over"])


class CandidateTests(unittest.TestCase):
    def test_consensus_needs_three_books_and_counts_evidence_double(self):
        res = {"playbooks": [book("a", "A", "未检验"), book("b", "B", "否定"), book("c", "C", "有依据"), book("d", "D", "未检验"),
                             book("t", "T", "未检验", school="大盘择时")],
               "by_ticker": {"X": [["a", "买入"], ["b", "买入"], ["c", "买入"]],
                             "Y": [["a", "买入"], ["b", "买入"], ["d", "买入"], ["t", "买入"]],
                             "Z": [["a", "买入"], ["b", "卖出"], ["t", "买入"]]}}
        out = todo.consensus(res)
        self.assertEqual([(c["ticker"], c["score"]) for c in out], [("X", 4), ("Y", 3)])
        self.assertEqual(out[0]["books"][0]["name"], "C")                # the one with evidence first

    def test_stats_line(self):
        line = todo.stats_line({"plan": {"n": 4, "excess": 0.021}, "skip": {"n": 9, "excess": -0.004}})
        self.assertIn("打算买入 4 个，平均相对 SPY +2.1%", line)
        self.assertIn("跳过 9 个，平均相对 SPY -0.4%", line)
        self.assertIsNone(todo.stats_line({}))


class DecideTests(unittest.IsolatedAsyncioTestCase):
    async def test_plan_adds_to_the_watchlist_and_skip_does_not(self):
        with patch("scout.db.fetch_one", AsyncMock(return_value={"ticker": "CART", "payload": {}})), \
             patch("scout.db.execute", AsyncMock()) as ex, patch("scout.universe.add_watch", AsyncMock()) as watch:
            self.assertIn("打算买入", await todo.decide(5, "plan"))
            watch.assert_awaited_once_with("CART")
            self.assertIn('"decision": "plan"', ex.await_args.args[1]["p"])
            watch.reset_mock()
            self.assertIn("已跳过", await todo.decide(5, "skip"))
            watch.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
