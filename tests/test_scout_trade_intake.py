import asyncio
import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

from scout import trade_intake as ti

BUY = {"side": "buy", "ticker": "PBF", "shares": 300, "all_shares": False, "price": 75.76, "date": None,
       "reason": "财报超预期后回到 72–73 支撑上方", "falsification": "跌破 71.9 说明支撑失效", "falsify_price": 71.9,
       "falsify_below": True, "source": "own", "horizon_sessions": None}


class IntentTests(unittest.TestCase):
    def test_reports_of_trades_are_trades_questions_are_not(self):
        self.assertTrue(ti.is_trade("买了 PBF 300 股 75.76，理由：支撑"))
        self.assertTrue(ti.is_trade("今天卖了 CART 全部 56.81"))
        self.assertTrue(ti.is_trade("清仓 VLO"))
        self.assertTrue(ti.is_trade("完成购买XLU，价格43.28，股数500，理由：公用事业相对强势持续"))
        self.assertTrue(ti.is_trade("已买入 SPY 100 股 756"))
        self.assertFalse(ti.is_trade("准备买XLU，价格43.4，股数500"))
        self.assertFalse(ti.is_trade("卖出信号是什么？"))
        self.assertFalse(ti.is_trade("买了 PBF 300 股，现在该卖吗"))
        self.assertFalse(ti.is_trade("PBF 为什么跌"))
        self.assertFalse(ti.is_trade("买什么好"))


class ProblemTests(unittest.TestCase):
    def test_a_complete_buy_has_no_problems(self):
        self.assertEqual(ti.problems(BUY, 78.0, None), [])

    def test_missing_numbers_are_asked_not_guessed(self):
        self.assertIn("缺少成交价", ti.problems({**BUY, "price": None}, 78.0, None))
        self.assertIn("缺少股数", ti.problems({**BUY, "shares": None}, 78.0, None))

    def test_a_typo_in_the_price_is_caught(self):
        self.assertTrue(any("相差超过" in x for x in ti.problems({**BUY, "price": 7.576}, 78.0, None)))

    def test_sells_need_a_holding_and_enough_shares(self):
        sell = {**BUY, "side": "sell", "shares": 400}
        self.assertTrue(any("没有 PBF 的持仓" in x for x in ti.problems(sell, 78.0, None)))
        self.assertTrue(any("超过持有的 300" in x for x in ti.problems(sell, 78.0, 300)))
        self.assertEqual(ti.problems({**sell, "shares": None, "all_shares": True}, 78.0, 300), [])


class DayAndCardTests(unittest.TestCase):
    def test_a_weekend_moves_to_the_session_before(self):
        self.assertEqual(ti.trade_day({**BUY, "date": None}, date(2026, 11, 1)), date(2026, 10, 30))   # a Sunday
        self.assertEqual(ti.trade_day({**BUY, "date": "2026-10-29"}, date(2026, 11, 1)), date(2026, 10, 29))

    def test_card_shows_the_reasoning_and_flags_a_missing_falsification(self):
        txt = ti.card(BUY, date(2026, 10, 29), ["持仓计划：初始止损 60.61"], [])
        self.assertIn("跌破 71.9 说明支撑失效", txt)
        self.assertIn("自己的判断", txt)
        self.assertIn("Scout 不会下单", txt)
        self.assertIn("建议补上", ti.card({**BUY, "falsification": None}, date(2026, 10, 29), [], []))
        self.assertIn("请重新发送", ti.card(BUY, date(2026, 10, 29), [], ["缺少股数"]))
        self.assertEqual(ti.keyboard(7)["inline_keyboard"][0][0]["callback_data"], "td:ok:7")


class ThesisAlertTests(unittest.TestCase):
    def test_the_users_invalidation_is_reported_once_and_is_not_a_sell(self):
        from scout import portfolio

        bars = [{"date": date(2026, 10, 29), "close": 75.5}, {"date": date(2026, 10, 30), "close": 71.5}]
        t = {"id": 1, "ticker": "PBF", "opened_on": date(2026, 10, 29), "entry_price": 75.76, "shares": 300.0,
             "plan": {"policy": "trail20", "stop": 60.61, "stop_basis": "x", "thesis": {"falsify_price": 71.9, "falsify_below": True,
                                                                                    "falsification": "跌破 71.9"}}}
        with patch.object(portfolio.db, "fetch_all", AsyncMock(return_value=bars)), \
             patch.object(portfolio.db, "execute", AsyncMock()) as saved:
            msgs = asyncio.run(portfolio.manage_trade(t))
        self.assertTrue(any("证伪条件触发" in m and "不是卖出信号" in m for m in msgs))
        plan = saved.await_args.args[1]["p"]
        self.assertIn('"broken_on": "2026-10-30"', plan)


class CallbackTests(unittest.TestCase):
    def test_confirm_button_reaches_decide(self):
        from scout import telegram

        with patch.object(telegram, "_api", AsyncMock(return_value={"ok": True})), \
             patch.object(telegram, "send", AsyncMock()) as send, \
             patch("scout.trade_intake.decide", AsyncMock(return_value="✅ 已记录")) as decide:
            asyncio.run(telegram._handle_callback({"id": "q", "data": "td:ok:12"}))
        decide.assert_awaited_once_with(12, True)
        send.assert_awaited_once_with("✅ 已记录")


if __name__ == "__main__":
    unittest.main()
