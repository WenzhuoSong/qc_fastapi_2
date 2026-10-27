import unittest
from datetime import date, datetime
from zoneinfo import ZoneInfo

from scout import intraday

OPP = {"id": "o1", "ticker": "VLO", "status": "new", "asof": date(2026, 10, 23), "primary_detector": "pullback_in_uptrend",
       "entry_lo": 410.75, "entry_hi": 423.54, "stop": 383.88, "current_stop": None, "target1": 467.05}


class SessionCountTests(unittest.TestCase):
    def test_counts_trading_days_only(self):
        self.assertEqual(intraday.sessions_since(date(2026, 10, 23), date(2026, 10, 26)), 1)   # Fri → Mon
        self.assertEqual(intraday.sessions_since(date(2026, 10, 23), date(2026, 10, 28)), 3)


class OpportunityEventTests(unittest.TestCase):
    def test_entry_zone_only_for_live_rules_inside_the_window(self):
        today = date(2026, 10, 27)
        self.assertEqual(intraday.opportunity_events(OPP, 415.0, today, 3, live=True)[0][0], "opp_zone")
        self.assertEqual(intraday.opportunity_events(OPP, 415.0, today, 3, live=False), [])
        self.assertEqual(intraday.opportunity_events(OPP, 415.0, date(2026, 10, 30), 3, live=True), [])   # window over
        self.assertEqual(intraday.opportunity_events(OPP, 380.0, today, 3, live=True)[0][0], "opp_void")
        self.assertEqual(intraday.opportunity_events(OPP, 440.0, today, 3, live=True), [])

    def test_triggered_uses_current_stop_and_target(self):
        o = {**OPP, "status": "triggered", "current_stop": 400.0}
        self.assertEqual(intraday.opportunity_events(o, 399.0, date(2026, 10, 27), 3, True)[0][0], "opp_stop")
        self.assertEqual(intraday.opportunity_events(o, 470.0, date(2026, 10, 27), 3, True)[0][0], "opp_target")
        self.assertEqual(intraday.opportunity_events(o, 430.0, date(2026, 10, 27), 3, True), [])


class TradeEventTests(unittest.TestCase):
    def test_stop_and_target(self):
        t = {"entry_price": 100.0, "stop": 95.0, "target": 110.0}
        self.assertIn("跌破持仓止损 95.00", intraday.trade_events(t, 94.0)[0][1])
        self.assertEqual(intraday.trade_events(t, 111.0)[0][0], "trade_target")
        self.assertEqual(intraday.trade_events({**t, "stop": None, "target": None}, 50.0), [])


class RenderTests(unittest.TestCase):
    def test_orders_by_urgency_and_escapes(self):
        alerts = [
            {"kind": "news", "ticker": "CART", "payload": {"tag": "机会", "headline": "Deal <talks>", "source": "Reuters",
                                                         "flags": ["并购/出售"]}},
            {"kind": "trade_stop", "ticker": "VLO", "payload": {"text": "跌破持仓止损 400.00", "price": 398.2, "time_et": "10-27 10:15"}},
        ]
        text = intraday.render(alerts, datetime(2026, 10, 27, 14, 15, tzinfo=ZoneInfo("UTC")))
        self.assertTrue(text.startswith("⏱ <b>盘中提醒</b> 10:15 美东"))
        self.assertLess(text.index("VLO"), text.index("CART"))
        self.assertIn("Deal &lt;talks&gt;", text)
        self.assertIn("⚡并购/出售", text)
        self.assertIsNone(intraday.render([], datetime.now()))

    def test_big_move_shows_cause(self):
        alerts = [{"kind": "big_move", "ticker": "CORT", "payload": {
            "tag": "持仓", "change": -0.201, "price": 85.86, "time_et": "10-23 15:55",
            "cause": {"type": "竞争对手/同业传导", "summary": "Genmab 三期数据成功", "external": True}}}]
        text = intraday.render(alerts, datetime(2026, 10, 23, 19, 55, tzinfo=ZoneInfo("UTC")))
        self.assertIn("⚡ [持仓] <b>CORT</b> 盘中 -20.1%", text)
        self.assertIn("竞争对手/同业传导：Genmab 三期数据成功（据外部报道）", text)


if __name__ == "__main__":
    unittest.main()
