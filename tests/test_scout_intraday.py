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
        self.assertIn("盘中低于回落止损 95.00", intraday.trade_events(t, 94.0)[0][1])
        self.assertIn("按收盘判断", intraday.trade_events(t, 94.0)[0][1])
        self.assertEqual(intraday.trade_events(t, 111.0), [])            # holdings have no target to alert on
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


class RenderLabelTests(unittest.TestCase):
    def test_card_alerts_say_they_are_not_holdings(self):
        from datetime import datetime, timezone

        alerts = [{"kind": "opp_stop", "ticker": "RRC", "payload": {"text": "跌破止损 53.05（收盘后按规则结算）", "price": 52.88,
                                                                    "time_et": "09:30", "rule": "放量异动"}},
                  {"kind": "trade_stop", "ticker": "SHOP", "payload": {"text": "盘中低于回落止损 133.51", "price": 130.0, "time_et": "09:31"}}]
        text = intraday.render(alerts, datetime(2026, 11, 4, 14, 31, tzinfo=timezone.utc))
        self.assertIn("RRC</b> 机会卡·放量异动（不是你的持仓）", text)
        self.assertIn("SHOP</b> 你的持仓", text)

    def test_holding_move_line_gives_peers_sector_market_and_verdict(self):
        a = {"kind": "hold_move", "ticker": "PBF", "payload": {
            "change": -0.045, "price": 74.28, "time_et": "09:40", "sigmas": 2.1, "peers": {"VLO": -0.048, "MPC": -0.050},
            "etf": "XLE", "etf_change": -0.029, "live": ["3-2-1 炼油差价 今日 -3.0%"], "spy": 0.017, "beta": -0.14,
            "industry": "炼油", "verdict": "行业"}}
        line = intraday.hold_move_line(a)
        for part in ("[你的持仓] <b>PBF</b> 盘中 -4.5%", "约 2.1 倍日常波动", "同行 VLO -4.8%、MPC -5.0%", "板块 XLE -2.9%",
                     "3-2-1 炼油差价 今日 -3.0%", "大盘 SPY +1.7%", "主要是整个行业在动", "盘中不需要操作"):
            self.assertIn(part, line)


if __name__ == "__main__":
    unittest.main()
