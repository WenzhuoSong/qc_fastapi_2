import unittest
from datetime import date

from scout import prediction as pm
from scout.data import prediction as feed


class FeedParseTests(unittest.TestCase):
    def test_polymarket_mid_and_last(self):
        e = {"id": 1, "slug": "fed-decision-in-december-2026", "title": "Fed decision in December?", "endDate": "2026-12-10T04:59:00Z",
             "volume": 12560129, "volume24hr": 300000, "markets": [
                 {"id": 10, "groupItemTitle": "25 bps increase", "outcomePrices": '["0.825", "0.175"]', "bestBid": 0.82, "bestAsk": 0.83,
                  "volume": "9000000", "oneDayPriceChange": 0.01, "description": "rules"},
                 {"id": 11, "groupItemTitle": "No change", "outcomePrices": '["0.175", "0.825"]', "volume": "3000000"},
                 {"id": 12, "groupItemTitle": "old", "closed": True, "outcomePrices": '["1", "0"]'}]}
        ev = feed.polymarket_event(e)
        self.assertEqual([o["label"] for o in ev["outcomes"]], ["25 bps increase", "No change"])
        self.assertAlmostEqual(ev["outcomes"][0]["prob"], 0.825)
        self.assertAlmostEqual(ev["outcomes"][1]["prob"], 0.175)          # no book: the last price
        self.assertEqual(ev["end_date"], date(2026, 12, 10))
        self.assertIsNone(feed.polymarket_event({**e, "closed": True}))

    def test_kalshi_event(self):
        markets = [{"ticker": "KXFEDDECISION-26DEC-H25", "yes_sub_title": "Hike 25bps", "yes_bid_dollars": "0.80", "yes_ask_dollars": "0.81",
                    "last_price_dollars": "0.81", "volume_fp": "712754.69", "volume_24h_fp": "33004", "close_time": "2026-12-09T18:59:00Z",
                    "status": "active", "rules_primary": "If the Federal Reserve does a Hike of 25bps ..."},
                   {"ticker": "KXFEDDECISION-26DEC-H0", "yes_sub_title": "Fed maintains rate", "yes_bid_dollars": "0.17",
                    "yes_ask_dollars": "0.19", "volume_fp": "500000", "close_time": "2026-12-09T18:59:00Z", "status": "active"}]
        ev = feed.kalshi_event({"event_ticker": "KXFEDDECISION-26DEC", "title": "Fed decision in Dec 2026?"}, markets)
        self.assertEqual(ev["end_date"], date(2026, 12, 9))
        self.assertAlmostEqual(ev["outcomes"][0]["prob"], 0.805)
        self.assertAlmostEqual(ev["volume"], 1212754.69)
        self.assertTrue(pm.liquid(ev))

    def test_decided_events_are_not_followed(self):
        ev = {"volume": 75_000_000, "outcomes": [{"prob": 0.9995}, {"prob": 0.0005}]}
        self.assertFalse(pm.liquid(ev))
        self.assertTrue(pm.liquid({**ev, "outcomes": [{"prob": 0.6}]}))

    def test_mid_ignores_empty_books(self):
        self.assertEqual(feed.mid(0.0, 0.0, 0.4), 0.4)
        self.assertEqual(feed.mid(None, 0.2, 0.4), 0.4)


class TriggerTests(unittest.TestCase):
    def test_move_rule(self):
        self.assertAlmostEqual(pm.move(0.30, 0.50), 0.20)                 # ≥ 15 pts
        self.assertAlmostEqual(pm.move(0.46, 0.53), 0.07)                 # crossed 50 % by ≥ 5 pts
        self.assertIsNone(pm.move(0.48, 0.51))                            # crossed by 3 pts: noise
        self.assertIsNone(pm.move(0.70, 0.80))
        self.assertIsNone(pm.move(None, 0.8))

    def test_shown_keeps_the_likely_outcomes(self):
        outs = [{"label": str(i), "prob": p} for i, p in enumerate([0.01, 0.82, 0.17, 0.05, 0.04])]
        self.assertEqual([o["label"] for o in pm.shown(outs)], ["1", "2", "3"])

    def test_shown_drops_settled_and_reads_ladders_near_half(self):
        settled = [{"label": "November 1", "prob": 1.0}, {"label": "December 31, 2027", "prob": 0.52}, {"label": "March 31", "prob": 0.22}]
        self.assertEqual([o["label"] for o in pm.shown(settled)], ["December 31, 2027", "March 31"])
        ladder = [{"label": f"Above {x}", "prob": p} for x, p in ((0, 0.91), (10, 0.87), (50, 0.61), (70, 0.46), (90, 0.33), (100, 0.30))]
        self.assertEqual([o["label"] for o in pm.shown(ladder)], ["Above 50", "Above 70", "Above 90"])
        unlikely = [{"label": "Yes", "prob": 0.02}]
        self.assertEqual(pm.shown(unlikely), unlikely)


class TextTests(unittest.TestCase):
    def test_event_text_and_pack(self):
        e = {"title": "12 月美联储议息（预测市场）", "source": "polymarket", "volume": 12_560_129,
             "outcomes": [{"label": "加息 25 个基点", "prob": 0.825, "before": 0.73}, {"label": "不变", "prob": 0.175, "before": None}]}
        self.assertEqual(pm.event_text(e), "12 月美联储议息（预测市场）（Polymarket，成交 12.6 百万）：加息 25 个基点 82%（一周前 73%）；不变 18%")
        row = pm.pack_rows([e])[0]
        self.assertEqual(row["各结果概率"][0], {"结果": "加息 25 个基点", "现在": "82%", "一周前": "73%"})

    def test_move_line(self):
        m = {"title": "美国今年衰退", "label": "是", "prob": 0.35, "before": 0.18, "delta": 0.17, "tickers": [],
             "cause": [{"claim": "非农大幅低于预期", "source": "Reuters 2026-11-06"}]}
        self.assertIn("「是」18% → 35%（+17 个百分点）· 非农大幅低于预期（Reuters 2026-11-06）", pm.move_line(m))


if __name__ == "__main__":
    unittest.main()
