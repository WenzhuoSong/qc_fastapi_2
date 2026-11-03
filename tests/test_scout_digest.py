import unittest
from datetime import date

from scout import digest

ASOF = date(2026, 11, 3)


def trade(tk, entry, last, shares, sector, stop):
    return {"id": 1, "ticker": tk, "entry_price": entry, "last_close": last, "shares": shares, "sector": sector,
            "stop": stop, "plan": {"policy": "trail20"}, "closed_on": None, "sold_shares": 0, "sold_value": 0,
            "opened_on": date(2026, 10, 20)}


def signal(tk, src, impact, conf="high"):
    return {"ticker": tk, "impact": impact, "confidence": conf, "mechanism": f"{src} 削减订单，作为供应商收入可能下降",
            "src": {"ticker": src, "headline": "h"}, "link": {"relation": "customer"}}


def gathered():
    feats = {"MU": {"vol_63": 0.30, "ret_1d": 0.021, "market_ret_1d": 0.004, "sector_ret_1d": 0.006, "own_ret_1d": 0.011, "idio_ret_1d": 0.017},
             "PBF": {"ret_1d": -0.01, "market_ret_1d": 0.004, "idio_ret_1d": -0.014},
             "NVDA": {"ret_1d": 0.05, "market_ret_1d": 0.005, "idio_ret_1d": 0.045, "own_ret_1d": 0.04, "sector_ret_1d": 0.005},
             "KO": {"ret_1d": 0.002, "market_ret_1d": 0.002, "idio_ret_1d": 0.0, "own_ret_1d": 0.0, "sector_ret_1d": 0.0}}
    return {"held_rows": [trade("MU", 100, 120, 100, "Information Technology", 110),
                          trade("PBF", 75, 70, 300, "Energy", 64)],
            "watch": ["NVDA", "KO"], "feats": feats,
            "news": {"MU": [{"headline": "Micron raises guidance", "sentiment": "positive", "is_hard_event": True, "datetime_utc": 1}]},
            "info": {"NVDA": {"name": "NVIDIA"}}, "earnings": {"PBF": date(2026, 11, 6)},
            "prop": [signal("PBF", "VLO", -1), signal("AMKR", "AAPL", -1), signal("SWKS", "AAPL", -1, "low")],
            "managed": {}}


class DigestTests(unittest.TestCase):
    def test_holdings_needing_attention_come_first(self):
        items = digest.holding_items(gathered(), ASOF)
        self.assertEqual([i["ticker"] for i in items], ["PBF", "MU"])       # earnings soon + a negative hint beat a larger position
        self.assertTrue(items[0]["attention"])
        self.assertFalse(items[1]["attention"])

    def test_a_holding_near_its_stop_needs_attention(self):
        g = gathered()
        g["held_rows"][0]["stop"] = 117                  # 2.5 % below 120; MU's daily sigma ~1.9 %
        mu = next(i for i in digest.holding_items(g, ASOF) if i["ticker"] == "MU")
        self.assertTrue(mu["attention"])
        self.assertIn("不到 2 倍日常波动", "\n".join(mu["lines"]))

    def test_watchlist_shows_only_names_that_changed(self):
        self.assertEqual([w["ticker"] for w in digest.watch_items(gathered(), ASOF)], ["NVDA"])

    def test_render_has_every_part_and_routes_hints(self):
        market = {"regime": "uptrend", "spy_ret_1d": 0.004, "spy_ret_20d": 0.02}
        sector_rows = [{"kind": "sector", "sector": "Energy", "name": "能源", "key": "XLE", "ret_1d": -0.01, "rel_20": -0.03,
                        "heat_label": "冷", "quadrant": "lagging"}]
        text = digest.render(ASOF, market, sector_rows, None, None, gathered(), None, ["今日没有规则触发。"])
        for part in ("一、市场", "二、板块", "三、我的持仓", "四、自选股变化", "五、其他"):
            self.assertIn(part, text)
        self.assertIn("持有 PBF", text)
        self.assertIn("财报 2026-11-06（3 天后）", text)
        self.assertIn("AMKR", text)                       # another stock's hint under 其他
        self.assertNotIn("SWKS", text)                    # low confidence is recorded, not pushed
        self.assertLess(text.index("PBF"), text.index("<b>MU</b>"))
        self.assertIn("今日没有规则触发。", text)


if __name__ == "__main__":
    unittest.main()
