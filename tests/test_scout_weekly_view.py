import unittest

from scout import viewdesk
from scout import weekly_view as wv

SECTORS = [{"字段": "XLV.sector", "ETF": "XLV", "板块": "医疗保健", "热度": "热", "20 日相对 SPY": "+13.3%"},
           {"字段": "XLK.sector", "ETF": "XLK", "板块": "信息技术", "热度": "冷", "20 日相对 SPY": "-9.4%"}]
PACK = {"market": [{"字段": "SPY.ret_20d", "名称": "标普 500（SPY）近 20 个交易日", "数值": "-3.2%", "含义": "x"},
                   {"字段": "macro.VIX", "名称": "VIX", "数值": "18.5（20 个交易日变化 +2.10，2026-11-02）", "含义": "x"}],
        "sectors": SECTORS,
        "facts": {"已核对": [{"id": 1, "内容": "美联储 9 月加息 25 个基点"}], "未核对（来源打不开，数字已隐去，不可引用数字）": [{"id": 2, "内容": "…"}]},
        "calendar": [], "portfolio": {}}


def arg(ids=(1,), check="Scout 数据一致"):
    return wv.MarketArgument(claim="c", source_kind="新闻媒体", fact_ids=list(ids), check=check, check_note="SPY 近 20 日 -3.2%")


def week(**over):
    base = dict(week="本周 SPY 近 20 个交易日 -3.2%", drivers=[wv.Driver(driver="利率", explanation="e", fact_ids=[1], fields=["SPY.ret_20d"])],
                bull=[arg()], bear=[arg((2,), "无法用 Scout 数据核对")],
                explained=[viewdesk.Explained(field="SPY.ret_20d", what="w", value="-3.2%", usually="u", for_you="f"),
                           viewdesk.Explained(field="macro.VIX", what="w", value="18.5", usually="u", for_you="f")],
                sectors=[wv.SectorLine(etf="XLV", line="热"), wv.SectorLine(etf="XLK", line="冷")],
                ahead="a", stance="中性", confidence="中", confidence_reason="r", wrong_if="w", for_you="f")
    return wv.MarketWeek(**{**base, **over})


class MarketWeekTests(unittest.TestCase):
    def test_clean_passes(self):
        self.assertEqual(wv.validate(week(), PACK), [])

    def test_checks(self):
        bad = week(week="SPY 跌了 7.5%", bull=[arg((9,))], sectors=[wv.SectorLine(etf="XLV", line="x")],
                   drivers=[wv.Driver(driver="d", explanation="e", fact_ids=[], fields=["nope"])])
        issues = "\n".join(wv.validate(bad, PACK))
        for part in ("7.5", "fact_ids", "nope", "XLK"):
            self.assertIn(part, issues)

    def test_high_confidence_rule(self):
        self.assertTrue(any("「高」" in i for i in wv.validate(week(confidence="高"), PACK)))
        self.assertEqual(wv.validate(week(confidence="高", bull=[arg(), arg()]), PACK), [])

    def test_render_has_three_sections_and_sources(self):
        rep = {"asof": "2026-11-08", "market": week().model_dump(), "pack": PACK,
               "facts": {"1": {"label": "Reuters 2026-11-06", "url": "u", "status": "verified"}},
               "views": [{"ticker": "XLV", "name": "医疗", "kind": "sector",
                          "view": {"stance": "看多", "horizon": 20, "confidence": "中", "summary": "s"}},
                         {"ticker": "SHOP", "name": "Shopify", "kind": "stock",
                          "view": {"stance": "看空", "horizon": 20, "confidence": "中", "summary": "s"}}], "failed": []}
        text = wv.render(rep)
        for part in ("一、市场", "二、板块", "三、个股", "Reuters 2026-11-06", "🔥 XLV", "未来 20 个交易日 SPY"):
            self.assertIn(part, text)


if __name__ == "__main__":
    unittest.main()
