import unittest
from datetime import date, timedelta

from scout import knowledge, structure
from scout.analysis import DRIVER_MARKET, DRIVER_MIXED, DRIVER_STOCK, attribution
from scout.features import SNAPSHOT_FEATURES
from scout.insight import Insight, LensView, evidence_keys, fallback, validate


class DictionaryTests(unittest.TestCase):
    def test_every_snapshot_feature_is_defined(self):
        spec = knowledge.features()
        for key in SNAPSHOT_FEATURES:
            self.assertIn(key, spec, f"{key} missing from scout/knowledge/features.yaml")
            for field in ("zh", "unit", "definition"):
                self.assertTrue(spec[key].get(field), f"{key}.{field}")

    def test_lens_fields_are_defined_and_lenses_load(self):
        lenses = knowledge.lenses()
        self.assertEqual(len(lenses), 14)
        for lens in lenses:
            self.assertTrue(lens.body and lens.sources, lens.id)
            for f in lens.fields:
                self.assertIn(f, knowledge.features(), f"{lens.id} uses undefined field {f}")

    def test_readings_and_formatting(self):
        self.assertEqual(knowledge.reading("rsi2", 4.8), "极度超卖（≤ 10）")
        self.assertEqual(knowledge.reading("rsi2", 95), "极度超买（≥ 90）")
        self.assertIsNone(knowledge.reading("close", 100.0))           # no bands defined
        self.assertIsNone(knowledge.reading("rsi2", float("nan")))
        self.assertEqual(knowledge.fmt("ret_20d", 0.0523), "+5.2%")
        self.assertEqual(knowledge.fmt("vol_20", 0.293), "29.3%")      # magnitude: no sign
        self.assertEqual(knowledge.fmt("rs_pct", 0.92), "92%")
        self.assertEqual(knowledge.fmt("zscore_20", -2.04), "-2.0σ")
        self.assertEqual(knowledge.fmt("vol_ratio", 1.84), "1.8 倍")

    def test_bands_are_ordered_and_cover_values(self):
        for key, spec in knowledge.features().items():
            for band in spec.get("readings") or []:
                self.assertTrue("min" in band or "max" in band, key)
                if "min" in band and "max" in band:
                    self.assertLessEqual(band["min"], band["max"], key)


def _bars(closes, vol_up=2.0, vol_down=1.0):
    d0 = date(2026, 1, 1)
    out = []
    for i, c in enumerate(closes):
        prev = closes[i - 1] if i else c
        out.append({"date": d0 + timedelta(days=i), "open": prev, "high": c * 1.01, "low": c * 0.99, "close": c,
                    "volume": 1e6 * (vol_up if c >= prev else vol_down)})
    return out


class StructureTests(unittest.TestCase):
    def test_zigzag_uptrend(self):
        closes = []
        level = 100.0
        for _ in range(8):                       # up 12, down 6, repeat: higher highs and higher lows
            closes += [level + i for i in range(12)]
            closes += [level + 11 - i for i in range(6)]
            level += 6
        s = structure.compute(_bars(closes))
        self.assertTrue(s["swing_trend"].startswith("上升"), s["swing_trend"])
        self.assertGreater(s["updown_vol_20"], 1.0)
        self.assertLess(s["dist_support"], 0)

    def test_close_below_last_swing_low_is_flagged(self):
        closes = []
        level = 100.0
        for _ in range(6):
            closes += [level + i for i in range(12)] + [level + 11 - i for i in range(6)]
            level += 6
        closes += [closes[-1] - 2 * i for i in range(1, 15)]     # collapse through the last low
        s = structure.compute(_bars(closes))
        self.assertIn("跌破最近波段低点", s["swing_break"])

    def test_short_history(self):
        self.assertEqual(structure.compute(_bars([100.0] * 10)), {})


class AttributionTests(unittest.TestCase):
    def test_driver_rules(self):
        self.assertEqual(attribution({"idio_ret_20d": -0.368, "market_ret_20d": -0.012})[0], DRIVER_STOCK)
        self.assertEqual(attribution({"idio_ret_20d": 0.005, "market_ret_20d": -0.06})[0], DRIVER_MARKET)
        self.assertEqual(attribution({"idio_ret_20d": -0.035, "market_ret_20d": -0.028})[0], DRIVER_MIXED)
        # older snapshots: market part derived from ret_20d - idio_ret_20d
        self.assertEqual(attribution({"idio_ret_20d": 0.002, "ret_20d": -0.05})[0], DRIVER_MARKET)
        self.assertIsNone(attribution({"ret_20d": 0.01}))


def _pack():
    lenses = [{"id": l.id, "title": l.title, "method": "", "evidence": []} for l in knowledge.lenses()]
    lenses[0]["evidence"] = [knowledge.evidence("rsi14", 38.0).as_dict(), knowledge.evidence("ret_20d", -0.063).as_dict()]
    return {"ticker": "NVDA", "data_date": "2026-10-23", "lenses": lenses,
            "context": {"scout_view": {"结论": "x", "要点": {}}, "driver": "混合", "news": "近 7 天没有可匹配的新闻"}}


def _insight(**over):
    lv = [LensView(lens=l.id, verdict="中性", points=["RSI(14) 为 38"], evidence=["rsi14"]) for l in knowledge.lenses()]
    base = dict(headline="短期整理", driver="混合", attribution="共同作用", lenses=lv, watch=["站上 50 日线"],
                invalidation=["跌破支撑"], key_risk="高 β", confidence="medium", confidence_reason="证据分歧")
    base.update(over)
    return Insight(**base)


class ValidationTests(unittest.TestCase):
    def test_clean_insight_passes(self):
        self.assertEqual(validate(_insight(), _pack(), ""), [])
        self.assertIn("news", evidence_keys(_pack()))

    def test_catches_invented_numbers_unknown_keys_and_driver(self):
        bad = _insight(headline="目标 250 美元", driver="个股自身",
                       lenses=[LensView(lens="trend_dow", verdict="偏多", points=["x"], evidence=["pe_ratio"])])
        issues = "\n".join(validate(bad, _pack(), "down"))
        self.assertIn("250", issues)
        self.assertIn("pe_ratio", issues)
        self.assertIn("必须且只能覆盖", issues)
        self.assertIn("驱动力", issues)
        self.assertIn("趋势方向相反", issues)

    def test_swing_shape_misread_is_caught(self):
        from scout.insight import structure_conflicts

        pack = _pack()
        pack["lenses"][0]["evidence"].append(knowledge.evidence("swing_trend", "下降：高点降低、低点降低").as_dict())
        wrong = _insight(lenses=[LensView(lens=l.id, verdict="中性", points=["高点和低点均抬高"], evidence=["rsi14"])
                                 for l in knowledge.lenses()])
        self.assertTrue(structure_conflicts(wrong, pack))
        self.assertEqual(structure_conflicts(_insight(), pack), [])

    def test_fallback_covers_every_lens(self):
        fb = fallback(_pack())
        self.assertEqual(len(fb.lenses), len(knowledge.lenses()))
        self.assertEqual(fb.confidence, "low")
        self.assertEqual(validate(fb, _pack(), ""), [])


if __name__ == "__main__":
    unittest.main()
