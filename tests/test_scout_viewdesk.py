import unittest

from scout import viewdesk as vd

PACK = {"subject": {"代码": "SHOP"}, "debate": {"内容": "Analysts cut targets to 150 after GMV missed 120.96 billion"},
        "evidence": [{"字段": "eps_rev_90d", "名称": "EPS 预期 90 日修正", "含义": "x", "数值": "+3.6%", "读数": "上调"},
                     {"字段": "idio_ret_20d", "名称": "20 日扣除大盘后", "含义": "x", "数值": "-10.9%", "读数": "大幅跑输"},
                     {"字段": "pe_ratio", "名称": "市盈率", "含义": "x", "数值": "106.4", "读数": "很高"}],
        "context": {}}


def arg(check="Scout 数据一致", fields=("eps_rev_90d",), kind="分析师", note="EPS 预期 90 日修正 +3.6%"):
    return vd.Argument(claim="c", source_kind=kind, outlet="o", published="2026-11-03", check=check, check_note=note, fields=list(fields))


def view(**over):
    base = dict(summary="s", stance="中性", horizon=60, bull=[arg()], bear=[arg("Scout 数据一致", ("idio_ret_20d",), "新闻媒体", "20 日扣除大盘后 -10.9%")],
                explained=[vd.Explained(field="pe_ratio", what="市盈率", value="106.4", usually="很贵", for_you="波动大"),
                           vd.Explained(field="eps_rev_90d", what="预期修正", value="+3.6%", usually="上调", for_you="基本面支撑")],
                holds_when="h", wrong_if="w", settle="st", confidence="中", confidence_reason="r")
    return vd.View(**{**base, **over})


class ViewValidationTests(unittest.TestCase):
    def test_clean_view_passes(self):
        self.assertEqual(vd.validate(view(), PACK), [])

    def test_numbers_fields_and_sides_are_checked(self):
        bad = view(bull=[], bear=[arg(fields=("made_up",), note="目标价 75")],
                   explained=[vd.Explained(field="pe_ratio", what="市盈率", value="106.4", usually="u", for_you="f")])
        issues = "\n".join(vd.validate(bad, PACK))
        for part in ("75", "bull 和 bear", "made_up", "2–4"):
            self.assertIn(part, issues)

    def test_high_confidence_needs_two_confirmations_and_no_contradiction(self):
        contradicted = view(confidence="高", bull=[arg(), arg()], bear=[arg("Scout 数据矛盾", ("pe_ratio",), note="市盈率 106.4")])
        self.assertTrue(any("「高」" in i for i in vd.validate(contradicted, PACK)))
        unchecked = view(confidence="高", bull=[arg(), arg()],
                         bear=[arg("无法用 Scout 数据核对", (), note="n") for _ in range(4)])
        self.assertTrue(any("最多写「中」" in i for i in vd.validate(unchecked, PACK)))
        self.assertEqual(vd.validate(view(confidence="高", bull=[arg(), arg()]), PACK), [])

    def test_checked_argument_needs_a_field(self):
        self.assertTrue(any("fields" in i for i in vd.validate(view(bull=[arg(fields=())]), PACK)))

    def test_source_mix_and_text(self):
        v = view()
        self.assertEqual(vd.source_mix(v)["分析师"], 1)
        self.assertEqual(vd.source_mix(v)["新闻媒体"], 1)
        rec = {"ticker": "SHOP", "name": "Shopify", "view": v.model_dump(), "mix": vd.source_mix(v)}
        text = vd.render_text(rec)
        self.assertIn("未来 60 个交易日相对 SPY 中性", text)
        self.assertIn("看空的理由", text)
        self.assertIn("据外部报道（未经核实）", text)

    def test_schema_is_strict(self):
        schema = vd.strict_schema(vd.View)
        self.assertFalse(schema["additionalProperties"])
        self.assertIn("bull", schema["required"])


if __name__ == "__main__":
    unittest.main()
