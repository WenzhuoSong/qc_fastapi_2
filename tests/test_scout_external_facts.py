import asyncio
import unittest
from datetime import date
from unittest import mock

from scout import external_facts as xf

PAGE = ("Shopify Inc. reported third-quarter results on Tuesday. Gross merchandise volume rose to $119.1 billion, "
        "short of the $120.96 billion analysts expected, according to LSEG data. Shares fell 16% in early trading.")


class CheckTests(unittest.TestCase):
    def test_exact_quote_survives_markup_and_spacing(self):
        self.assertEqual(xf.check("Gross merchandise volume rose to $119.1 billion, short of the $120.96 billion analysts expected",
                                  PAGE.replace(" ", "  ")), "exact")

    def test_close_needs_words_and_every_number(self):
        para = "Gross merchandise volume rose to $119.1 billion, short of the $120.96 billion that analysts had expected"
        self.assertEqual(xf.check(para, PAGE), "close")
        self.assertIsNone(xf.check("Gross merchandise volume rose to $125.0 billion, short of the $120.96 billion analysts expected",
                                   PAGE))

    def test_short_or_absent_quotes_fail(self):
        self.assertIsNone(xf.check("Shares fell", PAGE))
        self.assertIsNone(xf.check("Shopify raised its full-year revenue guidance well above consensus estimates", PAGE))

    def test_cjk_quotes_compare(self):
        page = "公司表示，第三季度营收同比增长百分之三十二，超出管理层此前的预期区间。"
        self.assertEqual(xf.check("第三季度营收同比增长百分之三十二，超出管理层此前的预期区间", page), "exact")


class PackTests(unittest.TestCase):
    def test_unreachable_facts_lose_their_numbers_and_mismatches_never_appear(self):
        rows = [{"id": 1, "subject": "SHOP", "fact_type": "业绩与指引", "claim": "GMV 1191 亿美元，低于预期 1209.6 亿", "value": 1191.0,
                 "unit": "亿美元", "quote": "q", "outlet": "Reuters", "url": "u", "published": date(2026, 11, 3), "status": "verified"},
                {"id": 2, "subject": "SHOP", "fact_type": "目标价", "claim": "某券商把目标价下调到 150 美元", "value": 150.0, "unit": "美元",
                 "quote": "q", "outlet": "Bloomberg", "url": "u2", "published": None, "status": "unreachable"},
                {"id": 3, "subject": "SHOP", "fact_type": "其他", "claim": "编的 999", "value": None, "unit": None,
                 "quote": "q", "outlet": "x", "url": "u3", "published": None, "status": "mismatch"}]
        p = xf.pack_rows(rows)
        self.assertEqual([r["id"] for r in p["已核对"]], [1])
        weak = p["未核对（来源打不开，数字已隐去，不可引用数字）"]
        self.assertEqual([r["id"] for r in weak], [2])
        self.assertNotIn("150", str(weak))
        self.assertNotIn("999", str(p))

    def test_fingerprint_ignores_quote_formatting(self):
        a = {"subject": "SHOP", "url": "https://x/y", "quote": "Shares fell 16% in early trading."}
        self.assertEqual(xf.fingerprint(a), xf.fingerprint({**a, "quote": "shares  fell 16%  in early trading"}))

    def test_clean_url_drops_tracking(self):
        self.assertEqual(xf.clean_url("https://a.com/x?utm_source=openai"), "https://a.com/x")


class StoreTests(unittest.TestCase):
    def test_store_commits_its_insert(self):
        fact = {"subject": "SHOP", "fact_type": "其他", "claim": "c", "value": None, "unit": None, "quote": "q" * 30,
                "url": "https://r.com/a", "outlet": "o", "published": None, "status": "verified", "match": "exact"}
        with mock.patch.object(xf.db, "execute_returning", mock.AsyncMock(return_value={"id": 1, **fact})) as w, \
             mock.patch.object(xf.db, "fetch_one", mock.AsyncMock(side_effect=AssertionError("fetch_one rolls inserts back"))):
            rows = asyncio.run(xf.store([fact], "test"))
        self.assertEqual(rows[0]["id"], 1)
        w.assert_awaited_once()


class ExtractTests(unittest.TestCase):
    def test_keeps_cited_quoted_facts_only(self):
        from scout import responses

        facts = {"facts": [
            {"subject": "shop", "fact_type": "业绩与指引", "claim_zh": "GMV 不及预期", "value": 119.1, "unit": "十亿美元",
             "quote": "Gross merchandise volume rose to $119.1 billion, short of the $120.96 billion", "url": "https://r.com/a?utm_source=openai",
             "outlet": "Reuters", "published": "2026-11-03"},
            {"subject": "SHOP", "fact_type": "其他", "claim_zh": "未引用来源", "value": None, "unit": None,
             "quote": "this sentence comes from a page nobody cited at all", "url": "https://made.up/b", "outlet": "x", "published": ""},
            {"subject": "SHOP", "fact_type": "其他", "claim_zh": "太短", "value": None, "unit": None, "quote": "fell", "url": "https://r.com/a",
             "outlet": "x", "published": "2027-01-01"}]}

        async def fake(body, timeout=None, purpose="other"):
            import json
            return {"output": [{"type": "message", "content": [{"type": "output_text", "text": json.dumps(facts)}]}]}

        found = {"text": "answer", "sources": [{"title": "t", "url": "https://r.com/a"}]}
        with mock.patch.object(responses, "create", fake):
            out = asyncio.run(xf.extract(found, ["SHOP"], date(2026, 11, 4), "test"))
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["subject"], "SHOP")
        self.assertEqual(out[0]["url"], "https://r.com/a")
        self.assertEqual(out[0]["published"], date(2026, 11, 3))


if __name__ == "__main__":
    unittest.main()
