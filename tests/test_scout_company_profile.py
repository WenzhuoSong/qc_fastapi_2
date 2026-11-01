import unittest

from scout import company_profile as cp

BODY = ("We design memory. " * 150) + ("Our largest customer is Nvidia. " * 10)
FILING = ("Table of Contents Item 1. Business 3 Item 1A. Risk Factors 12 Item 2. Properties 30 "
          "PART I Item 1. Business Overview " + BODY + " Item 1A. Risk Factors We face risks. "
          "Sales to Nvidia Corporation accounted for 21% of our total revenue in fiscal 2026. Item 2. Properties ...")


class TextTests(unittest.TestCase):
    def test_the_section_is_the_long_span_not_the_table_of_contents(self):
        s = cp.section(FILING, "10-K")
        self.assertTrue(s.startswith("Item 1. Business Overview"))
        self.assertIn("largest customer is Nvidia", s)
        self.assertNotIn("Risk Factors We face", s)
        self.assertIsNone(cp.section("Item 1. Business short. Item 1A. Risk Factors", "10-K"))

    def test_concentration_sentences_anywhere_in_the_filing(self):
        self.assertEqual(cp.snippets(FILING), ["Sales to Nvidia Corporation accounted for 21% of our total revenue in fiscal 2026."])

    def test_html_to_text(self):
        self.assertEqual(cp.html_text("<html><body><p>Item&nbsp;1.</p><script>x</script><p>Business</p></body></html>"), "Item 1.Business")


class ValidationTests(unittest.TestCase):
    def test_only_quoted_items_survive(self):
        prof = {"summary": "s", "not_disclosed": [], "segments": [], "end_markets": [], "suppliers": [], "competitors": [], "partners": [],
                "advantages": [], "business_risks": [],
                "customers": [
                    {"name": "Nvidia", "detail": "占收入 21%", "quote": "Sales to Nvidia Corporation accounted for 21% of our total revenue"},
                    {"name": "Apple", "detail": None, "quote": "Apple is our second largest customer by revenue"},     # not in the text
                    {"name": "Nvidia", "detail": "占收入 35%", "quote": "Our largest customer is Nvidia."}]}         # number not in quote
        kept, rep = cp.validate(prof, FILING)
        self.assertEqual([c["detail"] for c in kept["customers"]], ["占收入 21%"])
        self.assertEqual(rep["dropped"], 2)
        self.assertEqual({d["reason"][:12] for d in rep["dropped_items"]}, {"quote not in", "numbers not "})


class MatchTests(unittest.TestCase):
    IDX = cp.name_index([{"ticker": "NVDA", "name": "Nvidia Corporation", "kind": "stock"},
                         {"ticker": "MU", "name": "Micron Technology, Inc.", "kind": "stock"},
                         {"ticker": "XLK", "name": "Technology", "kind": "etf"}])

    def test_names_map_to_tickers(self):
        self.assertEqual(cp.match("NVIDIA Corp.", self.IDX), "NVDA")
        self.assertEqual(cp.match("Micron Technology", self.IDX), "MU")
        self.assertEqual(cp.match("Taiwan Semiconductor Manufacturing Company Limited", self.IDX), "TSM")
        self.assertEqual(cp.match("Alphabet Inc.", self.IDX), "GOOGL")
        self.assertIsNone(cp.match("Samsung Electronics", self.IDX))
        self.assertIsNone(cp.match("U.S. Government", self.IDX))

    def test_relations_skip_the_company_itself(self):
        rels = cp.relations("MU", {"customers": [{"name": "Nvidia", "detail": None, "quote": "q"}],
                                   "competitors": [{"name": "Micron Technology", "detail": None, "quote": "q"}]}, self.IDX)
        self.assertEqual([(r["relation"], r["related_ticker"]) for r in rels], [("customer", "NVDA")])


if __name__ == "__main__":
    unittest.main()
