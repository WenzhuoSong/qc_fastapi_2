import io
import unittest
import zipfile
from datetime import date

from scout.data import sec_insiders


def _zip(tables: dict[str, list[list[str]]]) -> zipfile.ZipFile:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, rows in tables.items():
            z.writestr(name, "\n".join("\t".join(r) for r in rows))
    buf.seek(0)
    return zipfile.ZipFile(buf)


class ParseTests(unittest.TestCase):
    def test_keeps_open_market_trades_of_followed_issuers(self):
        z = _zip({
            "SUBMISSION.tsv": [["ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERCIK", "AFF10B5ONE"],
                               ["A1", "05-MAR-2025", "4", "0000000001", "0"], ["A2", "06-MAR-2025", "4", "0000000002", ""]],
            "REPORTINGOWNER.tsv": [["ACCESSION_NUMBER", "RPTOWNERCIK", "RPTOWNERNAME", "RPTOWNER_RELATIONSHIP", "RPTOWNER_TITLE"],
                                   ["A1", "99", "Jane CEO", "Officer", "CEO"]],
            "NONDERIV_TRANS.tsv": [["ACCESSION_NUMBER", "NONDERIV_TRANS_SK", "SECURITY_TITLE", "TRANS_DATE", "TRANS_CODE",
                                    "TRANS_SHARES", "TRANS_PRICEPERSHARE"],
                                   ["A1", "1", "Common Stock", "03-MAR-2025", "P", "1000", "50"],
                                   ["A1", "2", "Common Stock", "03-MAR-2025", "M", "10", "0"],          # option exercise: dropped
                                   ["A1", "3", "Series A Preferred", "03-MAR-2025", "P", "5", "100"],
                                   ["A2", "4", "Common Stock", "04-MAR-2025", "S", "200", "10"]],          # issuer not followed
        })
        rows = sec_insiders.parse(z, {"0000000001": "AAA"})
        self.assertEqual([(r["trans_sk"], r["code"], r["common"]) for r in rows], [(1, "P", True), (3, "P", False)])
        r = rows[0]
        self.assertEqual((r["ticker"], r["filed"], r["trans_date"], r["value"], r["owner_name"], r["plan_10b5_1"]),
                         ("AAA", date(2025, 3, 5), date(2025, 3, 3), 50000.0, "Jane CEO", False))

    def test_periods(self):
        self.assertEqual(sec_insiders.periods(date(2025, 11, 1), date(2026, 2, 1)), ["2025q4", "2026q1"])
        self.assertTrue(sec_insiders.is_common("Class A Common Stock"))
        self.assertFalse(sec_insiders.is_common("6.5% Series A Preferred Stock"))
