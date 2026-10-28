import unittest
from datetime import date, timedelta

import numpy as np
import pandas as pd

from scout import angles, rankers


class EventTests(unittest.TestCase):
    def test_one_announcement_per_quarter(self):
        d = date(2025, 1, 30)
        out = angles.earnings_dates([("A", d), ("A", d + timedelta(days=3)), ("A", d + timedelta(days=91)), ("B", d)])
        self.assertEqual(out["A"], [d, d + timedelta(days=91)])

    def test_announcement_return_is_known_after_its_window(self):
        idx = pd.bdate_range("2025-01-01", periods=30)

        class P:
            dates = pd.Index([x.date() for x in idx])
            frame = pd.DataFrame({"A": 100.0, "SPY": 100.0}, index=dates)

            def __getitem__(self, k):
                return self.frame

        p = P()
        p.frame.loc[p.dates[11]:, "A"] = 110.0                  # +10 % on the filing day
        (ev,) = angles.announcement_returns({"A": [p.dates[11]]}, p)
        self.assertEqual(ev["known"], 12)                        # the close after the filing day
        self.assertAlmostEqual(ev["ear"], 0.10)


def _q(year, q):
    return date(year, 3 * q, 28)


class FinancialTests(unittest.TestCase):
    def rows(self):
        out = []
        for k, (y, q) in enumerate([(2023, 1), (2023, 2), (2023, 3), (2023, 4), (2024, 1), (2024, 2), (2024, 3), (2024, 4)]):
            end = _q(y, q)
            filed = end + timedelta(days=35)
            out += [{"ticker": "A", "metric": "revenue", "period_end": end, "value": 100.0 + 10 * k, "filed": filed},
                    {"ticker": "A", "metric": "cost_of_revenue", "period_end": end, "value": 60.0, "filed": filed},
                    {"ticker": "A", "metric": "net_income", "period_end": end, "value": 10.0, "filed": filed},
                    {"ticker": "A", "metric": "operating_cash_flow", "period_end": end, "value": 15.0, "filed": filed},
                    {"ticker": "A", "metric": "total_assets", "period_end": end, "value": 1000.0 + 50 * k, "filed": filed},
                    {"ticker": "A", "metric": "shares_diluted", "period_end": end, "value": 100.0 - k, "filed": filed}]
        # a restatement of the last quarter's revenue, filed much later
        out.append({"ticker": "A", "metric": "revenue", "period_end": _q(2024, 4), "value": 999.0, "filed": date(2025, 12, 1)})
        return out

    def test_point_in_time_and_features(self):
        pit = angles.PointInTime(self.rows())
        d = date(2025, 2, 15)
        pit.advance(d)
        f = angles.fundamentals_now(pit, "A", d)
        self.assertAlmostEqual(f["asset_growth"], 1350 / 1150 - 1)
        self.assertAlmostEqual(f["gross_profitability"], (sum(100 + 10 * k for k in range(4, 8)) - 240) / 1350)
        self.assertAlmostEqual(f["accruals"], (40 - 60) / ((1350 + 1150) / 2))
        self.assertAlmostEqual(f["net_issuance"], 93 / 97 - 1)
        self.assertAlmostEqual(f["revenue_growth"], 170 / 130 - 1)
        pit.advance(date(2025, 12, 2))                           # the restatement is now known
        self.assertEqual(pit.series("A", "revenue")[-1][1], 999.0)

    def test_nothing_before_it_was_filed(self):
        pit = angles.PointInTime(self.rows())
        pit.advance(date(2023, 3, 1))
        self.assertEqual(angles.fundamentals_now(pit, "A", date(2023, 3, 1)), {})


class InsiderTests(unittest.TestCase):
    def test_cluster_of_officers_and_directors_buying(self):
        d = date(2025, 6, 30)
        base = {"ticker": "A", "code": "P", "common": True, "plan_10b5_1": False, "value": 1e5}
        trades = [{**base, "filed": d - timedelta(days=10), "relationship": "Officer", "owner_cik": "1"},
                  {**base, "filed": d - timedelta(days=20), "relationship": "Director", "owner_cik": "2"},
                  {**base, "filed": d - timedelta(days=20), "relationship": "Director", "owner_cik": "2"},
                  {**base, "filed": d - timedelta(days=30), "relationship": "TenPercentOwner", "owner_cik": "3"},
                  {**base, "filed": d - timedelta(days=5), "relationship": "Officer", "owner_cik": "4", "plan_10b5_1": True},
                  {**base, "filed": d - timedelta(days=200), "relationship": "Officer", "owner_cik": "5"},
                  {**base, "filed": d + timedelta(days=1), "relationship": "Officer", "owner_cik": "6"}]
        self.assertEqual(angles.insider_buying(trades, d)["A"]["buyers"], 2)

    def test_insider_cluster_ranker(self):
        a = {"angle": {800: {1: 2.0, 2: 1.0, 3: 3.0}}, "liquid": np.ones((900, 5), dtype=bool),
             "close": np.ones((900, 5))}
        self.assertEqual(sorted(rankers.insider_cluster(a, 800, 0.1)), [1, 3])
