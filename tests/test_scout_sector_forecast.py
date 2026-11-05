import unittest

import numpy as np
import pandas as pd

from scout import sector_forecast as sf


def _ret(n=300, k=12, seed=0):
    rng = np.random.default_rng(seed)
    idx = pd.period_range("1990-01", periods=n, freq="M")
    return pd.DataFrame(rng.normal(0.008, 0.05, (n, k)), index=idx, columns=[f"I{i}" for i in range(k)])


class SectorForecastTests(unittest.TestCase):
    def test_seasonality_uses_the_target_calendar_month(self):
        r = _ret()
        r.loc[r.index.month == 3, "I0"] = 0.10                    # every March
        s = sf.f2_signal(r)
        feb = [m for m in s.index if m.month == 2 and m.year >= 2000][0]
        self.assertAlmostEqual(s.at[feb, "I0"], 0.10)              # at end of February: forecast for March
        jan = [m for m in s.index if m.month == 1 and m.year >= 2000][0]
        self.assertLess(abs(s.at[jan, "I0"]), 0.05)

    def test_seasonality_uses_only_past_data(self):
        r = _ret()
        full, cut = sf.f2_signal(r), sf.f2_signal(r.iloc[:200])
        pd.testing.assert_frame_equal(full.iloc[:200], cut)

    def test_excess_cum(self):
        r = _ret(n=30)
        rf = pd.Series(0.001, index=r.index)
        e = sf.excess_cum(r, rf, 12)
        expect = np.prod(1 + r["I1"].iloc[:12]) / 1.001 ** 12 - 1
        self.assertAlmostEqual(e["I1"].iloc[11], expect)
        self.assertTrue(np.isnan(e["I1"].iloc[10]))

    def test_quarter_signals(self):
        cd = pd.date_range("2015-03-31", periods=16, freq="QE")
        q = pd.DataFrame({"ticker": "A", "calendardate": cd, "date": cd + pd.Timedelta(days=40),
                          "eps": np.arange(16) * 0.1 + np.random.default_rng(3).normal(0, 0.02, 16),
                          "revenue": 100 * 1.02 ** (np.arange(16) ** 1.3)})
        out = sf.quarter_signals(q)
        self.assertTrue(out["sue"].iloc[:10].isna().all())         # a year-ago difference from q4, then 6 before it
        self.assertTrue(out["sue"].iloc[10:].notna().all())
        self.assertTrue(out["sue"].iloc[-1] > 0)
        self.assertEqual(out["accel"].iloc[-1], True)              # growth accelerating by construction

    def test_cross_section_top_minus_average(self):
        r = _ret(n=60)
        signal = r.shift(-1)                                       # perfect foresight of next month (row m = month m+1)
        d = sf.cross_section(signal, r.shift(-1))
        self.assertGreater(d["top"].mean(), 0.03)
        self.assertGreater(d["ic"].mean(), 0.99)

    def test_probability_out_of_sample(self):
        rng = np.random.default_rng(1)
        m = pd.period_range("1990-01", periods=360, freq="M")
        rows = []
        for p in m:
            for _ in range(10):
                x = rng.random()
                rows.append({"m": p, "rank": x, "y": float(rng.random() < 0.4 + 0.2 * x)})
        out = sf.probability(pd.DataFrame(rows), ["rank"])
        self.assertLess(out["all"]["logloss"], out["all"]["logloss_base"])
        self.assertTrue(0.5 < out["calibration_slope"] < 1.5)


if __name__ == "__main__":
    unittest.main()
