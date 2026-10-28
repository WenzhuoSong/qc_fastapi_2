import unittest
from datetime import date

from scout.gate import monthly_t


class MonthlyTTests(unittest.TestCase):
    def test_clustered_signals_count_once_per_month(self):
        dates = [date(2020, 1, d) for d in (2, 3, 6)] + [date(2020, 2, 3), date(2020, 3, 2), date(2020, 4, 1)]
        mean, t, months = monthly_t(dates, [0.01, 0.01, 0.01, 0.02, 0.0, 0.01])
        self.assertEqual(months, 4)
        self.assertAlmostEqual(mean, 0.06 / 6)
        self.assertGreater(t, 0)

    def test_too_few_months_gives_no_t(self):
        self.assertEqual(monthly_t([date(2020, 1, 2)], [0.01])[1], None)
        self.assertEqual(monthly_t([], []), (None, None, 0))


if __name__ == "__main__":
    unittest.main()
