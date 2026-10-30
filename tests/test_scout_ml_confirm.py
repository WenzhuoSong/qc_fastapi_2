import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

from scout.ml import confirm


class ConfirmTests(unittest.TestCase):
    def test_runs_only_once(self):
        with tempfile.TemporaryDirectory() as d:
            lock = Path(d) / "confirm_v1.json"
            lock.write_text("{}")
            with mock.patch.object(confirm, "RESULT", lock), mock.patch.object(confirm.pd, "read_parquet") as read:
                with self.assertRaises(SystemExit):
                    confirm.run()
                read.assert_not_called()

    def test_net_spread_pays_one_round_trip_and_passes_only_on_a_strong_edge(self):
        rng = np.random.default_rng(0)
        dates = pd.bdate_range("2016-01-04", periods=520, freq="5B")
        rows = []
        for d in dates:
            s = rng.normal(size=100)
            rows.append(pd.DataFrame({"date": d, "s": s, "y21": 0.002 * s + rng.normal(scale=0.01, size=100), "regime": 2}))
        df = pd.concat(rows, ignore_index=True)
        m = confirm.metrics(df, "s")
        self.assertAlmostEqual(m["top_decile_gross"]["all"]["mean"] - m["top_decile_net"]["all"]["mean"],
                               confirm.ROUND_TRIP_COST, places=4)
        self.assertTrue(m["passed"])
        df["s"] = rng.normal(size=len(df))
        self.assertFalse(confirm.metrics(df, "s")["passed"])


if __name__ == "__main__":
    unittest.main()
