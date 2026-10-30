"""Stage 4 data (pre-registered in `scout/knowledge/external_specs/ml_v4.yaml`): feature store v2 over the
market-cap top 3000 (v1's top 1000 plus ranks 1001–3000, discovery period only) and the residual,
volatility-scaled label.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from scout import sharadar_research as sr
from scout.ml import features as F

UNTIL = "2015-12-31"                 # last sample date of store v2
PRICES_UNTIL = "2016-06-30"          # enough history for the 63-session labels of late-2015 samples
RES_FACTORS = ["beta_252", "log_mcap", "bm", "mom_12_1"]


def merge(large: pd.DataFrame, small: pd.DataFrame) -> pd.DataFrame:
    """Union of the two raw parts with the universe-dependent columns recomputed over the top 3000:
    breadth (stock-count weighted), size bucket, labels relative to the union's mean."""
    large, small = large.assign(in_large=1), small.assign(in_large=0)
    raw = pd.concat([large, small], ignore_index=True)
    per = raw.groupby(["date", "in_large"])["breadth"].first()      # one value per part and date
    tot = raw.groupby(["date", "in_large"]).size()
    weighted = (per * tot).groupby(level=0).sum() / tot.groupby(level=0).sum()
    raw["breadth"] = raw["date"].map(weighted).to_numpy()
    raw["size_bucket"] = (raw["cap_rank"] * 10 // sr.SMALL).clip(0, 9).astype(int).rsub(9).to_numpy()
    for h in F.HORIZONS:
        s = raw[f"y{h}_raw"]
        raw[f"y{h}"] = s - s.groupby(raw["date"]).transform("mean")
        raw[f"y{h}_sec"] = s - s.groupby([raw["date"], raw["sector"]]).transform("mean")
    return raw


def residual_label(df: pd.DataFrame, n: int, vol_raw: pd.Series) -> pd.Series:
    """Per date: OLS of the raw return on a constant, the factor ranks and sector dummies; residual divided
    by the stock's 63-session volatility (known on the sample date). NaN where the return or volatility is."""
    y = df[f"y{n}_raw"].to_numpy(dtype=float)
    out = np.full(len(df), np.nan)
    sectors = sorted(df["sector"].dropna().unique())
    for _, idx in df.groupby("date").indices.items():
        ok = idx[np.isfinite(y[idx])]
        if len(ok) < 50:
            continue
        g = df.iloc[ok]
        X = np.column_stack([np.ones(len(ok)), g[RES_FACTORS].to_numpy(dtype=float),
                             *[(g["sector"] == s).to_numpy(dtype=float) for s in sectors[1:]]])
        beta, *_ = np.linalg.lstsq(X, y[ok], rcond=None)
        out[ok] = y[ok] - X @ beta
    v = vol_raw.to_numpy(dtype=float)
    with np.errstate(all="ignore"):
        scaled = out / np.where(v > 0, v, np.nan)
    return pd.Series(scaled, index=df.index)
