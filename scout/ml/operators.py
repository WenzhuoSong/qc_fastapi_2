"""Typed feature operators for the cross-sectional ranking model (pre-registered in
`scout/knowledge/external_specs/ml_v1.yaml#operators`).

- cross-sectional numeric (differs across stocks on a date): winsorize 1 % / 99 % within the date,
  rank to [−1, 1]; missing → 0 plus a missing flag; a sector-neutral variant ranks within the
  date's sector;
- context numeric (one value per date): expanding z-score, using only earlier dates;
- ordinal: integer codes in their order; nominal: one-hot for linear models (trees take the codes).
Every operator is point in time: a date's output depends on that date (and, for context, earlier ones).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

WINSOR = (0.01, 0.99)
MIN_HISTORY = 20                 # dates before an expanding z-score is defined


def winsorize(x: pd.Series) -> pd.Series:
    lo, hi = x.quantile(WINSOR[0]), x.quantile(WINSOR[1])
    return x.clip(lo, hi)


def rank_pm1(x: pd.Series) -> pd.Series:
    """Average rank mapped to [−1, 1] (ties share a rank); NaN stays NaN."""
    ok = x.notna()
    out = pd.Series(np.nan, index=x.index)
    n = int(ok.sum())
    if n == 1:
        out[ok] = 0.0
    elif n > 1:
        out[ok] = (x[ok].rank(method="average") - 1) / (n - 1) * 2 - 1
    return out


def cross_sectional(df: pd.DataFrame, cols: list[str], date: str = "date", group: str | None = None) -> pd.DataFrame:
    """Winsorize and rank each column within each date (or date × group); returns the ranked columns
    `<col>` plus `<col>_na` flags, missing ranks filled with 0."""
    keys = [date] if group is None else [date, group]
    out = pd.DataFrame(index=df.index)
    g = df.groupby(keys, sort=False, dropna=False)
    for c in cols:
        r = g[c].transform(lambda s: rank_pm1(winsorize(s)) if s.notna().any() else s)
        out[c] = r.fillna(0.0).astype("float32")
        out[f"{c}_na"] = df[c].isna().astype("int8")
    return out


def expanding_z(s: pd.Series, min_history: int = MIN_HISTORY) -> pd.Series:
    """z-score of each value against the mean and std of strictly earlier values (one row per date)."""
    prev = s.shift(1)
    mean = prev.expanding(min_history).mean()
    std = prev.expanding(min_history).std()
    return ((s - mean) / std.replace(0, np.nan)).clip(-5, 5)


def context(df: pd.DataFrame, cols: list[str], date: str = "date") -> pd.DataFrame:
    """Expanding z-scores of per-date values, broadcast back to the rows; missing → 0."""
    per_date = df.groupby(date, sort=True)[cols].first()
    z = pd.DataFrame({c: expanding_z(per_date[c]) for c in cols})
    return df[[date]].join(z, on=date)[cols].fillna(0.0).astype("float32")


def ordinal(s: pd.Series, order: list) -> pd.Series:
    """Integer codes in the given order; unknown or missing → −1."""
    codes = {v: i for i, v in enumerate(order)}
    return s.map(codes).fillna(-1).astype("int16")


def one_hot(s: pd.Series, prefix: str, categories: list) -> pd.DataFrame:
    """Fixed columns (the given categories), so every training window has the same design."""
    return pd.DataFrame({f"{prefix}={c}": (s == c).astype("int8") for c in categories}, index=s.index)
