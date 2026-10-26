"""Feature panel: wide (date × ticker) frames computed once, shared by live
scanning and historical base-rate replay so both see identical inputs."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import numpy as np
import pandas as pd

from scout.universe import BENCHMARK, SECTOR_ETFS

# Features persisted per ticker for the dashboard / Q&A agent.
SNAPSHOT_FEATURES = (
    "close", "ret_1d", "ret_5d", "ret_20d", "ret_63d", "ret_252d", "sma20", "sma50", "sma200",
    "atr14", "atr_pct", "vol_ratio", "dollar_vol20", "dist_52w_high", "rsi2", "rsi14",
    "gap_pct", "close_loc", "rs_pct", "sector_rs_pct", "sector_rel_20",
    "beta_60", "corr_60", "market_ret_1d", "idio_ret_1d", "idio_ret_5d", "idio_ret_20d", "market_ret_20d",
    "zscore_20", "bb_pct", "vol_20", "max_dd_60",
)


@dataclass
class FeaturePanel:
    f: dict[str, pd.DataFrame]
    regime: pd.Series                     # date → uptrend | mixed | downtrend
    sectors: dict[str, str | None]        # ticker → GICS sector
    stocks: list[str]                     # tickers eligible for detectors
    meta: dict = field(default_factory=dict)

    def __getitem__(self, name: str) -> pd.DataFrame:
        return self.f[name]

    @property
    def dates(self) -> pd.Index:
        return self.f["close"].index

    def row(self, ticker: str, d: date, names: tuple[str, ...] | None = None) -> dict[str, float]:
        out = {}
        for name in names or tuple(self.f):
            frame = self.f.get(name)
            if frame is None or ticker not in frame.columns:
                continue
            value = frame.at[d, ticker]
            if pd.notna(value):
                out[name] = float(value)
        return out


def _wide(bars: dict[str, pd.DataFrame], column: str) -> pd.DataFrame:
    return pd.DataFrame({t: b[column] for t, b in bars.items()}).sort_index()


def rsi(close: pd.DataFrame, n: int) -> pd.DataFrame:
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    rs = gain / loss.replace(0, np.nan)
    out = 100 - 100 / (1 + rs)
    return out.where(loss != 0, 100.0).where(gain.notna())


def market_regime(close: pd.Series) -> pd.Series:
    sma50 = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()
    up = (close > sma200) & (sma50 > sma200)
    down = (close < sma200) & (sma50 < sma200)
    out = pd.Series("mixed", index=close.index)
    out[up] = "uptrend"
    out[down] = "downtrend"
    out[sma200.isna()] = "unknown"
    return out


def build_panel(
    bars: dict[str, pd.DataFrame],
    instruments: dict[str, dict],
    min_price: float = 5.0,
    min_dollar_volume: float = 20_000_000.0,
) -> FeaturePanel:
    if BENCHMARK not in bars:
        raise ValueError(f"benchmark {BENCHMARK} bars are required")
    o, h, l, c, v = (_wide(bars, col) for col in ("open", "high", "low", "close", "volume"))
    # Align on benchmark trading days so holidays/gaps in one ticker don't shift others.
    idx = bars[BENCHMARK].index
    o, h, l, c, v = (x.reindex(idx) for x in (o, h, l, c, v))

    f: dict[str, pd.DataFrame] = {"open": o, "high": h, "low": l, "close": c, "volume": v}
    prev_c = c.shift(1)
    for n in (1, 5, 20, 63, 126, 252):
        f[f"ret_{n}d"] = c / c.shift(n) - 1
    for n in (20, 50, 200):
        f[f"sma{n}"] = c.rolling(n, min_periods=n).mean()

    tr = np.maximum.reduce([
        (h - l).to_numpy(), (h - prev_c).abs().to_numpy(), (l - prev_c).abs().to_numpy()
    ])
    tr = pd.DataFrame(tr, index=c.index, columns=c.columns)
    f["atr14"] = tr.rolling(14, min_periods=14).mean()
    f["atr_pct"] = f["atr14"] / c
    f["atr_pct_prev"] = f["atr_pct"].shift(1)

    vol_avg50 = v.rolling(50, min_periods=20).mean().shift(1)
    f["vol_ratio"] = v / vol_avg50.replace(0, np.nan)
    f["dollar_vol20"] = (c * v).rolling(20, min_periods=10).mean()

    f["high_55_prior"] = h.rolling(55, min_periods=55).max().shift(1)
    f["high_20"] = h.rolling(20, min_periods=20).max()
    f["low_5"] = l.rolling(5, min_periods=5).min()
    f["high_252"] = h.rolling(252, min_periods=200).max()
    f["dist_52w_high"] = c / f["high_252"] - 1

    # Risk profile: distance from the 20-day mean in standard deviations,
    # Bollinger %B (0 = lower band, 1 = upper band), annualised realised vol,
    # and the worst peak-to-trough drop over the last 60 sessions.
    std20 = c.rolling(20, min_periods=20).std()
    f["zscore_20"] = (c - f["sma20"]) / std20.replace(0, np.nan)
    f["bb_pct"] = (c - (f["sma20"] - 2 * std20)) / (4 * std20).replace(0, np.nan)
    f["vol_20"] = f["ret_1d"].rolling(20, min_periods=20).std() * np.sqrt(252)
    drawdown = c / c.rolling(60, min_periods=20).max() - 1
    f["max_dd_60"] = drawdown.rolling(60, min_periods=20).min()
    f["rsi2"] = rsi(c, 2)
    f["rsi14"] = rsi(c, 14)

    # Is it the stock or the market? 60-day beta/correlation to SPY; the move is
    # split into the market part (yesterday's beta × SPY return) and the rest.
    r, spy_r = f["ret_1d"], f["ret_1d"][BENCHMARK]
    roll = {"window": 60, "min_periods": 40}
    mean_r, mean_s = r.rolling(**roll).mean(), spy_r.rolling(**roll).mean()
    cov = r.mul(spy_r, axis=0).rolling(**roll).mean() - mean_r.mul(mean_s, axis=0)
    var_s = spy_r.rolling(**roll).var(ddof=0)
    f["beta_60"] = cov.div(var_s, axis=0)
    f["corr_60"] = cov.div(r.rolling(**roll).std(ddof=0).mul(np.sqrt(var_s), axis=0))
    f["market_ret_1d"] = f["beta_60"].shift(1).mul(spy_r, axis=0)
    f["idio_ret_1d"] = r - f["market_ret_1d"]
    f["idio_ret_5d"] = f["idio_ret_1d"].rolling(5, min_periods=5).sum()
    f["idio_ret_20d"] = f["idio_ret_1d"].rolling(20, min_periods=20).sum()
    f["market_ret_20d"] = f["market_ret_1d"].rolling(20, min_periods=20).sum()
    f["gap_pct"] = o / prev_c - 1
    rng = (h - l).replace(0, np.nan)
    f["close_loc"] = ((c - l) / rng).fillna(0.5)

    # Relative strength composite (IBD-style weighting of 3/6/12-month returns).
    rs_raw = 0.4 * f["ret_63d"] + 0.3 * f["ret_126d"] + 0.3 * f["ret_252d"]
    stocks = [t for t in c.columns if (instruments.get(t) or {}).get("kind", "stock") == "stock"]
    f["rs_raw"] = rs_raw
    f["rs_pct"] = rs_raw[stocks].rank(axis=1, pct=True).reindex(columns=c.columns)

    sectors = {t: (instruments.get(t) or {}).get("sector") for t in c.columns}
    sector_rs: list[pd.DataFrame] = []
    rel20_cols: dict[str, pd.Series] = {}
    rel63_cols: dict[str, pd.Series] = {}
    spy20, spy63 = f["ret_20d"][BENCHMARK], f["ret_63d"][BENCHMARK]
    for sector, etf in SECTOR_ETFS.items():
        members = [t for t in stocks if sectors.get(t) == sector]
        if not members:
            continue
        sector_rs.append(rs_raw[members].rank(axis=1, pct=True))
        if etf in c.columns:
            rel20 = f["ret_20d"][etf] - spy20
            rel63 = f["ret_63d"][etf] - spy63
            for t in members:
                rel20_cols[t] = rel20
                rel63_cols[t] = rel63

    def _full(frame: pd.DataFrame) -> pd.DataFrame:
        return frame.reindex(index=c.index, columns=c.columns)

    f["sector_rs_pct"] = _full(pd.concat(sector_rs, axis=1) if sector_rs else pd.DataFrame(index=c.index))
    f["sector_rel_20"] = _full(pd.DataFrame(rel20_cols, index=c.index))
    f["sector_rel_20_prev5"] = f["sector_rel_20"].shift(5)
    f["sector_rel_63"] = _full(pd.DataFrame(rel63_cols, index=c.index))

    f["liquid"] = (c >= min_price) & (f["dollar_vol20"] >= min_dollar_volume)

    return FeaturePanel(
        f=f,
        regime=market_regime(c[BENCHMARK]),
        sectors=sectors,
        stocks=stocks,
        meta={"tickers": len(c.columns), "stocks": len(stocks), "start": idx[0], "end": idx[-1]},
    )
