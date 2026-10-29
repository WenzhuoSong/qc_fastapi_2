"""Point-in-time, survivorship-free research arrays from the local Sharadar files (local only).

Pre-registered in `scout/knowledge/external_specs/sharadar_rechecks.yaml`:
- eligible: domestic common stock (incl. primary class) on NYSE / NASDAQ / NYSEMKT, unadjusted close
  ≥ $5, with a market cap that day; large / mid = the 1000 largest by market cap that day, small =
  ranks 1001–3000; companies that later delisted are included;
- a holding is bought at the next open and valued with the dividend-adjusted close; a stock that
  stops trading inside the window is settled at its last close, −30 % for a performance delisting
  (bankruptcy, or delisted without an acquisition within 30 days; Shumway 1997).
Arrays are dates × stocks (numpy), built only for the stocks that are ever in the universe on the
days a study needs.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from scout.data.sharadar import ROOT

PARQUET = ROOT / "parquet"
CATEGORIES = ("Domestic Common Stock", "Domestic Common Stock Primary Class")
EXCHANGES = ("NYSE", "NASDAQ", "NYSEMKT")
MIN_PRICE = 5.0
LARGE = 1000
SMALL = 3000
DELIST_HAIRCUT = -0.30
DELIST_WINDOW_DAYS = 30
START = "1998-01-01"


def _read(table: str, columns: list[str], filters=None) -> pd.DataFrame:
    import pyarrow.parquet as pq

    return pq.read_table(PARQUET / f"{table}.parquet", columns=columns, filters=filters).to_pandas()


def eligible() -> pd.DataFrame:
    t = _read("tickers", ["table", "ticker", "permaticker", "category", "exchange", "sector", "isdelisted"])
    t = t[(t["table"] == "SEP") & t["category"].isin(CATEGORIES) & t["exchange"].isin(EXCHANGES)]
    return t.drop_duplicates("ticker").set_index("ticker")


def trading_days() -> pd.DatetimeIndex:
    spy = _read("funds", ["ticker", "date"], filters=[("ticker", "=", "SPY")])
    d = pd.to_datetime(spy["date"]).sort_values()
    return pd.DatetimeIndex(d[d >= START].unique())


def universe(days: pd.DatetimeIndex, elig: pd.DataFrame) -> dict[pd.Timestamp, dict[str, list[str]]]:
    """{day: {"large": [...], "small": [...]}} by that day's market cap among eligible stocks priced ≥ $5."""
    want = [d.date() for d in days]
    cap = _read("daily", ["ticker", "date", "marketcap"], filters=[("date", "in", want)])
    px = _read("stocks", ["ticker", "date", "closeunadj"], filters=[("date", "in", want)])
    df = cap.merge(px, on=["ticker", "date"], how="inner")
    df = df[df["ticker"].isin(elig.index) & (df["closeunadj"] >= MIN_PRICE) & df["marketcap"].notna()]
    out = {}
    for d, g in df.groupby("date"):
        names = g.sort_values("marketcap", ascending=False)["ticker"].tolist()
        out[pd.Timestamp(d)] = {"large": names[:LARGE], "small": names[LARGE:SMALL]}
    return out


@dataclass
class Prices:
    dates: pd.DatetimeIndex
    tickers: list[str]
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    closeadj: np.ndarray
    volume: np.ndarray
    haircut: np.ndarray            # per stock: the delisting adjustment applied at its last close
    col: dict[str, int] = field(default_factory=dict)

    def __post_init__(self):
        self.col = {t: j for j, t in enumerate(self.tickers)}


def delisting_haircut(tickers: list[str]) -> dict[str, float]:
    """−30 % for a performance delisting (bankruptcy, or delisted with no acquisition within 30 days), else 0."""
    a = _read("actions", ["date", "action", "ticker"], filters=[("ticker", "in", tickers)])
    a["date"] = pd.to_datetime(a["date"])
    out = {}
    for t, g in a.groupby("ticker"):
        dl = g[g["action"].isin(["delisted", "bankruptcyliquidation"])]
        if dl.empty:
            continue
        when = dl["date"].max()
        acq = g[(g["action"] == "acquisitionby") & ((g["date"] - when).abs() <= pd.Timedelta(days=DELIST_WINDOW_DAYS))]
        bankrupt = (g["action"] == "bankruptcyliquidation").any()
        out[t] = DELIST_HAIRCUT if bankrupt or acq.empty else 0.0
    return out


def prices(tickers: list[str], dates: pd.DatetimeIndex) -> Prices:
    cols = ["ticker", "date", "open", "high", "low", "close", "closeadj", "volume"]
    s = _read("stocks", cols, filters=[("ticker", "in", tickers)])
    s["date"] = pd.to_datetime(s["date"])
    s = s[s["date"].isin(dates)]
    arrays = {}
    for k in cols[2:]:
        w = s.pivot(index="date", columns="ticker", values=k).reindex(index=dates, columns=tickers)
        arrays[k] = w.to_numpy(dtype=float)
    hc = delisting_haircut(tickers)
    return Prices(dates, tickers, arrays["open"], arrays["high"], arrays["low"], arrays["close"], arrays["closeadj"],
                  arrays["volume"], np.array([hc.get(t, 0.0) for t in tickers]))


def holding_return(p: Prices, j: int, i: int, hold: int) -> float | None:
    """Buy at the open after day i, value at the close `hold` sessions later (dividend-adjusted);
    a stock that stops trading is settled at its last close, with its delisting adjustment."""
    a, b = i + 1, i + hold
    if b >= len(p.dates) or not (np.isfinite(p.open[a, j]) and p.open[a, j] > 0 and np.isfinite(p.closeadj[a, j])
                                  and np.isfinite(p.close[a, j]) and p.close[a, j] > 0):
        return None
    entry = p.closeadj[a, j] * p.open[a, j] / p.close[a, j]        # the open, on the dividend-adjusted scale
    seg = p.closeadj[a:b + 1, j]
    if np.isfinite(seg[-1]):
        return float(seg[-1] / entry - 1)
    ok = np.flatnonzero(np.isfinite(seg))
    if not ok.size:
        return None
    later = p.closeadj[b + 1:, j] if b + 1 < len(p.dates) else np.array([])
    stopped = not np.isfinite(later).any()                          # never trades again: delisted
    last = seg[ok[-1]] / entry - 1
    return float((1 + last) * (1 + (p.haircut[j] if stopped else 0.0)) - 1)


def asset_growth(days: pd.DatetimeIndex, tickers: list[str]) -> dict[pd.Timestamp, dict[str, float]]:
    """{day: {ticker: total-asset growth}}: the latest quarter filed by that day over the quarter four
    reports earlier (As Reported, ARQ), the two report periods 330–400 days apart, filed within 400 days."""
    f = _read("fundamentals", ["ticker", "dimension", "datekey", "reportperiod", "assets"],
              filters=[("dimension", "=", "ARQ"), ("ticker", "in", tickers)])
    f = f.dropna(subset=["assets", "datekey", "reportperiod"])
    f["datekey"], f["reportperiod"] = pd.to_datetime(f["datekey"]), pd.to_datetime(f["reportperiod"])
    f = f.sort_values(["ticker", "reportperiod"]).drop_duplicates(["ticker", "reportperiod"], keep="first")
    g = f.groupby("ticker")
    f["prev_assets"], f["prev_period"] = g["assets"].shift(4), g["reportperiod"].shift(4)
    gap = (f["reportperiod"] - f["prev_period"]).dt.days
    f = f[(gap >= 330) & (gap <= 400) & (f["prev_assets"] > 0)]
    f["growth"] = f["assets"] / f["prev_assets"] - 1
    f = f.sort_values("datekey")
    out = {}
    for d in days:
        known = f[(f["datekey"] <= d) & (f["datekey"] > d - pd.Timedelta(days=400))]
        last = known.groupby("ticker").tail(1)
        out[d] = dict(zip(last["ticker"], last["growth"]))
    return out
