"""Cross-sectional ranking strategies from the literature, for the gate's ranking mode.

Each ranker takes the panel arrays and a day index `i` and returns the stocks it would hold
from the next open, using only data up to the close of day i. Definitions follow the source
papers; where Scout's data forces a simplification it is stated here and in the spec.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

MONTH = 21
YEAR = 252


def _universe(a: dict, i: int) -> np.ndarray:
    """Liquid stocks with a full year of history at day i (column indexes)."""
    return np.flatnonzero(a["liquid"][i] & np.isfinite(a["close"][i]) & np.isfinite(a["close"][max(0, i - YEAR)]))


def _top(cols: np.ndarray, score: np.ndarray, frac: float) -> list[int]:
    ok = np.isfinite(score)
    cols, score = cols[ok], score[ok]
    if not len(cols):
        return []
    k = max(1, int(round(len(cols) * frac)))
    return [int(c) for c in cols[np.argsort(-score, kind="stable")[:k]]]


def residual_momentum(a: dict, i: int, frac: float) -> list[int]:
    """Blitz, Huij & Martens (2011): regress each stock's monthly returns over the past 36 months
    on the market, take the residuals of months t−12 … t−2 and rank by their sum divided by their
    standard deviation. Simplification: market model with SPY, not the Fama-French three factors."""
    if i < 36 * MONTH:
        return []
    cols = _universe(a, i)
    idx = [i - MONTH * k for k in range(36, -1, -1)]          # 37 month-end closes, oldest first
    px = a["close"][idx][:, cols]
    mk = a["spy"][idx]
    r = px[1:] / px[:-1] - 1
    m = mk[1:] / mk[:-1] - 1
    full = np.isfinite(r).all(axis=0)
    cols, r = cols[full], r[:, full]
    if not len(cols):
        return []
    mc = m - m.mean()
    beta = (mc[:, None] * (r - r.mean(axis=0))).sum(axis=0) / (mc ** 2).sum()
    alpha = r.mean(axis=0) - beta * m.mean()
    e = r - alpha - beta * m[:, None]
    window = e[-12:-1]                                          # months t−12 … t−2
    sd = window.std(axis=0, ddof=1)
    score = np.where(sd > 0, window.sum(axis=0) / sd, np.nan)
    return _top(cols, score, frac)


def high_52w(a: dict, i: int, frac: float) -> list[int]:
    """George & Hwang (2004): nearness to the 52-week high, close / highest high of the past year."""
    cols = _universe(a, i)
    hi = np.nanmax(a["high"][i - YEAR + 1: i + 1][:, cols], axis=0)
    score = a["close"][i, cols] / hi
    return _top(cols, score, frac)


def frog_in_pan(a: dict, i: int, frac: float) -> list[int]:
    """Da, Gurun & Warachka (2014): among past winners (top fifth by the return from t−12 to t−1
    months), prefer momentum built from many small moves. Information discreteness
    ID = sign(PRET) × (% negative days − % positive days) over the formation window; lowest ID first."""
    cols = _universe(a, i)
    c = a["close"]
    pret = c[i - MONTH, cols] / c[i - YEAR, cols] - 1
    win = _top(cols, pret, 0.2)
    if not win:
        return []
    w = np.array(win)
    d = c[i - YEAR + 1: i - MONTH + 1][:, w] / c[i - YEAR: i - MONTH][:, w] - 1
    n = np.isfinite(d).sum(axis=0)
    pos, neg = (d > 0).sum(axis=0) / np.maximum(n, 1), (d < 0).sum(axis=0) / np.maximum(n, 1)
    pr = c[i - MONTH, w] / c[i - YEAR, w] - 1
    idisc = np.sign(pr) * (neg - pos)
    k = max(1, int(round(len(cols) * frac)))
    return [int(x) for x in w[np.argsort(idisc, kind="stable")[:k]]]


def trend_template(a: dict, i: int, frac: float) -> list[int]:
    """Minervini's trend template (the common TradingView screener): close above the 150- and
    200-day averages, 150 above 200, the 200-day rising for a month, 50 above 150 and 200, close
    above the 50-day, at least 30 % above the 52-week low and within 25 % of the 52-week high,
    relative strength rank ≥ 70. All stocks that pass are held (`frac` is unused)."""
    cols = _universe(a, i)
    c = a["close"]
    sma = lambda n, at: np.nanmean(c[at - n + 1: at + 1][:, cols], axis=0)  # noqa: E731
    s50, s150, s200, s200_prev = sma(50, i), sma(150, i), sma(200, i), sma(200, i - MONTH)
    px = c[i, cols]
    lo = np.nanmin(a["low"][i - YEAR + 1: i + 1][:, cols], axis=0)
    hi = np.nanmax(a["high"][i - YEAR + 1: i + 1][:, cols], axis=0)
    ok = ((px > s150) & (px > s200) & (s150 > s200) & (s200 > s200_prev) & (s50 > s150) & (s50 > s200) & (px > s50)
          & (px >= 1.30 * lo) & (px >= 0.75 * hi) & (a["rs_pct"][i, cols] >= 0.70))
    return [int(x) for x in cols[ok]]


RANKERS = {"residual_momentum": residual_momentum, "high_52w": high_52w, "frog_in_pan": frog_in_pan,
           "trend_template": trend_template}


def arrays(p) -> tuple[dict, list[str]]:
    """Panel → numpy arrays over the stock columns, plus SPY."""
    stocks = [t for t in p.stocks if t in p["close"].columns]
    a = {k: p[k][stocks].to_numpy(dtype=float) for k in ("open", "high", "low", "close", "rs_pct")}
    a["liquid"] = p["liquid"][stocks].fillna(False).to_numpy(dtype=bool)
    a["spy"] = p["close"]["SPY"].to_numpy(dtype=float)
    a["spy_open"] = p["open"]["SPY"].to_numpy(dtype=float)
    return a, stocks
