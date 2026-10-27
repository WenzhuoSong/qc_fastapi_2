"""Abnormal moves ('异动'): detection and what usually happens afterwards.

A day is abnormal when the stock's own move (after the market part, beta ×
SPY) is at least EVENT_SIGMA standard deviations of its own recent idio
moves, measured before the day (ex ante), or the raw move is very large, or
volume explodes with a real move. Each event records how it happened: the
overnight gap versus the session, volume, the sector and SPY that day, and
the path since.

`compute_base_rates` replays the same definition over the scan universe:
forward 5- and 20-session returns after abnormal moves, by direction, by
timing (gap vs intraday) and by trend (above / below the 200-day line).
Academic background: Chan (2003, JFE) finds drift after moves with news and
reversal after moves without news; the replay measures it on our universe.
"""
from __future__ import annotations

import math
from datetime import date

import numpy as np
import pandas as pd

from scout import db
from scout.features import FeaturePanel
from scout.universe import BENCHMARK

EVENT_SIGMA = 3.0          # own idio moves, ex ante
EVENT_MIN_SIGMA_MOVE = 0.04
EVENT_BIG_MOVE = 0.08      # raw move counted regardless of sigma
EVENT_VOLUME = 3.0         # × 50-day average, with at least EVENT_VOLUME_MOVE
EVENT_VOLUME_MOVE = 0.03
SIGMA_WINDOW = 60
LOOKBACK_SESSIONS = 60
MAX_EVENTS = 3
TIMING_SHARE = 0.6         # a part (gap or session) carrying 60%+ of the move names the timing


def _series(rows: list[dict], col: str = "close") -> pd.Series:
    return pd.Series({r["date"]: float(r[col]) for r in rows}, dtype="float64").sort_index()


def frame(bars: list[dict], spy: list[dict], sector: list[dict] | None = None) -> pd.DataFrame:
    """One ticker's daily frame with the idio split, ex-ante sigma and volume ratio."""
    df = pd.DataFrame(bars).set_index("date").sort_index()[["open", "high", "low", "close", "volume"]].astype(float)
    spy_c = _series(spy).reindex(df.index).ffill()
    df["ret"] = df["close"].pct_change()
    df["spy_ret"] = spy_c.pct_change()
    if sector:
        df["sector_ret"] = _series(sector).reindex(df.index).ffill().pct_change()
    cov = df["ret"].rolling(60, min_periods=40).cov(df["spy_ret"])
    var = df["spy_ret"].rolling(60, min_periods=40).var()
    df["beta"] = (cov / var).shift(1)
    df["idio"] = df["ret"] - df["beta"].fillna(1.0) * df["spy_ret"]
    df["sigma"] = df["idio"].rolling(SIGMA_WINDOW, min_periods=40).std().shift(1)
    df["vol_ratio"] = df["volume"] / df["volume"].rolling(50, min_periods=20).mean().shift(1)
    df["gap"] = df["open"] / df["close"].shift(1) - 1
    df["intraday"] = df["close"] / df["open"] - 1
    return df


def is_event(ret: float, idio: float, sigma: float | None, vol_ratio: float | None) -> bool:
    if any(v is None or (isinstance(v, float) and math.isnan(v)) for v in (ret, idio)):
        return False
    if abs(ret) >= EVENT_BIG_MOVE:
        return True
    if sigma and not math.isnan(sigma) and abs(idio) >= EVENT_SIGMA * sigma and abs(ret) >= EVENT_MIN_SIGMA_MOVE:
        return True
    return bool(vol_ratio and not math.isnan(vol_ratio) and vol_ratio >= EVENT_VOLUME and abs(ret) >= EVENT_VOLUME_MOVE)


def timing(gap: float, intraday: float) -> str:
    total = abs(gap) + abs(intraday)
    if total == 0:
        return "—"
    if abs(gap) >= TIMING_SHARE * total:
        return "隔夜跳空"
    if abs(intraday) >= TIMING_SHARE * total:
        return "盘中"
    return "隔夜与盘中"


def detect(df: pd.DataFrame, lookback: int = LOOKBACK_SESSIONS, limit: int = MAX_EVENTS) -> list[dict]:
    """The largest abnormal days in the last `lookback` sessions, newest first."""
    recent = df.iloc[-lookback:]
    last_close = float(df["close"].iloc[-1])
    out = []
    for i, (d, r) in enumerate(recent.iterrows()):
        sigma = r.get("sigma")
        if not is_event(r["ret"], r["idio"], sigma, r.get("vol_ratio")):
            continue
        ev = {
            "date": d.isoformat() if hasattr(d, "isoformat") else str(d),
            "ret": float(r["ret"]), "idio": float(r["idio"]),
            "sigma_mult": float(r["idio"] / sigma) if sigma and not math.isnan(sigma) and sigma > 0 else None,
            "gap": float(r["gap"]), "intraday": float(r["intraday"]), "timing": timing(r["gap"], r["intraday"]),
            "vol_ratio": None if pd.isna(r.get("vol_ratio")) else float(r["vol_ratio"]),
            "spy_ret": None if pd.isna(r["spy_ret"]) else float(r["spy_ret"]),
            "sector_ret": None if pd.isna(r.get("sector_ret", np.nan)) else float(r["sector_ret"]),
            "after_ret": last_close / float(r["close"]) - 1,
            "sessions_ago": len(recent) - 1 - i,
            "close": float(r["close"]),
        }
        out.append(ev)
    out.sort(key=lambda e: abs(e["idio"]), reverse=True)
    return sorted(out[:limit], key=lambda e: e["date"], reverse=True)


async def recent(ticker: str, sector_etf: str | None, lookback: int = LOOKBACK_SESSIONS) -> list[dict]:
    """Detect abnormal days for one ticker from stored bars."""
    def q(t: str) -> str:
        return ("SELECT * FROM (SELECT date, open, high, low, close, volume FROM scout.bars_daily WHERE ticker = :t "
                "ORDER BY date DESC LIMIT 200) x ORDER BY date")

    bars = await db.fetch_all(q(ticker), {"t": ticker})
    if len(bars) < 80:
        return []
    spy = await db.fetch_all(q(BENCHMARK), {"t": BENCHMARK})
    sector = await db.fetch_all(q(sector_etf), {"t": sector_etf}) if sector_etf else None
    return detect(frame(bars, spy, sector), lookback)


# ── What usually happens next (replay over the scan universe) ──

def bucket_key(direction: str, when: str, trend: str) -> str:
    return f"{direction}|{when}|{trend}"


def compute_base_rates(p: FeaturePanel) -> dict[str, dict]:
    """Forward returns after abnormal moves, by direction × timing × trend.

    Excess is against SPY over the same sessions; survivorship bias applies
    (today's constituents)."""
    idio, ret = p["idio_ret_1d"], p["ret_1d"]
    sigma = idio.rolling(SIGMA_WINDOW, min_periods=40).std().shift(1)
    vol = p["vol_ratio"]
    big = ret.abs() >= EVENT_BIG_MOVE
    sig = (idio.abs() >= EVENT_SIGMA * sigma) & (ret.abs() >= EVENT_MIN_SIGMA_MOVE)
    volx = (vol >= EVENT_VOLUME) & (ret.abs() >= EVENT_VOLUME_MOVE)
    mask = (big | sig | volx) & p["liquid"].reindex_like(ret).fillna(False).astype(bool)
    if BENCHMARK in mask.columns:
        mask[BENCHMARK] = False
    c = p["close"]
    spy = c[BENCHMARK]
    gap = p["open"] / c.shift(1) - 1
    intraday = c / p["open"] - 1
    above = c > p["sma200"]
    def excess(n: int) -> pd.DataFrame:
        return (c.shift(-n) / c - 1).sub(spy.shift(-n) / spy - 1, axis=0)

    ex5, ex20, raw20 = excess(5), excess(20), c.shift(-20) / c - 1
    stacked = pd.DataFrame({
        "ret": ret[mask].stack(), "gap": gap[mask].stack(), "intraday": intraday[mask].stack(),
        "above": above[mask].stack(), "ex5": ex5[mask].stack(), "ex20": ex20[mask].stack(),
        "raw20": raw20[mask].stack(),
    }).dropna(subset=["ret", "ex5"])
    if stacked.empty:
        return {}
    stacked["direction"] = np.where(stacked["ret"] < 0, "跌", "涨")
    stacked["when"] = [timing(g, i) if not (pd.isna(g) or pd.isna(i)) else "—" for g, i in zip(stacked["gap"], stacked["intraday"])]
    stacked["trend"] = np.where(stacked["above"], "200 日线上方", "200 日线下方")
    out: dict[str, dict] = {}

    def summarize(sub: pd.DataFrame) -> dict:
        ex20_valid = sub["ex20"].dropna()
        return {
            "n": int(len(sub)),
            "ex5_mean": round(float(sub["ex5"].mean()), 5),
            "ex5_median": round(float(sub["ex5"].median()), 5),
            "ex20_mean": round(float(ex20_valid.mean()), 5) if len(ex20_valid) else None,
            "ex20_median": round(float(ex20_valid.median()), 5) if len(ex20_valid) else None,
            "ex20_up_share": round(float((ex20_valid > 0).mean()), 4) if len(ex20_valid) else None,
        }

    for direction in ("跌", "涨"):
        d = stacked[stacked["direction"] == direction]
        out[bucket_key(direction, "全部", "全部")] = summarize(d)
        for when in ("隔夜跳空", "盘中", "隔夜与盘中"):
            w = d[d["when"] == when]
            if len(w):
                out[bucket_key(direction, when, "全部")] = summarize(w)
                for trend in ("200 日线上方", "200 日线下方"):
                    t = w[w["trend"] == trend]
                    if len(t) >= 30:
                        out[bucket_key(direction, when, trend)] = summarize(t)
    return out


async def store_base_rates(rates: dict[str, dict]) -> None:
    await db.execute("DELETE FROM scout.event_base_rates")
    await db.execute_many(
        "INSERT INTO scout.event_base_rates (bucket, stats, computed_at) VALUES (:b, CAST(:s AS JSONB), now())",
        [{"b": k, "s": db.jsonb(v)} for k, v in rates.items()])


async def load_base_rates() -> dict[str, dict]:
    try:
        rows = await db.fetch_all("SELECT bucket, stats FROM scout.event_base_rates")
    except Exception:
        return {}
    return {r["bucket"]: r["stats"] for r in rows}


def base_rate_for(rates: dict[str, dict], ev: dict, above_200: bool | None) -> tuple[str, dict] | None:
    """Most specific bucket with data for this event."""
    direction = "跌" if ev["ret"] < 0 else "涨"
    trend = None if above_200 is None else ("200 日线上方" if above_200 else "200 日线下方")
    for key in ([bucket_key(direction, ev["timing"], trend)] if trend else []) + [
            bucket_key(direction, ev["timing"], "全部"), bucket_key(direction, "全部", "全部")]:
        if key in rates and rates[key].get("n", 0) >= 30:
            return key, rates[key]
    return None
