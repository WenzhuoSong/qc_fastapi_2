"""Historical base rates: replay every detector over the stored daily history.

Known bias: the scan universe uses *current* index members, so the replay has
survivorship bias and reads optimistic. Cards say so.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from scout import db
from scout.detectors import Detector, registry
from scout.features import FeaturePanel
from scout.simulate import ROUND_TRIP_COST, Bar, simulate
from scout.universe import BENCHMARK

logger = logging.getLogger("scout.base_rates")

COOLDOWN_SESSIONS = 10   # one sample per ticker × detector per 10 sessions
MIN_SAMPLES = 20         # below this, a regime bucket falls back to "all"
SHRINK_K = 50            # edge shrinkage: edge * n / (n + K)


@dataclass
class Sample:
    detector: str
    ticker: str
    signal_date: date
    regime: str
    state: str
    ret: float | None          # gross trade return
    spy_ret: float | None      # SPY over the same entry→exit window
    hold_ret: float | None     # holding this stock for the same number of days at its average drift
    mfe: float | None
    mae: float | None
    days: int
    r_multiple: float | None

    @property
    def net_ret(self) -> float | None:
        return None if self.ret is None else self.ret - ROUND_TRIP_COST


def hit_positions(mask: pd.DataFrame, cooldown: int = COOLDOWN_SESSIONS) -> list[tuple[str, int]]:
    out = []
    arr = mask.fillna(False).to_numpy(dtype=bool)
    for j, ticker in enumerate(mask.columns):
        last = -10**9
        for i in np.flatnonzero(arr[:, j]):
            if i - last >= cooldown:
                out.append((ticker, int(i)))
                last = i
    return out


def _bars(p: FeaturePanel, ticker: str, start: int, end: int) -> list[Bar]:
    o, h, l, c = (p[k][ticker].to_numpy() for k in ("open", "high", "low", "close"))
    dates = p.dates
    return [
        Bar(dates[i], o[i], h[i], l[i], c[i])
        for i in range(start, min(end, len(dates)))
        if not np.isnan(c[i])
    ]


def daily_drift(p: FeaturePanel) -> dict[str, float]:
    """Each ticker's average daily log return over the panel: the 'just hold it'
    baseline. A rule only has an edge if it beats holding the same stock for
    the same number of days, after costs."""
    logret = np.log(p["close"] / p["close"].shift(1))
    return {t: float(v) for t, v in logret.mean().items() if np.isfinite(v)}


def replay_detector(p: FeaturePanel, det: Detector, drift: dict[str, float] | None = None) -> list[Sample]:
    mask = det.mask(p)
    drift = drift if drift is not None else daily_drift(p)
    samples: list[Sample] = []
    n_dates = len(p.dates)
    span = 3 + det.horizon_days            # trigger window + holding horizon
    spy_open, spy_close = p["open"][BENCHMARK].to_numpy(), p["close"][BENCHMARK].to_numpy()
    for ticker, i in hit_positions(mask):
        if i + span >= n_dates:
            continue                         # outcome not yet observable
        d = p.dates[i]
        lv = det.levels(p.row(ticker, d))
        if not lv.valid():
            continue
        res = simulate(_bars(p, ticker, i + 1, i + 1 + span), lv, det.horizon_days)
        spy_ret = None
        if res.entry_date is not None and res.exit_date is not None:
            a = p.dates.get_loc(res.entry_date)
            b = p.dates.get_loc(res.exit_date)
            spy_ret = float(spy_close[b] / spy_open[a] - 1)
        risk = lv.entry_mid - lv.stop
        ret = res.return_pct
        hold_ret = None
        if ret is not None and ticker in drift:
            hold_ret = float(np.exp(drift[ticker] * max(res.days_held, 1)) - 1)
        samples.append(Sample(
            detector=det.name, ticker=ticker, signal_date=d, regime=str(p.regime.iloc[i]),
            state=res.state, ret=ret, spy_ret=spy_ret, hold_ret=hold_ret, mfe=res.mfe, mae=res.mae,
            days=res.days_held,
            r_multiple=(ret * res.entry_price / risk) if (ret is not None and risk > 0) else None,
        ))
    return samples


def summarize(samples: list[Sample]) -> dict:
    n_signals = len(samples)
    traded = [s for s in samples if s.ret is not None]
    if not traded:
        return {"n_signals": n_signals, "n": 0}
    ret = np.array([s.ret for s in traded])
    net = ret - ROUND_TRIP_COST
    excess = np.array([s.ret - s.spy_ret for s in traded if s.spy_ret is not None])
    vs_hold = np.array([s.net_ret - s.hold_ret for s in traded if s.hold_ret is not None])
    hold = np.array([s.hold_ret for s in traded if s.hold_ret is not None])
    r_mult = np.array([s.r_multiple for s in traded if s.r_multiple is not None])
    return {
        "n_signals": n_signals,
        "n": len(traded),
        "trigger_rate": round(len(traded) / n_signals, 4),
        "win_rate": round(float((ret > 0).mean()), 4),
        "target_rate": round(sum(s.state == "hit_target" for s in traded) / len(traded), 4),
        "stop_rate": round(sum(s.state == "stopped" for s in traded) / len(traded), 4),
        "avg_return": round(float(ret.mean()), 5),
        "median_return": round(float(np.median(ret)), 5),
        "avg_excess": round(float(excess.mean()), 5) if excess.size else None,
        "cost_bps": round(ROUND_TRIP_COST * 1e4, 1),
        "win_rate_net": round(float((net > 0).mean()), 4),
        "avg_net_return": round(float(net.mean()), 5),
        "avg_net_excess": round(float(excess.mean() - ROUND_TRIP_COST), 5) if excess.size else None,
        "avg_hold_return": round(float(hold.mean()), 5) if hold.size else None,
        "avg_edge_vs_hold": round(float(vs_hold.mean()), 5) if vs_hold.size else None,
        "beat_hold_rate": round(float((vs_hold > 0).mean()), 4) if vs_hold.size else None,
        "avg_r": round(float(r_mult.mean()), 3) if r_mult.size else None,
        "avg_mfe": round(float(np.mean([s.mfe for s in traded])), 5),
        "avg_mae": round(float(np.mean([s.mae for s in traded])), 5),
        "avg_days": round(float(np.mean([s.days for s in traded])), 2),
        "first_signal": min(s.signal_date for s in samples).isoformat(),
        "last_signal": max(s.signal_date for s in samples).isoformat(),
    }


def compute(p: FeaturePanel, detectors: list[Detector] | None = None) -> dict[str, dict[str, dict]]:
    out: dict[str, dict[str, dict]] = {}
    drift = daily_drift(p)
    for det in detectors or registry():
        samples = replay_detector(p, det, drift)
        buckets = {"all": summarize(samples)}
        for regime in ("uptrend", "mixed", "downtrend"):
            buckets[regime] = summarize([s for s in samples if s.regime == regime])
        out[det.name] = buckets
        logger.info("base rate %s: %s", det.name, buckets["all"])
    return out


async def store(rates: dict[str, dict[str, dict]]) -> None:
    rows = [
        {"d": det, "r": regime, "n": stats.get("n", 0), "s": db.jsonb(stats)}
        for det, buckets in rates.items()
        for regime, stats in buckets.items()
    ]
    await db.execute_many(
        """
        INSERT INTO scout.detector_base_rates (detector, regime, n, stats, computed_at)
        VALUES (:d, :r, :n, CAST(:s AS JSONB), now())
        ON CONFLICT (detector, regime) DO UPDATE SET
            n = EXCLUDED.n, stats = EXCLUDED.stats, computed_at = now()
        """,
        rows,
    )


async def load() -> dict[str, dict[str, dict]]:
    rows = await db.fetch_all("SELECT detector, regime, stats FROM scout.detector_base_rates")
    out: dict[str, dict[str, dict]] = {}
    for r in rows:
        out.setdefault(r["detector"], {})[r["regime"]] = r["stats"]
    return out


def pick(rates: dict[str, dict[str, dict]], detector: str, regime: str) -> dict:
    """Regime bucket when it has enough samples, else the all-regime bucket."""
    buckets = rates.get(detector) or {}
    chosen = buckets.get(regime) or {}
    if chosen.get("n", 0) >= MIN_SAMPLES:
        return {**chosen, "regime": regime}
    return {**(buckets.get("all") or {}), "regime": "all"}


def edge_of(stats: dict) -> float | None:
    """Net-of-cost edge over just holding the stock; older rows only have gross excess vs SPY."""
    edge = stats.get("avg_edge_vs_hold")
    return edge if edge is not None else stats.get("avg_excess")


def shrunk_edge(stats: dict) -> float:
    n = stats.get("n") or 0
    edge = edge_of(stats)
    if edge is None or n == 0:
        return 0.0
    return edge * n / (n + SHRINK_K)
