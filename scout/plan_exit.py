"""The holdings plan's exit, replayed for any buy (the gate's `exit: plan`).

A buy at a known fill gets the plan a recorded trade gets (`levels.locked_plan` on the price
map of the last close before the fill, point in time) and is managed the way
`portfolio.manage_trade` manages it: the initial stop until target 1 trades, then the
chandelier stop (highest close since the buy − 3 ATR, only rising), no partial sells, at most
`levels.HOLD_SESSIONS` sessions (`exit_study` policy `chandelier_all_t1`, its daily-bar
conventions). A window shorter than that sells the rest at the window's last close; money
that left earlier earns nothing until the window ends, so every buy covers the same window.
"""
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor

import numpy as np

from scout import exit_study, levels, structure

POLICY = next(p for p in exit_study.POLICIES if p.name == "chandelier_all_t1")
FEATURES = ("close", "atr14", "sma20", "sma50", "sma200", "dist_52w_high")
ARRAYS = ("open", "high", "low", "close", "volume", *FEATURES[1:])
WARMUP = 30                    # bars of history the structure needs


def cap(hold: int) -> int:
    """Sessions a plan may hold within a window of `hold` sessions."""
    return min(int(hold), levels.HOLD_SESSIONS)


def stock_arrays(p, ticker: str) -> dict:
    a = {k: p[k][ticker].to_numpy(dtype=float) for k in ARRAYS}
    a["dates"] = list(p.dates)
    return a


def plan_for(a: dict, i: int, fill: float) -> dict | None:
    """The plan for a fill after the close of bar i, from bars up to i only."""
    if not (np.isfinite(a["close"][i]) and np.isfinite(a["atr14"][i])):
        return None
    f = {k: float(a[k][i]) for k in FEATURES if np.isfinite(a[k][i])}
    lo = max(0, i - structure.LOOKBACK + 1)
    hist = [{"date": a["dates"][k], "open": a["open"][k], "high": a["high"][k], "low": a["low"][k], "close": a["close"][k],
             "volume": a["volume"][k]} for k in range(lo, i + 1) if np.isfinite(a["close"][k])]
    pm = levels.price_map(f, structure.compute(hist))
    plan = levels.locked_plan(pm, fill, a["dates"][i]) if pm else None
    return plan if plan and plan["stop"] < fill < plan["target1"] else None


def outcome(a: dict, start: int, fill: float, sessions: int) -> tuple[float, int] | None:
    """(gross return, sessions held) of a fill on bar `start` managed by the plan for at most `sessions`
    sessions (counting the fill bar); None without a plan."""
    if start < WARMUP or start + sessions > len(a["close"]):
        return None
    plan = plan_for(a, start - 1, fill)
    if not plan:
        return None
    path = exit_study.Path(a["open"], a["high"], a["low"], a["close"], a["atr14"], a["sma20"], a["sma50"], start)
    return exit_study.manage(path, fill, plan["stop"], plan["target1"], max(plan["target2"], plan["target1"]), POLICY,
                             max_hold=sessions)


def buy_next_open(a: dict, i: int, hold: int) -> float | None:
    """Gross return over a window of `hold` sessions of a buy at the open after day i."""
    s = i + 1
    if s >= len(a["open"]) or not np.isfinite(a["open"][s]) or a["open"][s] <= 0:
        return None
    got = outcome(a, s, float(a["open"][s]), cap(hold))
    return None if got is None else got[0]


def _stock_matrix(args: tuple[dict, list[int], tuple[int, ...]]) -> dict[int, np.ndarray]:
    """Per window length: the return of a next-open buy on each of `days` (NaN without a plan).
    One managed path per day; shorter windows are read off it (no partial sells, so the
    position is whole until it leaves)."""
    a, days, holds = args
    n, longest = len(a["close"]), max(cap(h) for h in holds)
    out = {h: np.full(n, np.nan) for h in holds}
    for i in days:
        s = i + 1
        if s >= n or not np.isfinite(a["open"][s]) or a["open"][s] <= 0:
            continue
        fill = float(a["open"][s])
        got = outcome(a, s, fill, min(longest, n - s))
        if got is None:
            continue
        ret, held = got
        for h in holds:
            c = cap(h)
            if s + c > n:
                continue
            if held <= c:
                out[h][i] = ret                         # left inside the window (a path ending later held on past it)
            elif np.isfinite(a["close"][s + c - 1]):
                out[h][i] = a["close"][s + c - 1] / fill - 1
    return out


def matrix(p, stocks: list[str], days: list[int], holds: tuple[int, ...], workers: int | None = None) -> dict[int, np.ndarray]:
    """{hold: array (dates × stocks)} of next-open buys managed by the plan, for every stock on `days`."""
    assert POLICY.scale == (), "windows are read off one path only without partial sells"
    jobs = [(stock_arrays(p, t), days, holds) for t in stocks]
    with ProcessPoolExecutor(max_workers=workers) as ex:
        cols = list(ex.map(_stock_matrix, jobs, chunksize=4))
    return {h: np.column_stack([c[h] for c in cols]) if cols else np.empty((len(p.dates), 0)) for h in holds}
