"""Holding-horizon profile of the strategies that passed the gate (descriptive only).

For each horizon H, every pick or signal is held from its fill to the close H sessions later,
against same-day other stocks bought the same way (ranking: the mean of every other stock of
the pool; rule: up to `gate.DRAWS` random other stocks with the same entry formula that fill).
Edge, t on monthly means and both halves per horizon show how fast an edge arrives and when
it fades. Nothing here changes a verdict or picks a holding period: the holding-cap study
(`scout/knowledge/exit_specs/hold_cap.yaml`) was registered before this was run.
"""
from __future__ import annotations

import zlib

import numpy as np
import pandas as pd

from scout import base_rates, gate, rankers
from scout.detectors import Detector

HORIZONS = (5, 10, 21, 63, 126, 252)


def _fill(p, det: Detector, ticker: str, i: int) -> tuple[int, float] | None:
    """The rule's entry after the signal on day i: (bar index, price), as `scout.simulate` fills."""
    try:
        lv = det.levels(p.row(ticker, p.dates[i]))
    except KeyError:
        return None
    if not lv.valid():
        return None
    for b in base_rates._bars(p, ticker, i + 1, i + 1 + det.trigger_window):
        if b.low <= lv.entry_hi and b.high >= lv.entry_lo:
            return p.dates.get_loc(b.date), float(min(max(b.open, lv.entry_lo), lv.entry_hi))
    return None


def rule_rows(p, det: Detector) -> list[dict]:
    """Per signal and horizon: return from the fill and the mean of its same-day controls."""
    close = p["close"]
    m = gate.Market(p)
    n = len(p.dates)
    rng = np.random.default_rng([gate.SEED, zlib.crc32(("horizon:" + det.name).encode())])
    rows = []
    for ticker, i in base_rates.hit_positions(det.mask(p)):
        got = _fill(p, det, ticker, i)
        if not got or ticker not in m.col:
            continue
        names = [m.stocks[k] for k in np.flatnonzero(m.valid[i]) if m.stocks[k] != ticker]
        legs = []
        for _ in range(gate.MAX_TRIES * gate.DRAWS):
            if len(legs) == gate.DRAWS or not names:
                break
            leg = names[int(rng.integers(len(names)))]
            f = _fill(p, det, leg, i)
            if f:
                legs.append((leg, f))
        a, px = got
        for h in HORIZONS:
            if a + h - 1 >= n:
                continue
            r = close[ticker].iloc[a + h - 1] / px - 1
            ctl = [close[t].iloc[b + h - 1] / q - 1 for t, (b, q) in legs if b + h - 1 < n]
            ctl = [x for x in ctl if np.isfinite(x)]
            if np.isfinite(r) and ctl:
                rows.append({"h": h, "date": p.dates[i], "ret": float(r), "control": float(np.mean(ctl))})
    return rows


def rank_rows(p, spec: dict, angle: dict) -> list[dict]:
    """Per pick and horizon: next open to the close H sessions later, vs every other stock of the pool."""
    a, stocks = rankers.arrays(p)
    a["angle"] = angle
    fn, frac = rankers.RANKERS[spec["ranker"]], float(spec.get("top_frac", 0.1))
    o, c, n = a["open"], a["close"], len(p.dates)
    rows = []
    for i in rank_days(n, spec):
        uni = rankers._universe(a, i)
        picks = fn(a, i, frac)
        for h in HORIZONS:
            if i + h >= n:
                continue
            r = c[i + h, uni] / o[i + 1, uni] - 1
            ok = np.isfinite(r)
            rets = dict(zip(uni[ok].tolist(), r[ok].tolist()))
            total, cnt = sum(rets.values()), len(rets)
            for j in picks:
                if j in rets and cnt > 1:
                    rows.append({"h": h, "date": p.dates[i], "ret": rets[j], "control": (total - rets[j]) / (cnt - 1)})
    return rows


def rank_days(n: int, spec: dict) -> list[int]:
    return list(range(rankers.YEAR + rankers.MONTH, n - 1, int(spec["every"])))


def nw_t(dates: list, diffs: list[float], h: int) -> float | None:
    """t of the mean of monthly means with Newey-West errors over the months a holding overlaps
    (ceil(h / 21) − 1 lags): monthly samples of long holdings share most of their days."""
    s = pd.Series(diffs, index=pd.to_datetime(dates))
    m = s.groupby(s.index.to_period("M")).mean().to_numpy()
    if len(m) < 6:
        return None
    lags = max(0, int(np.ceil(h / 21)) - 1)
    x = m - m.mean()
    var = x @ x / len(x)
    for lag in range(1, min(lags, len(x) - 1) + 1):
        var += 2 * (1 - lag / (lags + 1)) * (x[lag:] @ x[:-lag]) / len(x)
    return None if var <= 0 else round(float(m.mean() / np.sqrt(var / len(m))), 2)


def profile(rows: list[dict]) -> dict:
    """Per horizon: n, mean return, edge vs control (full, first, second half; t on monthly means).
    The halves split at the median signal date of the shortest horizon, the same for every horizon."""
    if not rows:
        return {}
    first = sorted(r["date"] for r in rows if r["h"] == HORIZONS[0])
    split = first[len(first) // 2]
    out = {"split": str(split), "horizons": {}}
    for h in HORIZONS:
        rs = [r for r in rows if r["h"] == h]
        if not rs:
            continue
        row = {"n": len(rs), "mean": round(float(np.mean([r["ret"] for r in rs])), 5)}
        for name, part in (("all", rs), ("first", [r for r in rs if r["date"] < split]),
                           ("second", [r for r in rs if r["date"] >= split])):
            mean, t, months = gate.monthly_t([r["date"] for r in part], [r["ret"] - r["control"] for r in part])
            row[name] = {"edge": None if mean is None else round(mean, 5), "t": None if t is None else round(t, 2), "months": months,
                         "t_nw": nw_t([r["date"] for r in part], [r["ret"] - r["control"] for r in part], h) if part else None}
        out["horizons"][h] = row
    return out


def report_text(name: str, prof: dict) -> str:
    pct = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731
    lines = [f"{name}（仅描述，不用来挑持有期；前后两半按 {prof.get('split')} 分）",
             "持有交易日 | 笔数 | 平均收益 | 相对同日其他股票 全部（t / 重叠校正 t） | 前半（t） | 后半（t） | 折合每 21 日"]
    for h, v in prof.get("horizons", {}).items():
        e = v["all"]["edge"]
        lines.append(f"{h} | {v['n']} | {pct(v['mean'])} | {pct(e)}（{v['all']['t']} / {v['all']['t_nw']}） | "
                     f"{pct(v['first']['edge'])}（{v['first']['t']}） | {pct(v['second']['edge'])}（{v['second']['t']}） | "
                     f"{pct(None if e is None else e * 21 / int(h))}")
    return "\n".join(lines)

