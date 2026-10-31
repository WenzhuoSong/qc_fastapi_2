"""Classic chart patterns detected as in Lo, Mamaysky & Wang (2000), on the local Sharadar files.

Pre-registered in `scout/knowledge/specs/chart_patterns_lmw.yaml` (family chart_patterns): rolling
38-day windows (l = 35, d = 3) of dividend-adjusted closes, Nadaraya–Watson Gaussian kernel with
0.3 × the leave-one-out cross-validated bandwidth, extrema where the smoothed curve turns (each
priced at the actual high / low close within a day of it), a pattern when the window's last
extremum falls on day l − 1; ten definitions (HS, IHS, BTOP, BBOT, TTOP, TBOT, RTOP, RBOT, DTOP,
DBOT) from the paper's Section II.A. Each detection is bought at the next open and held 21 sessions
against the mean of every other member over the same window. Pure computation, no database.
"""
from __future__ import annotations

import json
import warnings
from datetime import date

import numpy as np
import pandas as pd

from scout import gate, osap_check
from scout import sharadar_research as sr
from scout.sharadar_recheck import rebalance_days
from scout.shock_reversal import _window_returns

KEY = "chart_patterns_lmw"
FAMILY = "chart_patterns"
L, D = 35, 3
W = L + D
GRID = np.geomspace(0.5, 20.0, 25)           # trading days; h* is chosen per window on this grid
SHRINK = 0.3                                 # LMW: 0.3 × the cross-validated bandwidth
HS_TOL, RECT_TOL = 0.015, 0.0075
DOUBLE_GAP = 22
HOLD = 21
REPORTED_HOLDS = (1, 5, 63)
TARGET_HOLD = 63
VOL_RATIO = 1.2                              # LMW III.D, within the window here
SAMPLE_START = "1999-03-01"
TRIALS = 10
PATTERNS = ("HS", "IHS", "BTOP", "BBOT", "TTOP", "TBOT", "RTOP", "RBOT", "DTOP", "DBOT")
BOOK = {"HS": -1, "DTOP": -1, "BTOP": -1, "IHS": 1, "DBOT": 1}     # Edwards & Magee's direction; the rest open
ZH = {"HS": "头肩顶", "IHS": "头肩底", "BTOP": "扩散顶", "BBOT": "扩散底", "TTOP": "三角顶", "TBOT": "三角底",
      "RTOP": "矩形顶", "RBOT": "矩形底", "DTOP": "双顶", "DBOT": "双底"}
UNSEEN_FROM = "2016-01-01"
CRISIS = ("2008-01-01", "2009-12-31")


# ── detection ────────────────────────────────────────────────────────────────

def _kernel(h: float) -> np.ndarray:
    k = np.arange(W, dtype=float)
    return np.exp(-((k[:, None] - k[None, :]) ** 2) / (2 * h * h))


_K = [_kernel(h) for h in GRID]
_S = [(lambda K: K / K.sum(1, keepdims=True))(_kernel(SHRINK * h)) for h in GRID]


def smooth(P: np.ndarray) -> np.ndarray:
    """Kernel fits of windows P (n × W): per window the bandwidth minimising the leave-one-out
    error (LMW eq. 12–13) on GRID, then the fit with 0.3 × that bandwidth."""
    best = np.full(len(P), np.inf)
    pick = np.zeros(len(P), dtype=int)
    for g, K in enumerate(_K):
        den = K.sum(1) - 1.0                                   # K_tt = 1 is left out
        loo = (P @ K.T - P) / den
        err = ((P - loo) ** 2).mean(1)
        better = err < best
        best[better], pick[better] = err[better], g
    m = np.empty_like(P)
    for g in np.unique(pick):
        rows = pick == g
        m[rows] = P[rows] @ _S[g].T
    return m


def slope_signs(m: np.ndarray) -> np.ndarray:
    """Signs of m(k+1) − m(k) (n × W−1); a zero takes the previous non-zero sign (flat prices)."""
    s = np.sign(np.diff(m, axis=1))
    idx = np.where(s != 0, np.arange(s.shape[1]), 0)
    np.maximum.accumulate(idx, axis=1, out=idx)
    return np.take_along_axis(s, idx, axis=1)


def extrema(s: np.ndarray, p: np.ndarray) -> list[tuple[int, int, float]]:
    """(position, +1 max / −1 min, price) of one window: turns of the smoothed curve at k ∈ 1..W−2,
    priced at the highest / lowest actual price in [k−1, k+1]."""
    out = []
    for k in range(1, W - 1):
        a, b = s[k - 1], s[k]
        if a > 0 > b:
            out.append((k, 1, float(p[k - 1:k + 2].max())))
        elif a < 0 < b:
            out.append((k, -1, float(p[k - 1:k + 2].min())))
    return out


def _near(a: float, b: float, tol: float) -> bool:
    mid = (a + b) / 2
    return abs(a - mid) <= tol * mid and abs(b - mid) <= tol * mid


def _all_near(xs: list[float], tol: float) -> bool:
    mid = sum(xs) / len(xs)
    return all(abs(x - mid) <= tol * mid for x in xs)


def classify(ext: list[tuple[int, int, float]]) -> dict[str, dict]:
    """The patterns completed by this window (its last extremum is on day L − 1), with the
    levels the measured-move report needs: {pattern: {"height", "base"}}."""
    if not ext or ext[-1][0] != L - 1:
        return {}
    out: dict[str, dict] = {}
    if len(ext) >= 5:
        (_, t1, e1), (_, _, e2), (_, _, e3), (_, _, e4), (_, _, e5) = ext[-5:]
        top = t1 > 0
        if top and e3 > e1 and e3 > e5 and _near(e1, e5, HS_TOL) and _near(e2, e4, HS_TOL):
            out["HS"] = {"base": (e2 + e4) / 2, "height": e3 - (e2 + e4) / 2}
        if not top and e3 < e1 and e3 < e5 and _near(e1, e5, HS_TOL) and _near(e2, e4, HS_TOL):
            out["IHS"] = {"base": (e2 + e4) / 2, "height": (e2 + e4) / 2 - e3}
        if top and e1 < e3 < e5 and e2 > e4:
            out["BTOP"] = {}
        if not top and e1 > e3 > e5 and e2 < e4:
            out["BBOT"] = {}
        if top and e1 > e3 > e5 and e2 < e4:
            out["TTOP"] = {}
        if not top and e1 < e3 < e5 and e2 > e4:
            out["TBOT"] = {}
        tops, bots = ([e1, e3, e5], [e2, e4]) if top else ([e2, e4], [e1, e3, e5])
        if _all_near(tops, RECT_TOL) and _all_near(bots, RECT_TOL) and min(tops) > max(bots):
            out["RTOP" if top else "RBOT"] = {}
    k1, t1, e1 = ext[0]
    later = ext[1:]
    if t1 > 0:
        maxes = [x for x in later if x[1] > 0]
        if maxes:
            ka, _, ea = max(maxes, key=lambda x: x[2])
            if _near(e1, ea, HS_TOL) and ka - k1 > DOUBLE_GAP:
                low = min(x[2] for x in ext if x[1] < 0 and k1 < x[0] < ka)
                out["DTOP"] = {"base": low, "height": (e1 + ea) / 2 - low}
    else:
        mins = [x for x in later if x[1] < 0]
        if mins:
            kb, _, eb = min(mins, key=lambda x: x[2])
            if _near(e1, eb, HS_TOL) and kb - k1 > DOUBLE_GAP:
                high = max(x[2] for x in ext if x[1] > 0 and k1 < x[0] < kb)
                out["DBOT"] = {"base": high, "height": high - (e1 + eb) / 2}
    return out


def detect(close: np.ndarray, volume: np.ndarray, ok_day: np.ndarray) -> list[dict]:
    """Detections in one stock's series: {i (detection day = window end), pattern, levels, vol}.
    ok_day[i]: whether day i may be a detection day (member, inside the sample)."""
    if len(close) < W:
        return []
    win = np.lib.stride_tricks.sliding_window_view(close, W)
    ends = np.arange(W - 1, len(close))
    use = ok_day[ends] & np.isfinite(win).all(1)
    if not use.any():
        return []
    P, ends = win[use], ends[use]
    s = slope_signs(smooth(P))
    turn = (s[:, L - 2] != s[:, L - 1])                         # an extremum on day L − 1 (k = 34)
    out = []
    vol = np.lib.stride_tricks.sliding_window_view(volume, W)[use]
    for r in np.flatnonzero(turn):
        found = classify(extrema(s[r], P[r]))
        if not found:
            continue
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            v1, v2 = np.nanmean(vol[r, :W // 2]), np.nanmean(vol[r, W // 2:])
        trend = "up" if v2 > VOL_RATIO * v1 else "down" if v1 > VOL_RATIO * v2 else "flat"
        for name, lv in found.items():
            out.append({"i": int(ends[r]), "pattern": name, "vol": trend, **lv})
    return out


# ── the test ────────────────────────────────────────────────────────────────

def edge(rows: list[dict], hold: int = HOLD) -> dict:
    rs = [r for r in rows if r.get(f"ret{hold}") is not None]
    mean, t, months = gate.monthly_t([r["date"] for r in rs], [r[f"ret{hold}"] - r[f"ctl{hold}"] for r in rs])
    return {"edge": None if mean is None else round(mean, 5), "t": None if t is None else round(t, 2), "months": months,
            "n": len(rs), "hit": round(float(np.mean([r[f"ret{hold}"] > r[f"ctl{hold}"] for r in rs])), 3) if rs else None}


def verdict(name: str, s: dict, required: float) -> str:
    """pass / opposite (book direction reversed) / fail, from the full sample and both halves."""
    a, f, b = s["all"], s["first"], s["second"]
    if a["t"] is None or abs(a["t"]) < required or not f["edge"] or not b["edge"]:
        return "fail"
    sign = np.sign(a["t"])
    if np.sign(f["edge"]) != sign or np.sign(b["edge"]) != sign:
        return "fail"
    if name in BOOK and sign != BOOK[name]:
        return "opposite"
    return "pass"


def summarize(name: str, rows: list[dict], required: float) -> dict:
    rows = [r for r in rows if r.get(f"ret{HOLD}") is not None]
    if not rows:
        return {"n": 0, "verdict": "fail", "required_t": required}
    dates = sorted(r["date"] for r in rows)
    mid = dates[len(dates) // 2]
    out = {"n": len(rows), "first_date": str(dates[0].date()), "last_date": str(dates[-1].date()), "split": str(mid.date()),
           "all": edge(rows), "first": edge([r for r in rows if r["date"] < mid]),
           "second": edge([r for r in rows if r["date"] >= mid]), "required_t": required, "book": BOOK.get(name)}
    out["verdict"] = verdict(name, out, required)
    return out


def describe(rows: list[dict]) -> dict:
    """Reported only, no weight in the verdict."""
    crisis = lambda r: pd.Timestamp(CRISIS[0]) <= r["date"] <= pd.Timestamp(CRISIS[1])  # noqa: E731
    bear = set(m for a, b in osap_check.BEARS for m in pd.period_range(a, b, freq="M"))
    tg = [r for r in rows if r.get("target_hit") is not None]
    return {"holds": {h: edge(rows, h) for h in REPORTED_HOLDS},
            "volume": {v: edge([r for r in rows if r["vol"] == v]) for v in ("up", "flat", "down")},
            "since_2016": edge([r for r in rows if r["date"] >= pd.Timestamp(UNSEEN_FROM)]),
            "ex_crisis": edge([r for r in rows if not crisis(r)]),
            "bear": edge([r for r in rows if r["date"].to_period("M") in bear]),
            "target": ({"n": len(tg), "hit": round(float(np.mean([r["target_hit"] for r in tg])), 3),
                        "control": round(float(np.mean([r["target_ctl"] for r in tg])), 3)} if tg else None),
            "years": {y: sum(r["date"].year == y for r in rows) for y in sorted({r["date"].year for r in rows})}}


def _path_extremes(p: sr.Prices, i: int, cols: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(entry on the adjusted scale, lowest and highest adjusted close over TARGET_HOLD sessions) per column."""
    a, b = i + 1, min(i + TARGET_HOLD, len(p.dates) - 1)
    with np.errstate(all="ignore"):
        entry = p.closeadj[a, cols] * p.open[a, cols] / p.close[a, cols]
        seg = p.closeadj[a:b + 1][:, cols]
        return entry, np.nanmin(seg, 0), np.nanmax(seg, 0)


def run() -> dict:
    """Load the local data, detect, and compute the pre-registered test plus the reported splits."""
    full = sr.trading_days()
    grid = full[rebalance_days(full, hold=0)]
    elig = sr.eligible()
    uni = {d: v["large"] for d, v in sr.universe(grid, elig).items()}
    gdays = sorted(uni)
    names = sorted({t for v in uni.values() for t in v})
    dates = full[full >= gdays[0] - pd.Timedelta(days=90)]
    p = sr.prices(names, dates)
    gi = np.searchsorted(pd.DatetimeIndex(gdays), dates, side="right") - 1      # membership list in force each day
    start = int(np.searchsorted(dates, pd.Timestamp(SAMPLE_START)))
    last = len(dates) - 1 - max(TARGET_HOLD, HOLD)
    member = {t: np.zeros(len(dates), dtype=bool) for t in names}
    for k, g in enumerate(gdays):
        days = (gi == k)
        for t in uni[g]:
            member[t] |= days
    sector = elig["sector"].to_dict()

    found = []
    for t in names:
        j = p.col[t]
        ok = member[t].copy()
        ok[:start], ok[last + 1:] = False, False
        busy: dict[str, int] = {}
        for d in detect(p.closeadj[:, j], p.volume[:, j], ok):
            if busy.get(d["pattern"], -1) >= d["i"]:
                continue
            busy[d["pattern"]] = d["i"] + HOLD
            found.append({**d, "j": j, "ticker": t})

    cache: dict[tuple, dict[int, float]] = {}
    paths: dict[int, tuple] = {}
    rows = []
    for d in found:
        i, j = d["i"], d["j"]
        g = gdays[gi[i]]
        cols = [p.col[x] for x in uni[g] if x in p.col]
        r = {"date": dates[i], "ticker": d["ticker"], "pattern": d["pattern"], "vol": d["vol"], "sector": sector.get(d["ticker"])}
        for h in (HOLD, *REPORTED_HOLDS):
            key = (i, gi[i], h)
            if key not in cache:
                cache[key] = (_window_returns(p, i, cols, h) if h > 1 else
                              {c: float(p.closeadj[i + 1, c] / p.closeadj[i, c] - 1) for c in cols
                               if np.isfinite(p.closeadj[i, c]) and np.isfinite(p.closeadj[i + 1, c]) and p.closeadj[i, c] > 0})
            w = cache[key]
            if j in w and len(w) > 1:
                r[f"ret{h}"], r[f"ctl{h}"] = w[j], (sum(w.values()) - w[j]) / (len(w) - 1)
            else:
                r[f"ret{h}"] = r[f"ctl{h}"] = None
        if d.get("height"):                       # ch. 32 measured move, reported only
            if i not in paths:
                c = np.array(cols)
                paths[i] = (c, *_path_extremes(p, i, c))
            c, entry, lo, hi = paths[i]
            k = int(np.flatnonzero(c == j)[0]) if (c == j).any() else None
            down = d["pattern"] in ("HS", "DTOP")
            target = d["base"] - d["height"] if down else d["base"] + d["height"]
            if k is not None and np.isfinite(entry[k]) and entry[k] > 0:
                move = target / entry[k] - 1
                if (move < 0) == down:            # the target still lies ahead of the entry
                    others = np.delete(np.arange(len(c)), k)
                    with np.errstate(all="ignore"):
                        reach = (lo[others] / entry[others] - 1 <= move) if down else (hi[others] / entry[others] - 1 >= move)
                        good = np.isfinite(entry[others]) & (entry[others] > 0)
                    r["target_hit"] = bool((lo[k] <= target) if down else (hi[k] >= target))
                    r["target_ctl"] = float(reach[good].mean()) if good.any() else None
                    if r["target_ctl"] is None:
                        r["target_hit"] = None
        rows.append(r)

    req = gate.required_t(TRIALS)
    by = {n: [r for r in rows if r["pattern"] == n] for n in PATTERNS}
    return {"key": KEY, "trials": TRIALS, "asof": str(date.today()), "stocks": len(names),
            "patterns": {n: {**summarize(n, v, req), "detail": describe(v)} for n, v in by.items()}}


def report_text(res: dict) -> str:
    p = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731

    def e(x: dict) -> str:
        hit = "—" if x["hit"] is None else f"{x['hit']:.0%}"
        return f"{p(x['edge'])}（t {x['t']}，{x['n']} 次，胜率 {hit}）"

    res = json.loads(json.dumps(res, default=str))
    word = {"pass": "通过", "fail": "未通过", "opposite": "与书相反（未通过）"}
    lines = [f"检验关（Sharadar 点时市值前 1000）：{res['key']}，本族十项同时检验，|t| 门槛 "
             f"{next(iter(res['patterns'].values()))['required_t']}；持有 21 日，对照 = 同窗口股票池其他股票平均"]
    for n, s in res["patterns"].items():
        if not s.get("n"):
            lines.append(f"{ZH[n]}（{n}）：无样本")
            continue
        book = {1: "书：看涨", -1: "书：看跌"}.get(s.get("book"), "书：方向不定")
        d = s["detail"]
        lines.append(f"{ZH[n]}（{n}，{book}）→ {word[s['verdict']]}：全部 {e(s['all'])}；前半 {p(s['first']['edge'])}"
                     f"（t {s['first']['t']}）/ 后半 {p(s['second']['edge'])}（t {s['second']['t']}），按 {s['split']} 分")
        lines.append("    仅报告：" + "；".join(f"{h} 日 {e(d['holds'][str(h)])}" for h in REPORTED_HOLDS)
                     + f"；放量 {p(d['volume']['up']['edge'])}（{d['volume']['up']['n']}）/ 缩量 {p(d['volume']['down']['edge'])}"
                     f"（{d['volume']['down']['n']}）；2016 起 {p(d['since_2016']['edge'])}（t {d['since_2016']['t']}）；"
                     f"去掉 2008–09 {p(d['ex_crisis']['edge'])}；熊市月份 {p(d['bear']['edge'])}"
                     + (f"；量度目标 63 日内达到 {d['target']['hit']:.0%}（同窗口其他股票达到同样幅度 {d['target']['control']:.0%}，"
                        f"{d['target']['n']} 次）" if d.get("target") else ""))
    return "\n".join(lines)
