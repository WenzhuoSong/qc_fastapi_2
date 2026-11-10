"""The playbooks' history v4, pre-registered in `scout/knowledge/external_specs/playbook_history_v4.yaml` (one run,
local only: `python -m scout.jobs.playbook_history_v4`).

v3's window and protocols, with the v3 configurations the review found degenerate or incomplete replaced: the turtles'
and Elder's risk sizing scaled to fit an unleveraged stock account (the turtles' own 4 / 6 / 12 unit limits), insider
buying and earnings drift traded in event time with the papers' holding periods, the breadth thrust held 12 months,
CAN SLIM with a flat-base entry and O'Neil's sell rules, a 1/50-per-signal portfolio for the signal playbooks whose
20 slots stayed full, Sharadar's full fields for the playbooks Scout approximates live, and a trade-level record for
the signal playbooks (every trade, no portfolio limits). Descriptive: no knowledge-base status changes.
"""
from __future__ import annotations

import logging
import math
from dataclasses import replace

import numpy as np
import pandas as pd

from scout import playbook_history as ph
from scout import playbook_history_v3 as v3

logger = logging.getLogger("scout.playbook_history")

KEY = "playbook_history_v4"
COST, COST_HIGH = ph.COST, ph.COST_HIGH
FRAC = 1 / 50                                   # signal portfolio: each signal 1/50 of the account
SIGNAL = ("wilcox", "golden_cross", "macd", "wyckoff", "chart_patterns", "livermore")
SIGNAL_STOP = {"livermore": "low20"}           # v3's stops kept
DATA_FULL = {"magic_formula": ("annual_rebuild", 30), "piotroski": ("annual_rebuild", None),
             "trending_value": ("annual_rebuild", 25), "graham": ("annual_review", 30), "lynch": ("quarterly_review", 20)}
CHANGED = ("turtle_s1", "turtle_s2", "elder", "insider", "breadth_thrust", "can_slim", "pead", *SIGNAL, *DATA_FULL)
# trade-level record: the signal playbooks (growth, technical, events), not the screens and rankers
TRADE_LEVEL = ("can_slim", "minervini", "darvas", "weinstein", "turtle_s1", "turtle_s2", "elder", "connors_rsi2",
               *SIGNAL, "insider", "pead")
SMALL = ("magic_formula", "piotroski", "trending_value", "lynch", "can_slim")
TURTLE_UNITS, TURTLE_SECTOR_UNITS, TURTLE_STOCK_UNITS = 12, 6, 4
ELDER_MAX_RISK, ELDER_TYPICAL, ELDER_CAP, ELDER_MONTH = 0.02, 20, 0.10, 3
INSIDER_HOLD, PEAD_HOLD, PEAD_LOOK, PEAD_MIN_EVENTS = 126, 60, 63, 20
THRUST_HOLD = 252
BASE, BASE_DEPTH, BASE_CHASE, BASE_VOL, VOL_AVG = 25, 0.15, 0.05, 1.4, 50
TARGET, FAST, EIGHT_WEEKS = 1.20, 15, 40
INST_LAG_DAYS = 45                              # a quarter's 13F holdings are public 45 days after its end

PROTOCOL_ZH = {
    "turtle_s1": "原系统的 4 / 6 / 12 单位上限，单位风险按中位波动一个单位约 1/12 净值换算",
    "turtle_s2": "原系统的 4 / 6 / 12 单位上限，单位风险按中位波动一个单位约 1/12 净值换算",
    "elder": "2% 为上限、典型仓位约 1/20，单只 ≤ 1/10，6% 规则按比例",
    "insider": "首次出现 ≥ 2 位买入者为信号，每个信号 1/50，持有 6 个月",
    "breadth_thrust": "信号后持有 SPY 12 个月，其余现金",
    "can_slim": "平台底部放量突破买入；8% 止损、枢轴 + 20% 止盈（8 周规则）、放量跌破 50 日线卖出；最多 8 只",
    "pead": "按公告时间：反应前 10%，次日开盘买入，每个信号 1/50，持有 60 个交易日",
    **dict.fromkeys(SIGNAL, "每个信号 1/50（不限只数），卖出同 v3"),
    **{k: "同 v3 的批次，Sharadar 完整字段的原版公式" for k in DATA_FULL},
}


# ── accounts ──

class Book(v3.Book):
    """v3's account plus a trade log (one row per position, opened → fully closed). unlimited: every buy is filled at
    its size whatever the cash (the trade-level record; the account value means nothing then)."""

    def __init__(self, H, cash, cost, unlimited: bool = False):
        super().__init__(H, cash, cost)
        self.unlimited = unlimited
        self.opened: dict[int, int] = {}
        self.log: list[tuple] = []                  # (column, entry session, exit session, basis, proceeds)

    def afford(self, usd: float, c: float) -> bool:
        return self.unlimited or self.cash >= usd * (1 + c) - 1e-12

    def buy(self, d, j, usd, price) -> bool:
        if not self.unlimited:
            ok = super().buy(d, j, usd, price)
        else:
            c = self.cost(d, j)
            ok = c is not None and np.isfinite(price) and price > 0 and usd > 0
            if ok:
                self.pos[j] = self.pos.get(j, 0.0) + usd / price
                self.basis[j] = self.basis.get(j, 0.0) + usd * (1 + c)
                self.cash -= usd * (1 + c)
                self.trades += 1
        if ok:
            self.opened.setdefault(j, d)
        return ok

    def sell(self, d, j, price, shares=None) -> float:
        n, basis = self.pos.get(j, 0.0), self.basis.get(j, 0.0)
        pnl = super().sell(d, j, price, shares)
        if n > 0 and j not in self.pos:
            self.log.append((j, self.opened.pop(j, d), d, basis, basis + pnl))
        return pnl


def _live(ver, d: int) -> set[int]:
    x = ver.get(d)
    return set(x["stocks"].tolist()) if x and x["stocks"] is not None else set()


def _out(r: dict, b: Book, held: list, extra: dict | None = None) -> dict:
    return {**r, "trades": b.trades, "avg_positions": float(np.mean(held)) if held else float("nan"), "log": b.log, **(extra or {})}


def _median_ratio(num: np.ndarray, den: np.ndarray, cols: list[int]) -> float:
    if not cols:
        return float("nan")
    x = num[cols] / den[cols]
    x = x[np.isfinite(x) & (x > 0)]
    return float(np.median(x)) if x.size else float("nan")


# ── turtles ──

def sim_turtle(H, ver, s, cost, system: int, fill: str = "stop", pre: dict | None = None, tape: bool = False) -> dict:
    """v3's long-only turtles with the original unit limits (4 a stock, 6 a sector, 12 in all) and a unit risk r_t that
    makes one unit of a median-volatility stock about 1/12 of the account: r_t = median(N / close) ÷ 12 over the
    liquid pool the evening before. tape: every breakout traded, no portfolio limits (per-stock 4 units kept)."""
    pre = pre or {}
    n_in, n_out = (20, 10) if system == 1 else (55, 20)
    N, hi_in, lo_out, hi55 = pre["N"], pre[f"hi{n_in}"], pre[f"lo{n_out}"], pre["hi55"]
    b = Book(H, 1.0, cost, unlimited=tape)
    st: dict[int, dict] = {}
    ns = H.n_stocks
    sector = [H.sectors.get(H.names[j]) or f"_{j}" for j in range(ns)]
    vin = np.zeros(ns, bool)
    ventry, vstop, vwin = np.zeros(ns), np.zeros(ns), np.zeros(ns, bool)
    held = []
    stop_mode = fill == "stop"

    def units() -> tuple[int, dict]:
        per: dict[str, int] = {}
        for j, x in st.items():
            per[sector[j]] = per.get(sector[j], 0) + x["units"]
        return sum(x["units"] for x in st.values()), per

    def room(j: int) -> bool:
        if tape:
            return True
        tot, per = units()
        return tot < TURTLE_UNITS and per.get(sector[j], 0) < TURTLE_SECTOR_UNITS

    def virtual(d):
        nonlocal vin, vwin
        lo, o = H.l[d, :ns], H.o[d, :ns]
        trig = np.fmax(vstop, lo_out[d, :ns])
        if stop_mode:
            out = vin & np.isfinite(lo) & (lo <= trig)
            px = np.where(np.isfinite(o), np.fmin(o, trig), trig)
        else:
            out = vin & np.isfinite(H.c[d - 1, :ns]) & (H.c[d - 1, :ns] < np.fmax(vstop, lo_out[d - 1, :ns]))
            px = o
        vwin = np.where(out, px > ventry, vwin)
        vin = vin & ~out

    def new_virtual(d):
        nonlocal vin, ventry, vstop
        if stop_mode:
            lvl = hi_in[d, :ns]
            go = ~vin & np.isfinite(lvl) & np.isfinite(H.h[d, :ns]) & (H.h[d, :ns] >= lvl)
            px = np.fmax(np.nan_to_num(H.o[d, :ns], nan=-np.inf), lvl)
        else:
            go = ~vin & np.isfinite(hi_in[d - 1, :ns]) & (H.c[d - 1, :ns] > hi_in[d - 1, :ns]) & np.isfinite(H.o[d, :ns])
            px = H.o[d, :ns]
        nn = N[d - 1, :ns]
        ventry = np.where(go, px, ventry)
        vstop = np.where(go, px - 2 * nn, vstop)
        vin = vin | (go & np.isfinite(nn))

    def step(d, nav):
        eq = 1.0 if tape else nav[d - 1]
        o = H.o[d]
        live = [j for j in _live(ver, d - 1) if j < ns]
        r_t = _median_ratio(N[d - 1], H.c[d - 1], live) / TURTLE_UNITS
        for j in list(st):
            x = st[j]
            if x["day"] >= d:
                continue
            if stop_mode:
                trig = max(x["stop"], lo_out[d, j]) if np.isfinite(lo_out[d, j]) else x["stop"]
                if np.isfinite(H.l[d, j]) and H.l[d, j] <= trig:
                    b.sell(d, j, min(o[j], trig) if np.isfinite(o[j]) else trig)
                    st.pop(j)
            else:
                trig = max(x["stop"], lo_out[d - 1, j]) if np.isfinite(lo_out[d - 1, j]) else x["stop"]
                if np.isfinite(H.c[d - 1, j]) and H.c[d - 1, j] < trig:
                    b.sell(d, j, o[j])
                    st.pop(j)
        for j in list(st):
            x = st[j]
            if x["units"] >= TURTLE_STOCK_UNITS or x["day"] >= d or not room(j):
                continue
            lvl = x["last"] + 0.5 * x["n"]
            hit = (np.isfinite(H.h[d, j]) and H.h[d, j] >= lvl) if stop_mode else (np.isfinite(H.c[d - 1, j]) and H.c[d - 1, j] >= lvl)
            if hit:
                px = (max(o[j], lvl) if np.isfinite(o[j]) else lvl) if stop_mode else o[j]
                nn = N[d - 1, j]
                if not (np.isfinite(px) and np.isfinite(nn) and nn > 0 and np.isfinite(r_t)):
                    continue
                usd = r_t * eq / nn * px
                c = b.cost(d, j)
                if c is not None and b.afford(usd, c) and b.buy(d, j, usd, px):
                    x.update(units=x["units"] + 1, last=px, stop=px - 2 * x["n"], day=d)
        if system == 1:
            virtual(d)
        cand = []
        for j in live:
            if j in st:
                continue
            lvl = hi_in[d, j] if stop_mode else hi_in[d - 1, j]
            hit = (np.isfinite(lvl) and np.isfinite(H.h[d, j]) and H.h[d, j] >= lvl) if stop_mode else \
                (np.isfinite(lvl) and np.isfinite(H.c[d - 1, j]) and H.c[d - 1, j] > lvl)
            if hit and system == 1 and vwin[j]:
                lvl = hi55[d, j] if stop_mode else hi55[d - 1, j]
                hit = (np.isfinite(lvl) and np.isfinite(H.h[d, j]) and H.h[d, j] >= lvl) if stop_mode else \
                    (np.isfinite(lvl) and np.isfinite(H.c[d - 1, j]) and H.c[d - 1, j] > lvl)
            if hit:
                cand.append((-(H.caps[d - 1, j] if np.isfinite(H.caps[d - 1, j]) else 0.0), j, lvl))
        for _, j, lvl in sorted(cand):
            if not room(j):
                continue                                   # a full sector: another sector's stock may still fit
            px = (max(o[j], lvl) if np.isfinite(o[j]) else lvl) if stop_mode else o[j]
            nn = N[d - 1, j]
            if not (np.isfinite(px) and px > 0 and np.isfinite(nn) and nn > 0 and np.isfinite(r_t)):
                continue
            usd = r_t * eq / nn * px
            c = b.cost(d, j)
            if c is None:
                continue
            if not b.afford(usd, c):
                break
            if b.buy(d, j, usd, px):
                st[j] = {"units": 1, "last": px, "n": nn, "stop": px - 2 * nn, "day": d}
        if system == 1:
            new_virtual(d)
        for j in list(st):
            if j not in b.pos:
                st.pop(j)
        b.cash *= 1 + H.rf[d]
        held.append(len(b.pos))
        return b.value(d)

    return _out(v3._run(H, s, step), b, held)


# ── Elder ──

def sim_elder(H, ver, s, cost, fill: str = "stop", tape: bool = False) -> dict:
    """v3's Elder with the risk per trade r_t = min(2 %, median two-day stop distance ÷ 20) over the liquid pool the
    evening before (a typical position about 1/20), at most 1/10 a stock, the monthly stop at 3 × r_t (2 % : 6 %).
    tape: every signal traded, no cap, no monthly rule."""
    b = Book(H, 1.0, cost, unlimited=tape)
    stops: dict[int, float] = {}
    entered: dict[int, int] = {}
    pending: dict[int, dict] = {}
    month = H.dates.to_period("M")
    state = {"eq0": 1.0, "real": 0.0, "r0": ELDER_MAX_RISK}
    held = []

    def risk_today(d) -> float:
        live = [j for j in _live(ver, d - 1) if j < H.n_stocks]
        two = np.fmin(H.l[d - 2], H.l[d - 1])
        dist = _median_ratio(H.c[d - 1] - two, H.c[d - 1], live)
        return min(ELDER_MAX_RISK, dist / ELDER_TYPICAL) if np.isfinite(dist) else ELDER_MAX_RISK

    def size(eq, px, stop, j, d, r):
        risk = px - stop
        if not (np.isfinite(px) and px > 0 and np.isfinite(risk) and risk > 0):
            return False
        usd = r * eq / risk * px if not tape else 1.0
        if not tape:
            usd = min(usd, ELDER_CAP * eq)
        if b.buy(d, j, usd, px):
            stops[j], entered[j] = stop, d
            return True
        return False

    def step(d, nav):
        eq = nav[d - 1]
        r = risk_today(d)
        if month[d] != month[d - 1]:
            state.update(eq0=eq, real=0.0, r0=r)
        v, _ = v3._book(ver, d - 1, "elder")
        sells = set(v["sell"].tolist()) if v else set()
        for j in list(b.pos):
            if j in sells and entered.get(j, d) < d:
                state["real"] += b.sell(d, j, H.o[d, j])
                stops.pop(j, None)
        for j in list(stops):
            if j in b.pos and entered[j] < d and np.isfinite(H.l[d, j]) and H.l[d, j] <= stops[j]:
                o = H.o[d, j]
                state["real"] += b.sell(d, j, min(o, stops[j]) if np.isfinite(o) else stops[j])
                stops.pop(j)
        open_ok = tape or state["real"] > -ELDER_MONTH * state["r0"] * state["eq0"]
        for j in sells:
            pending.pop(j, None)
        if fill == "stop":
            if v is not None:
                for k, j in enumerate(v["buy"].tolist()):
                    if j not in b.pos and j not in pending:
                        pending[j] = {"until": d + 4, "stop": float(np.nanmin(H.l[d - 2:d, j])), "rank": (d, k)}
            for j, x in sorted(pending.items(), key=lambda kv: kv[1]["rank"]):
                if d > x["until"] or j in b.pos:
                    pending.pop(j)
                    continue
                lvl = H.h[d - 1, j]
                if open_ok and np.isfinite(lvl) and np.isfinite(H.h[d, j]) and H.h[d, j] >= lvl:
                    o = H.o[d, j]
                    size(eq, max(o, lvl) if np.isfinite(o) else lvl, x["stop"], j, d, r)
                    pending.pop(j)
        elif v is not None and open_ok:
            for j in v["buy"].tolist():
                if j not in b.pos:
                    size(eq, H.o[d, j], float(np.nanmin(H.l[d - 2:d, j])), j, d, r)
        b.cash *= 1 + H.rf[d]
        held.append(len(b.pos))
        return b.value(d)

    return _out(v3._run(H, s, step), b, held)


# ── signals traded one at a time (1/50 each, or every one on the tape) ──

def verdict_entries(H, ver, key: str, s: int, cadence: str = "daily") -> dict[int, list[int]]:
    """Decision session → the playbook's buys that day, in its order."""
    out = {}
    for d in v3.days_from(H, s)[cadence]:
        v, _ = v3._book(ver, d, key)
        if v is not None and len(v["buy"]):
            out[d] = v["buy"].tolist()
    return out


def insider_entries(H, ver, s: int) -> dict[int, list[int]]:
    """The first session a stock has ≥ 2 insider buyers in 90 days (none in the 126 sessions before)."""
    out, last, prev = {}, {}, set()
    for d in range(s - INSIDER_HOLD, len(H.dates)):
        v, _ = v3._book(ver, d, "insider")
        now = set(v["buy"].tolist()) if v is not None else set()
        if d >= s:
            new = [j for j in v["buy"].tolist() if j not in prev and d - last.get(j, -10**9) > INSIDER_HOLD] if v is not None else []
            if new:
                out[d] = new
        for j in now - prev:
            last[j] = d
        prev = now
    return out


def pead_events(H, ver, s: int) -> list[tuple[int, int, float]]:
    """(signal session, column, reaction): close the day before the report → close the day after, minus SPY's
    (v2's ear_drift), known at the second close; stocks in the liquid pool that day."""
    spy = H.c[:, H.col["SPY"]]
    out = []
    for t, days in H.earnings.items():
        j = H.col.get(t)
        if j is None or j >= H.n_stocks:
            continue
        for e in days:
            k = int(H.dates.searchsorted(pd.Timestamp(e)))
            if k < 1 or k + 1 >= len(H.dates) or k + 1 < s:
                continue
            a, z = H.c[k - 1, j], H.c[k + 1, j]
            if np.isfinite(a) and np.isfinite(z) and a > 0 and j in _live(ver, k + 1):
                out.append((k + 1, j, float(z / a - 1 - (spy[k + 1] / spy[k - 1] - 1))))
    return sorted(out)


def pead_entries(events: list[tuple[int, int, float]]) -> dict[int, list[int]]:
    """Signals: a reaction at or above the 90th percentile of the reactions known over the last 63 sessions."""
    if not events:
        return {}
    days = np.array([e[0] for e in events])
    vals = np.array([e[2] for e in events])
    out: dict[int, list[tuple[float, int]]] = {}
    for d, j, x in events:
        lo = int(np.searchsorted(days, d - PEAD_LOOK + 1))
        hi = int(np.searchsorted(days, d, side="right"))
        window = vals[lo:hi]
        if window.size >= PEAD_MIN_EVENTS and x >= np.quantile(window, 0.9):
            out.setdefault(d, []).append((-x, j))
    return {d: [j for _, j in sorted(v)] for d, v in out.items()}


def sim_signals(H, ver, key, s, cost, entries: dict[int, list[int]], hold: int | None = None, stop: str | None = None,
                sells: bool = True, frac: float = FRAC, tape: bool = False, cadence: str = "daily") -> dict:
    """Each signal (decided at a close) bought at the next open for `frac` of the account, skipped only when the cash
    is short; sold on the playbook's sell verdict (sells), after `hold` sessions at the open, when it leaves the liquid
    pool, or at a stop fixed at the fill and checked daily from the next session."""
    dec = set(v3.days_from(H, s)[cadence])
    b = Book(H, 1.0, cost, unlimited=tape)
    stops: dict[int, float | None] = {}
    entered: dict[int, int] = {}
    held, seen, taken = [], [0], [0]

    def step(d, nav):
        if d - 1 in dec or not sells:
            live = _live(ver, d - 1)
            out = set()
            if sells:
                v, _ = v3._book(ver, d - 1, key)
                out = set(v["sell"].tolist()) if v is not None else set()
            for j in list(b.pos):
                if j in out or (live and j not in live) or (hold and d - entered[j] >= hold):
                    b.sell(d, j, H.o[d, j])
                    stops.pop(j, None)
        size = 1.0 if tape else b.value(d, H.o) * frac
        for j in entries.get(d - 1, []):
            if j in b.pos:
                continue
            seen[0] += 1
            px = H.o[d, j]
            c = b.cost(d, j)
            if c is None or not (np.isfinite(px) and px > 0) or not b.afford(size, c):
                continue
            if b.buy(d, j, size, px):
                taken[0] += 1
                entered[j] = d
                stops[j] = v3._stop(stop, H, d - 1, j, px)
        for j in list(stops):
            st = stops[j]
            if st is not None and entered[j] < d and j in b.pos and np.isfinite(H.l[d, j]) and H.l[d, j] <= st:
                o = H.o[d, j]
                b.sell(d, j, min(o, st) if np.isfinite(o) else st)
                stops.pop(j)
        b.cash *= 1 + H.rf[d]
        held.append(len(b.pos))
        return b.value(d)

    return _out(v3._run(H, s, step), b, held, {"signals": seen[0], "taken": taken[0]})


def sim_slots_tape(H, ver, key, s, cost, stop=None, fill="open", cadence="daily") -> dict:
    """v3's slot protocol (minervini, darvas, weinstein, connors) with every signal traded: the trade-level record."""
    entries = verdict_entries(H, ver, key, s, cadence)
    if fill == "open":
        return sim_signals(H, ver, key, s, cost, entries, stop=stop, tape=True, cadence=cadence)
    # Connors: bought and sold at the signal close
    dec = set(v3.days_from(H, s)[cadence])
    b = Book(H, 1.0, cost, unlimited=True)

    def step(d, nav):
        if d in dec:
            v, live = v3._book(ver, d, key)
            if v is not None:
                out = set(v["sell"].tolist())
                for j in list(b.pos):
                    if j in out or j not in live:
                        b.sell(d, j, H.c[d, j])
                for j in v["buy"].tolist():
                    if j not in b.pos:
                        b.buy(d, j, 1.0, H.c[d, j])
        return b.value(d)

    return _out(v3._run(H, s, step), b, [])


# ── CAN SLIM ──

def holders(path: str = "holdings_ticker") -> dict[str, list[tuple[pd.Timestamp, float]]]:
    """Sharadar SF3: institutions holding each stock per quarter end (from 2013-06)."""
    from scout.data import sharadar as sr

    d = sr._read(path, ["ticker", "date", "shrholders"])
    d["date"] = pd.to_datetime(d["date"])
    return {t: list(zip(g["date"], g["shrholders"].astype(float))) for t, g in d.sort_values("date").groupby("ticker")}


def inst_up(hold: dict, t: str, day: pd.Timestamp) -> bool | None:
    """More institutions in the latest public quarter than in the one before (None: not known)."""
    rows = [r for r in hold.get(t, []) if r[0] + pd.Timedelta(days=INST_LAG_DAYS) <= day]
    if len(rows) < 2:
        return None
    return rows[-1][1] > rows[-2][1]


def flat_base(H, d: int, j: int) -> float | None:
    """The pivot when session d's close breaks out of a flat base: the 25 sessions before at most 15 % deep, the close
    above their high by at most 5 %, volume ≥ 1.4 × the 50-session average."""
    if d < VOL_AVG:
        return None
    hh, ll = H.h[d - BASE:d, j], H.l[d - BASE:d, j]
    if not (np.isfinite(hh).all() and np.isfinite(ll).all()):
        return None
    pivot = float(hh.max())
    if pivot <= 0 or (pivot - ll.min()) / pivot > BASE_DEPTH:
        return None
    c, vol, avg = H.c[d, j], H.v[d, j], np.nanmean(H.v[d - VOL_AVG:d, j])
    if not (np.isfinite(c) and pivot < c <= pivot * (1 + BASE_CHASE)):
        return None
    return pivot if np.isfinite(vol) and np.isfinite(avg) and avg > 0 and vol >= BASE_VOL * avg else None


def sim_canslim(H, ver, s, cost, slots: int = 8, tape: bool = False, inst: dict | None = None) -> dict:
    """CAN SLIM (protocol can_slim): the screen (C, A, N, L, M) passing at a close that breaks out of a flat base →
    next open, 1/slots each. Exits: 8 % under the fill (daily); O'Neil's 20 % from the pivot (intraday, max(open, level)),
    except a stock reaching it within 15 sessions of the breakout, held to the 40th session (no target, no 50-day sell
    meanwhile; the stop stays) and never given the target after; the playbook's heavy-volume close below the 50-day
    average (next open); leaving the pool. inst: Sharadar holders — buys only with more institutions than the quarter before."""
    b = Book(H, 1.0, cost, unlimited=tape)
    st: dict[int, dict] = {}
    held = []

    def step(d, nav):
        v, live = v3._book(ver, d - 1, "can_slim")
        out = set(v["sell"].tolist()) if v is not None else set()
        for j in list(st):
            x = st[j]
            if (j in out and not (x["until"] and d <= x["until"])) or (live and j not in live):
                b.sell(d, j, H.o[d, j])
                st.pop(j)
        slot = 1.0 if tape else b.value(d, H.o) / slots
        if v is not None:
            for j in v["buy"].tolist():
                if j in st or (not tape and len(st) >= slots):
                    continue
                pivot = flat_base(H, d - 1, j)
                if pivot is None:
                    continue
                if inst is not None and not inst_up(inst, H.names[j], H.dates[d - 1]):
                    continue
                px, c = H.o[d, j], b.cost(d, j)
                if c is None or not (np.isfinite(px) and px > 0) or not b.afford(slot, c):
                    continue
                if b.buy(d, j, slot, px):
                    st[j] = {"fill": d, "signal": d - 1, "pivot": pivot, "stop": px * 0.92, "until": None, "fast": False}
        for j in list(st):
            x = st[j]
            if x["fill"] >= d or j not in b.pos:
                continue
            o, lo, hi = H.o[d, j], H.l[d, j], H.h[d, j]
            if np.isfinite(lo) and lo <= x["stop"]:
                b.sell(d, j, min(o, x["stop"]) if np.isfinite(o) else x["stop"])
                st.pop(j)
                continue
            level = x["pivot"] * TARGET
            if np.isfinite(hi) and hi >= level and not x["fast"]:
                if d - x["signal"] <= FAST:
                    x.update(fast=True, until=x["signal"] + EIGHT_WEEKS)   # the eight-week rule
                else:
                    b.sell(d, j, max(o, level) if np.isfinite(o) else level)
                    st.pop(j)
        for j in list(st):
            if j not in b.pos:
                st.pop(j)
        b.cash *= 1 + H.rf[d]
        held.append(len(b.pos))
        return b.value(d)

    return _out(v3._run(H, s, step), b, held)


# ── breadth thrust ──

def sim_thrust(H, ver, s, cost) -> dict:
    """SPY for 252 sessions from the next open after each new thrust (a new one restarts the count), cash otherwise."""
    spy = H.col["SPY"]
    b = Book(H, 1.0, cost)
    until, prev, signals = -1, False, []

    def step(d, nav):
        nonlocal until, prev
        x = ver.get(d - 1)
        v = x["v"].get("breadth_thrust") if x and x["stocks"] is not None else None
        on = bool(v and v["action"] == "买入")
        if on and not prev:
            until = d + THRUST_HOLD
            signals.append(d - 1)
        prev = on if v is not None else prev
        if d < until and spy not in b.pos:
            b.buy(d, spy, b.cash / (1 + COST), H.o[d, spy])
        elif d >= until and spy in b.pos:
            b.sell(d, spy, H.o[d, spy])
        b.cash *= 1 + H.rf[d]
        return b.value(d)

    r = v3._run(H, s, step)
    nav, dates = r["nav"], H.dates
    after = []
    for i in signals:
        k = min(i + THRUST_HOLD, len(dates) - 1)
        after.append({"date": dates[i].date().isoformat(), "spy_12m": float(H.c[k, spy] / H.c[i, spy] - 1),
                      "spy_6m": float(H.c[min(i + 126, len(dates) - 1), spy] / H.c[i, spy] - 1), "complete": i + THRUST_HOLD < len(dates)})
    return {**r, "trades": b.trades, "signals": after}


# ── trade-level record ──

def trade_stats(H, log: list[tuple], pool: np.ndarray, s: int) -> dict | None:
    """Every closed trade: net return (costs in the basis and proceeds), vs SPY and the equal-weight pool over the same
    closes (entry session − 1 → exit session), vs holding the stock (its mean daily log return over the window × days,
    as scout.base_rates' hold_ret); means and t clustered by entry month."""
    if not log:
        return None
    spy = H.c[:, H.col["SPY"]]
    lc = np.log(np.where(H.c[s:] > 0, H.c[s:], np.nan))
    mu = np.nanmean(np.diff(lc, axis=0), axis=0)
    rows = []
    for j, a, z, basis, proceeds in log:
        if basis <= 0:
            continue
        ret = proceeds / basis - 1
        days = max(z - a, 1)
        rows.append({"month": H.dates[a].to_period("M"), "ret": ret, "days": days,
                     "vs_spy": ret - (spy[z] / spy[a - 1] - 1), "vs_pool": ret - (pool[z] / pool[a - 1] - 1),
                     "vs_hold": ret - (math.exp(mu[j] * days) - 1) if np.isfinite(mu[j]) else np.nan})
    df = pd.DataFrame(rows)

    def clustered(col):
        m = df.groupby("month")[col].mean().dropna()
        t = m.mean() / (m.std(ddof=1) / math.sqrt(len(m))) if len(m) > 2 and m.std(ddof=1) > 0 else float("nan")
        return {"mean": float(df[col].mean()), "t": float(t), "months": int(len(m))}

    return {"n": int(len(df)), "win_rate": float((df["ret"] > 0).mean()), "avg_ret": float(df["ret"].mean()),
            "median_ret": float(df["ret"].median()), "avg_days": float(df["days"].mean()),
            "vs_spy": clustered("vs_spy"), "vs_pool": clustered("vs_pool"), "vs_hold": clustered("vs_hold")}


# ── one playbook ──

def protocol(H, ver, key, s, cost, pre, fill: str = "main", tape: bool = False, events: dict | None = None,
             inst: dict | None = None) -> dict | None:
    """The v4 protocol (None: the playbook is as v3)."""
    events = events or {}
    if key in ("turtle_s1", "turtle_s2"):
        return sim_turtle(H, ver, s, cost, int(key[-1]), "open" if fill == "open" else "stop", pre, tape)
    if key == "elder":
        return sim_elder(H, ver, s, cost, "open" if fill == "open" else "stop", tape)
    if key == "insider":
        return sim_signals(H, ver, key, s, cost, events["insider"], hold=INSIDER_HOLD, sells=False, tape=tape)
    if key == "pead":
        return sim_signals(H, ver, key, s, cost, events["pead"], hold=PEAD_HOLD, sells=False, tape=tape)
    if key == "breadth_thrust":
        return sim_thrust(H, ver, s, cost)
    if key == "can_slim":
        return sim_canslim(H, ver, s, cost, tape=tape, inst=inst)
    if key in SIGNAL:
        return sim_signals(H, ver, key, s, cost, verdict_entries(H, ver, key, s), stop=SIGNAL_STOP.get(key), tape=tape)
    if key in DATA_FULL:
        mode, size = DATA_FULL[key]
        period = {"annual_rebuild": 12, "annual_review": 12, "quarterly_review": 3}[mode]
        return v3.sim_tranches(H, ver, key, s, cost, period, "rebuild" if mode == "annual_rebuild" else "review", size)
    return None


def tape(H, ver, key, s, cost, pre, events) -> dict | None:
    """The trade-level run of a signal playbook."""
    if key in ("minervini", "darvas"):
        return sim_slots_tape(H, ver, key, s, cost, v3.CONCENTRATED[key])
    if key == "weinstein":
        return sim_slots_tape(H, ver, key, s, cost, "low50", cadence="weekly")
    if key == "connors_rsi2":
        return sim_slots_tape(H, ver, key, s, cost, fill="close")
    return protocol(H, ver, key, s, cost, pre, tape=True, events=events)


def _row(H, nav, bench, ref) -> dict:
    return {"all": ph.stats(nav, H.dates, H.rf), **ph.halves(nav, H.dates, H.rf),
            "vs_ref": ph.excess_t(nav, bench[ref], H.dates), "vs_spy": ph.excess_t(nav, bench["SPY"], H.dates)}


def _strip(r: dict) -> dict:
    return {k: v for k, v in r.items() if k not in ("nav", "log")}


def evaluate(H, ver, ver_full, s, v3_res: dict, small: tuple | None = None, inst: dict | None = None) -> dict:
    """ver: v2's + v3's verdicts; ver_full: the playbooks recomputed on the full fields (DATA_FULL + can_slim), merged
    over ver. small: (H3000, its full verdicts, cost table). v3_res: v3's result (its rows are shown beside)."""
    pre = v3.precompute(H)
    vf = v3.merge(ver, ver_full)
    Hs = replace(H, start=s, days=v3.days_from(H, s))
    bench = ph.benchmarks(Hs, ver)
    pool = bench["等权股票池"]
    events = {"insider": insider_entries(H, ver, s), "pead": pead_entries(pead_events(H, ver, s))}
    old = {r["key"]: r for r in v3_res["playbooks"]}
    books = {p.key: p for p in ph.books()}
    res = {"key": KEY, "period": [H.dates[s].date().isoformat(), H.dates[-1].date().isoformat()], "cost": COST,
           "cost_high": COST_HIGH, "benchmarks": {k: {"all": ph.stats(v, H.dates, H.rf), **ph.halves(v, H.dates, H.rf)}
                                                  for k, v in bench.items()}, "playbooks": [], "trade_level": []}
    for key in CHANGED:
        p = books[key]
        ref = "SPY" if p.school in ("大盘择时", "资产配置") else "等权股票池"
        new = protocol(H, vf, key, s, v3.flat_cost(COST), pre, events=events)
        high = protocol(H, vf, key, s, v3.flat_cost(COST_HIGH), pre, events=events)
        row = {"key": key, "name": p.name, "school": p.school, "protocol": PROTOCOL_ZH[key], "ref": ref,
               **_row(H, new["nav"], bench, ref), "high_cost": ph.stats(high["nav"], H.dates, H.rf),
               **{k: v for k, v in _strip(new).items() if k in ("trades", "avg_positions", "signals", "taken")},
               "v3": {k: old.get(key, {}).get(k) for k in ("all", "vs_ref", "trades", "avg_positions", "v2_window")}}
        if key in ("turtle_s1", "turtle_s2", "elder"):
            ctl = protocol(H, vf, key, s, v3.flat_cost(COST), pre, fill="open", events=events)
            row["next_open"] = {**_row(H, ctl["nav"], bench, ref), "trades": ctl["trades"]}
        if key == "can_slim" and inst is not None:
            s_i = int(H.dates.searchsorted(pd.Timestamp("2013-09-30") + pd.Timedelta(days=INST_LAG_DAYS)))
            ri = sim_canslim(H, vf, s_i, v3.flat_cost(COST), inst=inst)
            sp = ph.same_period(bench, H.dates[s_i].date().isoformat(), H.dates, H.rf)
            nav0 = protocol(H, vf, key, s, v3.flat_cost(COST), pre)["nav"].copy()
            nav0[:s_i] = np.nan
            row["with_i"] = {**_row(H, ri["nav"], bench, ref), "trades": ri["trades"], "same_period": sp,
                             "without_i_same_period": ph.stats(nav0, H.dates, H.rf)}
        res["playbooks"].append(row)
        logger.info("%s: v4 %.1f%% (v3 %.1f%%)", key, (row["all"] or {}).get("cagr", float("nan")) * 100,
                    ((old.get(key) or {}).get("all") or {}).get("cagr", float("nan")) * 100)
    for key in TRADE_LEVEL:
        t = tape(H, vf, key, s, v3.flat_cost(COST), pre, events)
        res["trade_level"].append({"key": key, "name": books[key].name, **(trade_stats(H, t["log"], pool, s) or {})})
        logger.info("%s: %d trades on the tape", key, len(t["log"]))
    if small:
        H3, ver3, table = small
        pre3 = v3.precompute(H3)
        main, hi = v3.small_cost(H3, table), v3.small_cost(H3, table, high=True)
        b3 = {"SPY": bench["SPY"], "前 3000 等权": v3.bench_equal(H3, ver3, s, main)}
        res["benchmarks_small"] = {k: {"all": ph.stats(v, H3.dates, H3.rf), **ph.halves(v, H3.dates, H3.rf)} for k, v in b3.items()}
        old_small = {r["key"]: r for r in v3_res.get("small", [])}
        res["small"] = []
        for k in SMALL:
            new, high = protocol(H3, ver3, k, s, main, pre3), protocol(H3, ver3, k, s, hi, pre3)
            res["small"].append({"key": k, "name": books[k].name, "ref": "前 3000 等权", **_row(H3, new["nav"], b3, "前 3000 等权"),
                                 "high_cost": ph.stats(high["nav"], H3.dates, H3.rf), "trades": new["trades"],
                                 "avg_positions": new.get("avg_positions"),
                                 "v3": {x: old_small.get(k, {}).get(x) for x in ("all", "vs_ref", "trades")}})
    return res


# ── report ──

def report_text(res: dict) -> str:
    _p = ph._p
    lines = [f"打法回测 v4（{res['period'][0]} 至 {res['period'][1]}，同 v3 的窗口；只重做 v3 里配置退化或缺规则的打法，"
             f"其余沿用 v3。单边成本 {res['cost'] * 100:.2f}%）。括号里是 v3。描述历史，不改变任何打法的检验状态。", ""]
    for k, x in res["benchmarks"].items():
        a = x["all"]
        lines.append(f"基准 {k}：年化 {_p(a['cagr'])}，最大回撤 {_p(a['max_dd'], 0)}，夏普 {a['sharpe']:.2f}")
    lines.append("")
    for r in res["playbooks"]:
        a, ref, old = r["all"] or {}, r["vs_ref"] or {}, r["v3"] or {}
        oa = old.get("all") or {}
        extra = ""
        if r.get("signals"):
            extra += f"；信号 {r['signals']} 个，执行 {_p(r['taken'] / r['signals'], 0)}"
        if r.get("next_open"):
            extra += f"；次日开盘成交 {_p(r['next_open']['all']['cagr'])}"
        if r.get("with_i"):
            w = r["with_i"]
            extra += (f"；加 I（2013-11 起）年化 {_p((w['all'] or {}).get('cagr'))}，同期不加 I {_p((w['without_i_same_period'] or {}).get('cagr'))}、"
                      f"同期 {r['ref']} {_p((w['same_period'].get(r['ref']) or {}).get('cagr'))}")
        if isinstance(r.get("signals"), list):
            extra = "；信号 " + "、".join(f"{x['date']}（之后 12 个月 SPY {_p(x['spy_12m'])}）" for x in r["signals"])
        lines.append(f"· {r['name']}（{r['protocol']}）：年化 {_p(a.get('cagr'))}（v3 {_p(oa.get('cagr'))}；高成本 "
                     f"{_p((r['high_cost'] or {}).get('cagr'))}），最大回撤 {_p(a.get('max_dd'), 0)}（v3 {_p(oa.get('max_dd'), 0)}），"
                     f"每月超额 {_p(ref.get('mean'), 2)}（t {ref.get('t', float('nan')):.1f}；v3 t {(old.get('vs_ref') or {}).get('t', float('nan')):.1f}）；"
                     f"交易 {r['trades']} 笔，平均持股 {r.get('avg_positions') or float('nan'):.1f}" + extra)
    if res.get("trade_level"):
        lines += ["", "【逐笔统计：不受资金和仓位数限制时的每一笔交易，扣双边成本】"]
        for r in res["trade_level"]:
            if not r.get("n"):
                lines.append(f"· {r['name']}：没有完成的交易")
                continue
            lines.append(f"· {r['name']}：{r['n']} 笔，胜率 {_p(r['win_rate'], 0)}，平均 {_p(r['avg_ret'])}（中位 {_p(r['median_ret'])}），"
                         f"平均持有 {r['avg_days']:.0f} 天；相对持有同一只股票 {_p(r['vs_hold']['mean'])}（t {r['vs_hold']['t']:.1f}），"
                         f"相对同期 SPY {_p(r['vs_spy']['mean'])}（t {r['vs_spy']['t']:.1f}），相对同期等权 {_p(r['vs_pool']['mean'])}（t {r['vs_pool']['t']:.1f}）")
    if res.get("small"):
        lines += ["", "【小盘版本：市值前 3000，1001–3000 名用 small_cap_costs_v1 成本】"]
        for k, x in res["benchmarks_small"].items():
            lines.append(f"基准 {k}：年化 {_p(x['all']['cagr'])}，最大回撤 {_p(x['all']['max_dd'], 0)}")
        for r in res["small"]:
            a, ref = r["all"] or {}, r["vs_ref"] or {}
            lines.append(f"· {r['name']}：年化 {_p(a.get('cagr'))}（v3 {_p(((r['v3'] or {}).get('all') or {}).get('cagr'))}；高成本 "
                         f"{_p((r['high_cost'] or {}).get('cagr'))}），最大回撤 {_p(a.get('max_dd'), 0)}；每月超额 {_p(ref.get('mean'), 2)}"
                         f"（t {ref.get('t', float('nan')):.1f}）；交易 {r['trades']} 笔")
    return "\n".join(lines)
