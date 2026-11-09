"""The playbooks' history with each playbook's own execution protocol, pre-registered in
`scout/knowledge/external_specs/playbook_history_v3.yaml` (one run, local only: `python -m scout.jobs.playbook_history_v3`).

Same signals as v2 (`scout.playbook_history`: the live playbook functions on point-in-time Sharadar contexts); what
changes is how each playbook is traded — one window from the close of 2008-06-30 for every playbook and benchmark,
staggered tranches for yearly / quarterly playbooks, stops checked every day, the authors' position sizing (turtles,
Clenow, Elder, concentrated growth), stop-order fills where the author used them, whole deciles for the literature
rankers, and a top-3000 version of the methods aimed at smaller companies. v2's rules are replayed on the same window
beside it. Descriptive: no knowledge-base status changes.
"""
from __future__ import annotations

import logging
from dataclasses import replace

import numpy as np
import pandas as pd

from scout import playbook_history as ph

logger = logging.getLogger("scout.playbook_history")

KEY = "playbook_history_v3"
START = pd.Timestamp("2008-06-30")
COST, COST_HIGH = ph.COST, ph.COST_HIGH

ANNUAL_REBUILD = {"magic_formula": 30, "piotroski": None, "trending_value": 25}    # list size (None = all of it)
ANNUAL_REVIEW = {"graham": 30, "buffett_quality": 30}
QUARTERLY_REVIEW = {"lynch": 20}
FUNDAMENTAL = ("gross_profitability", "low_accruals", "low_asset_growth", "net_buyback")
PRICE_RANKERS = ("high_52w", "residual_momentum", "frog_in_pan", "short_reversal", "pead")
CONCENTRATED = {"can_slim": "pct8", "minervini": "pct8", "darvas": "darvas", "livermore": "low20"}
ALLOC_TRANCHES = ("all_weather", "permanent", "sixty_forty")
AS_V2 = ("wilcox", "macd", "golden_cross", "chart_patterns", "wyckoff", "insider")
SMALL = ("magic_formula", "piotroski", "trending_value", "lynch", "can_slim")
MONTHLY_EXTRA = (*ANNUAL_REBUILD, *ANNUAL_REVIEW, *QUARTERLY_REVIEW, *ALLOC_TRANCHES)   # v2 decided them yearly / quarterly
SMALL_MIN_DV = 1_000_000.0
SMALL_COST_MULT = 4

PROTOCOL_ZH = {
    **dict.fromkeys(ANNUAL_REBUILD, "12 个月度批次，每批每年按新名单整组重建"),
    **dict.fromkeys(ANNUAL_REVIEW, "12 个月度批次，每批每年复核（卖出符合卖出规则的，补足 30 只）"),
    "lynch": "3 个批次，每批每季复核（PEG > 2 卖出，补足 20 只）",
    **dict.fromkeys(FUNDAMENTAL, "每月判断，买入整个前 10%，跌出前 30% 卖出"),
    **dict.fromkeys(PRICE_RANKERS, "整个前 10% 等权，每月重建"),
    **dict.fromkeys(CONCENTRATED, "最多 8 只，止损每天检查"),
    "weinstein": "20 只，初始止损（突破前 50 日最低）每天检查",
    "turtle_s1": "1% 风险单位、2N 止损、加仓到 4 单位，突破价挂单", "turtle_s2": "1% 风险单位、2N 止损、加仓到 4 单位，突破价挂单",
    "clenow": "0.1% ÷ ATR 定仓，每周三判断、两周调回", "elder": "2% / 6% 规则，前一日高点挂单",
    "connors_rsi2": "20 只，信号日收盘成交",
    **dict.fromkeys(AS_V2, "同 v2"),
    **dict.fromkeys(ALLOC_TRANCHES, "12 个月度批次，每批每年调回目标权重"),
}


# ── decision days ──

def window(H: ph.Hist) -> int:
    s = int(H.dates.searchsorted(START))
    assert H.dates[s] == START, "the window starts on a trading day"
    return s


def month_ends(H: ph.Hist, s: int) -> list[int]:
    return ph.decision_days(H.dates, s)["monthly"]                     # 2008-06-30 is itself a month end


def wednesdays(H: ph.Hist, s: int) -> list[int]:
    idx = pd.Series(np.arange(len(H.dates)), index=H.dates)
    return sorted({s} | {int(i) for i in idx.groupby(H.dates.to_period("W-WED")).max() if i >= s})


def days_from(H: ph.Hist, s: int) -> dict[str, list[int]]:
    """v2's decision days on the window, with the window's first close a decision for every cadence."""
    return {k: sorted({s} | {i for i in v if i >= s}) for k, v in ph.decision_days(H.dates, s).items()}


def extra_todo(H: ph.Hist, s: int, have: dict[int, dict]) -> dict[int, list]:
    """Sessions → playbooks v2's cache lacks: the yearly / quarterly ones at every month end, Clenow on Wednesdays."""
    books = {p.key: p for p in ph.books()}
    todo: dict[int, list] = {}
    for i in month_ends(H, s):
        todo.setdefault(i, []).extend(books[k] for k in MONTHLY_EXTRA if k not in (have.get(i) or {}).get("v", {}))
    for i in wednesdays(H, s):
        if "clenow" not in (have.get(i) or {}).get("v", {}):
            todo.setdefault(i, []).append(books["clenow"])
    return {i: b for i, b in todo.items() if b}


def small_todo(H: ph.Hist, s: int) -> dict[int, list]:
    books = {p.key: p for p in ph.books()}
    todo: dict[int, list] = {i: [books["can_slim"]] for i in range(s, len(H.dates))}
    for i in month_ends(H, s):
        todo[i] = todo[i] + [books[k] for k in SMALL if k != "can_slim"]
    return todo


def merge(a: dict[int, dict], b: dict[int, dict]) -> dict[int, dict]:
    out = {i: {"stocks": x["stocks"], "v": dict(x["v"])} for i, x in a.items()}
    for i, x in b.items():
        if i in out and out[i]["stocks"] is not None:
            out[i]["v"].update(x["v"])
        else:
            out[i] = {"stocks": x["stocks"], "v": dict(x["v"])}
    return out


# ── costs ──

def flat_cost(c: float):
    return lambda d, j, sell=False: c


def small_cost(H: ph.Hist, table: dict, high: bool = False):
    """Top 1000 that week: 0.05 %; ranks 1001–3000: small_cap_costs_v1's estimate from the month before, none = not
    tradeable (no buy; a sell uses the stock's last estimate, else 0.20 %). High: the larger of 4 × and 0.20 %."""
    month = H.dates.to_period("M")
    last: dict[str, float] = {}

    def f(d: int, j: int, sell: bool = False) -> float | None:
        t = H.names[j]
        if j >= H.n_stocks or t in H.large[H.week_of(d)]:
            c = COST
        else:
            x = table.get((t, month[d]))
            if x is None:
                if not sell:
                    return None
                c = last.get(t, COST_HIGH)
            else:
                c = last[t] = float(x["main"])
        return max(SMALL_COST_MULT * c, COST_HIGH) if high else c
    return f


# ── one account ──

class Book:
    def __init__(self, H: ph.Hist, cash: float, cost):
        self.H, self.cash, self.cost = H, cash, cost
        self.pos: dict[int, float] = {}
        self.basis: dict[int, float] = {}
        self.trades = 0

    def px(self, arr: np.ndarray, d: int, j: int) -> float:
        return ph._px(self.H, arr, d, j) or 0.0

    def value(self, d: int, arr: np.ndarray | None = None) -> float:
        arr = self.H.c if arr is None else arr
        return self.cash + sum(n * self.px(arr, d, j) for j, n in self.pos.items())

    def buy(self, d: int, j: int, usd: float, price: float) -> bool:
        c = self.cost(d, j)
        if c is None or not (np.isfinite(price) and price > 0) or usd <= 0:
            return False
        usd = min(usd, self.cash / (1 + c))
        if usd <= 0:
            return False
        self.pos[j] = self.pos.get(j, 0.0) + usd / price
        self.basis[j] = self.basis.get(j, 0.0) + usd * (1 + c)
        self.cash -= usd * (1 + c)
        self.trades += 1
        return True

    def sell(self, d: int, j: int, price: float, shares: float | None = None) -> float:
        """Sells (all by default); returns the realised P&L."""
        n = self.pos.get(j, 0.0)
        shares = n if shares is None else min(shares, n)
        if shares <= 0:
            return 0.0
        price = price if np.isfinite(price) and price > 0 else self.px(self.H.c, d, j)
        proceeds = shares * price * (1 - self.cost(d, j, sell=True))
        basis = self.basis.get(j, 0.0) * shares / n
        self.cash += proceeds
        self.trades += 1
        if shares >= n - 1e-12:
            self.pos.pop(j)
            self.basis.pop(j, None)
        else:
            self.pos[j] = n - shares
            self.basis[j] -= basis
        return proceeds - basis

    def rebalance(self, d: int, target: dict[int, float], arr: np.ndarray, live: set[int] | None = None) -> None:
        """To target weights of the account (others sold), trading only the differences; sells first."""
        for j in list(self.pos):
            if j not in target:
                self.sell(d, j, arr[d, j])
        value = self.value(d, arr)
        want = {j: w * value for j, w in target.items()}
        for j, usd in want.items():
            have = self.pos.get(j, 0.0) * self.px(arr, d, j)
            if have > usd and arr[d, j] > 0:
                self.sell(d, j, arr[d, j], (have - usd) / arr[d, j])
        for j, usd in want.items():
            have = self.pos.get(j, 0.0) * self.px(arr, d, j)
            if usd > have:
                self.buy(d, j, usd - have, arr[d, j])


def _book(ver: dict[int, dict], d: int, key: str) -> tuple[dict | None, set[int]]:
    x = ver.get(d)
    if not x or x["stocks"] is None:
        return None, set()
    return x["v"].get(key), set(x["stocks"].tolist())


def _run(H: ph.Hist, s: int, step) -> dict:
    """Day loop: nav = 1 at the window's close, then step(d) trades and returns the account value at d's close."""
    nav = np.full(len(H.dates), np.nan)
    nav[s] = 1.0
    for d in range(s + 1, len(H.dates)):
        nav[d] = step(d, nav)
    return {"nav": nav}


# ── protocols ──

def _stop(kind: str | None, H: ph.Hist, d: int, j: int, price: float) -> float | None:
    from scout.playbooks.technical import darvas_box

    if kind == "pct8":
        return price * 0.92
    if kind == "darvas":
        box = darvas_box(H.h[max(0, d - 400):d, j], H.l[max(0, d - 400):d, j])     # completed before the signal day
        return box[1] if box else None
    if kind in ("low20", "low50"):
        n = 20 if kind == "low20" else 50
        x = H.l[max(0, d - n):d, j]
        return float(np.nanmin(x)) if np.isfinite(x).any() else None
    return None


def sim_slots(H, ver, key, s, cost, slots=20, stop=None, fill="open", cadence="daily") -> dict:
    """At most `slots` positions of 1/slots each, bought in the playbook's order; sold on its sell verdict or when the
    stock leaves the liquid pool; a stop fixed at the fill and checked every day (not on the fill day)."""
    dec = set(days_from(H, s)[cadence])
    b = Book(H, 1.0, cost)
    stops: dict[int, float | None] = {}
    entered: dict[int, int] = {}
    held = []

    def decide(dd, de, arr):
        v, live = _book(ver, dd, key)
        if v is None:
            return
        out = set(v["sell"].tolist())
        for j in list(b.pos):
            if j in out or j not in live:
                b.sell(de, j, arr[de, j])
                stops.pop(j, None)
        slot = b.value(de, arr) / slots
        for j in v["buy"].tolist():
            if len(b.pos) >= slots or b.cash < 0.5 * slot:
                break
            if j not in b.pos and b.buy(de, j, slot, arr[de, j]):
                stops[j], entered[j] = _stop(stop, H, dd, j, arr[de, j]), de

    if fill == "close" and s in dec:
        decide(s, s, H.c)

    def step(d, nav):
        if fill == "open" and d - 1 in dec:
            decide(d - 1, d, H.o)
        for j in list(stops):
            st = stops[j]
            if st is not None and entered[j] < d and j in b.pos and np.isfinite(H.l[d, j]) and H.l[d, j] <= st:
                o = H.o[d, j]
                b.sell(d, j, min(o, st) if np.isfinite(o) else st)
                stops.pop(j)
        if fill == "close" and d in dec:
            decide(d, d, H.c)
        b.cash *= 1 + H.rf[d]
        held.append(len(b.pos))
        return b.value(d)

    return {**_run(H, s, step), "trades": b.trades, "avg_positions": float(np.mean(held))}


def sim_rankers(H, ver, key, s, cost, mode: str) -> dict:
    """Month-end decisions, next-open trades. rebuild: the whole buy list (top 10 %) in equal weight; buffer: v2's
    enter-top-10 % / leave-top-30 % with each new buy at account ÷ the list's size."""
    dec = set(month_ends(H, s))
    b = Book(H, 1.0, cost)
    held = []

    def step(d, nav):
        if d - 1 in dec:
            v, live = _book(ver, d - 1, key)
            if v is not None:
                buys = [j for j in v["buy"].tolist() if np.isfinite(H.o[d, j]) and H.o[d, j] > 0]
                if mode == "rebuild":
                    if buys:
                        b.rebalance(d, {j: 1 / len(buys) for j in buys}, H.o)
                    else:
                        b.rebalance(d, {}, H.o)
                else:
                    out = set(v["sell"].tolist())
                    for j in list(b.pos):
                        if j in out or j not in live:
                            b.sell(d, j, H.o[d, j])
                    if buys:
                        slot = b.value(d, H.o) / len(v["buy"])
                        for j in buys:
                            if b.cash < 0.5 * slot:
                                break
                            if j not in b.pos:
                                b.buy(d, j, slot, H.o[d, j])
        b.cash *= 1 + H.rf[d]
        held.append(len(b.pos))
        return b.value(d)

    return {**_run(H, s, step), "trades": b.trades, "avg_positions": float(np.mean(held))}


def sim_tranches(H, ver, key, s, cost, period: int, mode: str, size: int | None, weights: bool = False) -> dict:
    """`period` tranches of 1/period each, all formed at the window's close; then at each month end one tranche acts
    (month k → tranche k mod period), trading at the next open. rebuild: the new list (first `size`, or all; or the
    allocation's weights) in equal weight; review: sell what the playbook sells or left the pool, fill up to `size`."""
    ends = month_ends(H, s)
    acts = {ends[0]: list(range(period))} | {i: [k % period] for k, i in enumerate(ends) if k > 0}
    books = [Book(H, 1.0 / period, cost) for _ in range(period)]
    held = []

    def act(b: Book, dd: int, d: int):
        v, live = _book(ver, dd, key)
        if v is None:
            return
        if weights:
            col = H.col
            b.rebalance(d, {col[t]: w for t, w in v["w"].items() if t in col and w > 0}, H.o)
            return
        buys = [j for j in v["buy"].tolist() if np.isfinite(H.o[d, j]) and H.o[d, j] > 0]
        if mode == "rebuild":
            pick = buys[:size] if size else buys
            b.rebalance(d, {j: 1 / len(pick) for j in pick} if pick else {}, H.o)
            return
        out = set(v["sell"].tolist())
        for j in list(b.pos):
            if j in out or j not in live:
                b.sell(d, j, H.o[d, j])
        slot = b.value(d, H.o) / size
        for j in buys:
            if len(b.pos) >= size or b.cash < 0.5 * slot:
                break
            if j not in b.pos:
                b.buy(d, j, slot, H.o[d, j])

    def step(d, nav):
        for k in acts.get(d - 1, []):
            act(books[k], d - 1, d)
        for b in books:
            b.cash *= 1 + H.rf[d]
        held.append(len(set().union(*(b.pos for b in books))))
        return sum(b.value(d) for b in books)

    return {**_run(H, s, step), "trades": sum(b.trades for b in books), "avg_positions": float(np.mean(held))}


def _true_range(H: ph.Hist) -> np.ndarray:
    prev = np.vstack([np.full((1, H.c.shape[1]), np.nan), H.c[:-1]])
    return np.fmax(H.h - H.l, np.fmax(np.abs(H.h - prev), np.abs(H.l - prev)))


def wilder_n(H: ph.Hist) -> np.ndarray:
    tr = _true_range(H)
    n = np.full_like(tr, np.nan)
    first = pd.DataFrame(tr).rolling(20).mean().to_numpy()
    cur = np.full(tr.shape[1], np.nan)
    for d in range(len(tr)):
        x = tr[d]
        cur = np.where(np.isfinite(cur), np.where(np.isfinite(x), (19 * cur + x) / 20, cur), first[d])
        n[d] = cur
    return n


def _rolling(arr: np.ndarray, n: int, how: str) -> np.ndarray:
    """Max / min of the previous n sessions (today excluded)."""
    df = pd.DataFrame(arr).shift(1).rolling(n, min_periods=n)
    return (df.max() if how == "max" else df.min()).to_numpy()


def sim_turtle(H, ver, s, cost, system: int, fill: str = "stop", pre: dict | None = None) -> dict:
    """Long-only turtles on stocks (protocol `turtles`). fill "stop": orders at the levels known the evening before;
    "open": the same rules on closes, traded at the next open (v2's way)."""
    pre = pre or {}
    n_in, n_out = (20, 10) if system == 1 else (55, 20)
    N = pre.get("N")
    hi_in, lo_out = pre[f"hi{n_in}"], pre[f"lo{n_out}"]
    hi55 = pre["hi55"]
    b = Book(H, 1.0, cost)
    st: dict[int, dict] = {}                               # j → {units, last, n, stop, day}
    ns = H.n_stocks
    vin = np.zeros(ns, bool)
    ventry, vstop, vwin = np.zeros(ns), np.zeros(ns), np.zeros(ns, bool)
    held = []
    stop_mode = fill == "stop"

    def virtual(d):
        """System 1's filter: every stock's last 20-day breakout traded on paper."""
        nonlocal vin, ventry, vstop, vwin
        lo, o = H.l[d, :ns], H.o[d, :ns]
        trig = np.fmax(vstop, lo_out[d, :ns])
        if stop_mode:
            out = vin & np.isfinite(lo) & (lo <= trig)
            px = np.where(np.isfinite(o), np.fmin(o, trig), trig)
        else:
            prev = d - 1
            out = vin & np.isfinite(H.c[prev, :ns]) & (H.c[prev, :ns] < np.fmax(vstop, lo_out[prev, :ns]))
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
            prev = d - 1
            go = ~vin & np.isfinite(hi_in[prev, :ns]) & (H.c[prev, :ns] > hi_in[prev, :ns]) & np.isfinite(H.o[d, :ns])
            px = H.o[d, :ns]
        nn = N[d - 1, :ns]
        ventry = np.where(go, px, ventry)
        vstop = np.where(go, px - 2 * nn, vstop)
        vin = vin | (go & np.isfinite(nn))

    def step(d, nav):
        eq = nav[d - 1]
        o = H.o[d]
        # exits and stops
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
        # pyramiding
        for j in list(st):
            x = st[j]
            if x["units"] >= 4 or x["day"] >= d:
                continue
            lvl = x["last"] + 0.5 * x["n"]
            hit = (np.isfinite(H.h[d, j]) and H.h[d, j] >= lvl) if stop_mode else (np.isfinite(H.c[d - 1, j]) and H.c[d - 1, j] >= lvl)
            if hit:
                px = max(o[j], lvl) if stop_mode and np.isfinite(o[j]) else (lvl if stop_mode else o[j])
                nn = N[d - 1, j]
                if np.isfinite(px) and np.isfinite(nn) and nn > 0 and b.buy(d, j, 0.01 * eq / nn * px, px):
                    x.update(units=x["units"] + 1, last=px, stop=px - 2 * x["n"], day=d)
        if system == 1:
            virtual(d)
        # entries
        _, live = _book(ver, d - 1, "turtle_s1")
        cand = []
        for j in live:
            if j in st or j >= ns:
                continue
            lvl = hi_in[d, j] if stop_mode else hi_in[d - 1, j]
            hit = (np.isfinite(lvl) and np.isfinite(H.h[d, j]) and H.h[d, j] >= lvl) if stop_mode else \
                (np.isfinite(lvl) and np.isfinite(H.c[d - 1, j]) and H.c[d - 1, j] > lvl)
            if hit and system == 1 and vwin[j]:
                lvl = hi55[d, j] if stop_mode else hi55[d - 1, j]          # skipped: the 55-day failsafe still enters
                hit = (np.isfinite(lvl) and np.isfinite(H.h[d, j]) and H.h[d, j] >= lvl) if stop_mode else \
                    (np.isfinite(lvl) and np.isfinite(H.c[d - 1, j]) and H.c[d - 1, j] > lvl)
            if hit:
                cand.append((-(H.caps[d - 1, j] if np.isfinite(H.caps[d - 1, j]) else 0.0), j, lvl))
        for _, j, lvl in sorted(cand):
            px = (max(o[j], lvl) if np.isfinite(o[j]) else lvl) if stop_mode else o[j]
            nn = N[d - 1, j]
            if not (np.isfinite(px) and px > 0 and np.isfinite(nn) and nn > 0):
                continue
            usd = 0.01 * eq / nn * px
            c = b.cost(d, j)
            if c is None:
                continue
            if b.cash < usd * (1 + c):
                break
            if b.buy(d, j, usd, px):
                st[j] = {"units": 1, "last": px, "n": nn, "stop": px - 2 * nn, "day": d}
        if system == 1:
            new_virtual(d)
        for j in list(st):                                  # a stock that left the data is valued at its last close
            if j not in b.pos:
                st.pop(j)
        b.cash *= 1 + H.rf[d]
        held.append(len(b.pos))
        return b.value(d)

    return {**_run(H, s, step), "trades": b.trades, "avg_positions": float(np.mean(held))}


def sim_clenow(H, ver, s, cost, atr: np.ndarray) -> dict:
    dec = wednesdays(H, s)
    order = {i: k for k, i in enumerate(dec)}
    b = Book(H, 1.0, cost)
    held = []

    def step(d, nav):
        k = order.get(d - 1)
        if k is not None:
            v, live = _book(ver, d - 1, "clenow")
            if v is not None:
                out = set(v["sell"].tolist())
                for j in list(b.pos):
                    if j in out or j not in live:
                        b.sell(d, j, H.o[d, j])
                value = b.value(d, H.o)
                target = lambda j: value * 0.001 / atr[d - 1, j] if np.isfinite(atr[d - 1, j]) and atr[d - 1, j] > 0 else None  # noqa: E731
                if k % 2 == 1:                              # every second decision: positions back to target risk
                    for j in list(b.pos):
                        n = target(j)
                        if n is None or not np.isfinite(H.o[d, j]):
                            continue
                        diff = n - b.pos[j]
                        if diff < 0:
                            b.sell(d, j, H.o[d, j], -diff)
                    for j in list(b.pos):
                        n = target(j)
                        if n is not None and np.isfinite(H.o[d, j]) and n > b.pos[j]:
                            b.buy(d, j, (n - b.pos[j]) * H.o[d, j], H.o[d, j])
                for j in v["buy"].tolist():
                    n = target(j)
                    if j in b.pos or n is None or not (np.isfinite(H.o[d, j]) and H.o[d, j] > 0):
                        continue
                    usd = n * H.o[d, j]
                    c = b.cost(d, j)
                    if c is None:
                        continue
                    if b.cash < usd * (1 + c):
                        break
                    b.buy(d, j, usd, H.o[d, j])
        b.cash *= 1 + H.rf[d]
        held.append(len(b.pos))
        return b.value(d)

    return {**_run(H, s, step), "trades": b.trades, "avg_positions": float(np.mean(held))}


def sim_elder(H, ver, s, cost, fill: str = "stop") -> dict:
    b = Book(H, 1.0, cost)
    stops: dict[int, float] = {}
    entered: dict[int, int] = {}
    pending: dict[int, dict] = {}
    month = H.dates.to_period("M")
    state = {"eq0": 1.0, "real": 0.0}
    held = []

    def size(eq, px, stop, j, d):
        risk = px - stop
        if not (np.isfinite(px) and px > 0 and np.isfinite(risk) and risk > 0):
            return False
        usd = min(0.02 * eq / risk * px, eq / 5)
        if b.buy(d, j, usd, px):
            stops[j], entered[j] = stop, d
            return True
        return False

    def step(d, nav):
        eq = nav[d - 1]
        if month[d] != month[d - 1]:
            state.update(eq0=eq, real=0.0)
        v, _ = _book(ver, d - 1, "elder")
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
        open_ok = state["real"] > -0.06 * state["eq0"]
        for j in sells:
            pending.pop(j, None)
        if fill == "stop":
            if v is not None:                              # yesterday's signals: buy-stops from today, 5 sessions
                for r, j in enumerate(v["buy"].tolist()):
                    if j not in b.pos and j not in pending:
                        pending[j] = {"until": d + 4, "stop": float(np.nanmin(H.l[d - 2:d, j])), "rank": (d, r)}
            for j, x in sorted(pending.items(), key=lambda kv: kv[1]["rank"]):
                if d > x["until"] or j in b.pos:
                    pending.pop(j)
                    continue
                lvl = H.h[d - 1, j]
                if open_ok and np.isfinite(lvl) and np.isfinite(H.h[d, j]) and H.h[d, j] >= lvl:
                    o = H.o[d, j]
                    size(eq, max(o, lvl) if np.isfinite(o) else lvl, x["stop"], j, d)
                    pending.pop(j)
        elif v is not None and open_ok:
            for j in v["buy"].tolist():
                if j not in b.pos:
                    size(eq, H.o[d, j], float(np.nanmin(H.l[d - 2:d, j])), j, d)
        b.cash *= 1 + H.rf[d]
        held.append(len(b.pos))
        return b.value(d)

    return {**_run(H, s, step), "trades": b.trades, "avg_positions": float(np.mean(held))}


def bench_equal(H, ver, s, cost) -> np.ndarray:
    """Every liquid stock of the pool in equal weight, re-set at each month end's next open (costs per stock)."""
    dec = set(month_ends(H, s))
    b = Book(H, 1.0, cost)

    def step(d, nav):
        if d - 1 in dec:
            x = ver.get(d - 1)
            if x and x["stocks"] is not None and len(x["stocks"]):
                live = [j for j in x["stocks"].tolist() if np.isfinite(H.o[d, j]) and H.o[d, j] > 0
                        and b.cost(d, j) is not None]
                b.rebalance(d, {j: 1 / len(live) for j in live}, H.o)
        b.cash *= 1 + H.rf[d]
        return b.value(d)

    return _run(H, s, step)["nav"]


# ── one playbook under its protocol ──

def precompute(H: ph.Hist) -> dict:
    out = {"N": wilder_n(H), "atr20": pd.DataFrame(_true_range(H)).rolling(20).mean().to_numpy()}
    for n in (20, 55):
        out[f"hi{n}"] = _rolling(H.h, n, "max")
    for n in (10, 20):
        out[f"lo{n}"] = _rolling(H.l, n, "min")
    return out


def protocol(H, ver, key, s, cost, pre: dict, fill: str = "main") -> dict | None:
    """The playbook traded by its v3 protocol (fill "open": the next-open control where v3 changed the fill)."""
    if key in ANNUAL_REBUILD:
        return sim_tranches(H, ver, key, s, cost, 12, "rebuild", ANNUAL_REBUILD[key])
    if key in ANNUAL_REVIEW:
        return sim_tranches(H, ver, key, s, cost, 12, "review", ANNUAL_REVIEW[key])
    if key in QUARTERLY_REVIEW:
        return sim_tranches(H, ver, key, s, cost, 3, "review", QUARTERLY_REVIEW[key])
    if key in ALLOC_TRANCHES:
        return sim_tranches(H, ver, key, s, cost, 12, "rebuild", None, weights=True)
    if key in FUNDAMENTAL:
        return sim_rankers(H, ver, key, s, cost, "buffer")
    if key in PRICE_RANKERS:
        return sim_rankers(H, ver, key, s, cost, "rebuild")
    if key in CONCENTRATED:
        return sim_slots(H, ver, key, s, cost, 8, CONCENTRATED[key])
    if key == "weinstein":
        return sim_slots(H, ver, key, s, cost, 20, "low50", cadence="weekly")
    if key in ("turtle_s1", "turtle_s2"):
        return sim_turtle(H, ver, s, cost, int(key[-1]), "open" if fill == "open" else "stop", pre)
    if key == "clenow":
        return sim_clenow(H, ver, s, cost, pre["atr20"])
    if key == "elder":
        return sim_elder(H, ver, s, cost, "open" if fill == "open" else "stop")
    if key == "connors_rsi2":
        return sim_slots(H, ver, key, s, cost, 20, fill="open" if fill == "open" else "close")
    return None                                                        # as v2 (stock, timing, allocation)


def v2_rules(H, ver, p, s, cost: float) -> dict:
    """v2's rules replayed on the window (decisions on v2's cadence, the window's close a decision for all)."""
    Hs = replace(H, start=s, days=days_from(H, s))
    if p.school in ("大盘择时", "资产配置"):
        r = ph.simulate_weights(Hs, ver, p.key, cost, p.school == "大盘择时")
    else:
        r = ph.simulate_stocks(Hs, ver, p.key, cost, p.key in ph.REBALANCE)
    nav = r["nav"]
    first = s + int(np.argmax(np.isfinite(nav[s:]))) if np.isfinite(nav[s:]).any() else s
    nav[s:first] = 1.0                                     # a weekly verdict first on the Friday after: cash until then
    return r


def _row(H, nav, bench, ref) -> dict:
    return {"all": ph.stats(nav, H.dates, H.rf), **ph.halves(nav, H.dates, H.rf),
            "vs_ref": ph.excess_t(nav, bench[ref], H.dates), "vs_spy": ph.excess_t(nav, bench["SPY"], H.dates)}


def evaluate(H, ver, s, small: tuple | None = None) -> dict:
    """small: (H3000, ver3000, cost table) for the top-3000 versions."""
    pre = precompute(H)
    Hs = replace(H, start=s, days=days_from(H, s))
    bench = ph.benchmarks(Hs, ver)
    res = {"key": KEY, "period": [H.dates[s].date().isoformat(), H.dates[-1].date().isoformat()],
           "cost": COST, "cost_high": COST_HIGH, "benchmarks": {k: {"all": ph.stats(v, H.dates, H.rf), **ph.halves(v, H.dates, H.rf)}
                                                               for k, v in bench.items()},
           "playbooks": [], "not_run": dict(ph.NO_HISTORY)}
    for p in ph.books():
        timing, alloc = p.school == "大盘择时", p.school == "资产配置"
        ref = "SPY" if timing or alloc else "等权股票池"
        old = v2_rules(H, ver, p, s, COST)
        new = protocol(H, ver, p.key, s, flat_cost(COST), pre) or old
        high = protocol(H, ver, p.key, s, flat_cost(COST_HIGH), pre) or v2_rules(H, ver, p, s, COST_HIGH)
        st = ph.stats(new["nav"], H.dates, H.rf)
        if st is None or not new["trades"]:
            res["not_run"][p.key] = "没有足够的历史结果（数据缺失或从未给出交易）"
            continue
        row = {"key": p.key, "name": p.name, "school": p.school, "version": p.version, "protocol": PROTOCOL_ZH.get(p.key, "同 v2"),
               "same_as_v2": p.key in AS_V2 or (timing or (alloc and p.key not in ALLOC_TRANCHES)), "ref": ref,
               **_row(H, new["nav"], bench, ref), "high_cost": ph.stats(high["nav"], H.dates, H.rf),
               "trades": new["trades"], "avg_positions": new.get("avg_positions"),
               "v2_window": {**_row(H, old["nav"], bench, ref), "trades": old["trades"]}}
        if p.key in ("turtle_s1", "turtle_s2", "elder", "connors_rsi2"):
            ctl = protocol(H, ver, p.key, s, flat_cost(COST), pre, fill="open")
            row["next_open"] = {**_row(H, ctl["nav"], bench, ref), "trades": ctl["trades"]}
        res["playbooks"].append(row)
        logger.info("%s: v3 %.1f%% / v2 window %.1f%%", p.key, st["cagr"] * 100, (row["v2_window"]["all"] or {}).get("cagr", float("nan")) * 100)
    if small:
        H3, ver3, table = small
        pre3 = precompute(H3)
        main, hi = small_cost(H3, table), small_cost(H3, table, high=True)
        b3 = {"SPY": bench["SPY"], "前 3000 等权": bench_equal(H3, ver3, s, main)}
        res["benchmarks_small"] = {k: {"all": ph.stats(v, H3.dates, H3.rf), **ph.halves(v, H3.dates, H3.rf)} for k, v in b3.items()}
        res["small"] = []
        books = {p.key: p for p in ph.books()}
        for k in SMALL:
            new, high = protocol(H3, ver3, k, s, main, pre3), protocol(H3, ver3, k, s, hi, pre3)
            res["small"].append({"key": k, "name": books[k].name, "protocol": PROTOCOL_ZH[k], "ref": "前 3000 等权",
                                 **_row(H3, new["nav"], b3, "前 3000 等权"), "high_cost": ph.stats(high["nav"], H3.dates, H3.rf),
                                 "trades": new["trades"], "avg_positions": new.get("avg_positions")})
    return res


# ── report ──

def report_text(res: dict) -> str:
    _p = ph._p
    lines = [f"打法回测 v3（{res['period'][0]} 至 {res['period'][1]}，所有打法同一时间窗口；每个打法按自己的执行协议：仓位、止损、调仓、成交方式；"
             f"Sharadar 前 1000 大、含退市股，单边成本 {res['cost'] * 100:.2f}%）。括号里是 v2 规则在同一窗口的结果。描述历史，不改变任何打法的检验状态。", ""]
    for k, x in res["benchmarks"].items():
        a = x["all"]
        lines.append(f"基准 {k}：年化 {_p(a['cagr'])}，最大回撤 {_p(a['max_dd'], 0)}，夏普 {a['sharpe']:.2f}；"
                     f"前半 {_p(x['first']['cagr'])}，后半 {_p(x['second']['cagr'])}")
    from scout.playbooks.base import SCHOOLS

    for school in SCHOOLS:
        rows = [r for r in res["playbooks"] if r["school"] == school]
        if not rows:
            continue
        lines += ["", f"【{school}】（对比 {rows[0]['ref']}）"]
        for r in rows:
            a, ref, old = r["all"], r["vs_ref"] or {}, r["v2_window"]
            lines.append(f"· {r['name']}（{r['protocol']}）：年化 {_p(a['cagr'])}（v2 {_p((old['all'] or {}).get('cagr'))}；高成本 "
                         f"{_p((r['high_cost'] or {}).get('cagr'))}），最大回撤 {_p(a['max_dd'], 0)}（v2 {_p((old['all'] or {}).get('max_dd'), 0)}），"
                         f"夏普 {a['sharpe']:.2f}；前半 {_p((r['first'] or {}).get('cagr'))}，后半 {_p((r['second'] or {}).get('cagr'))}；"
                         f"每月超额 {_p(ref.get('mean'), 2)}（t {ref.get('t', float('nan')):.1f}；v2 t {(old['vs_ref'] or {}).get('t', float('nan')):.1f}）；"
                         f"交易 {r['trades']} 笔"
                         + (f"；次日开盘成交 {_p(r['next_open']['all']['cagr'])}" if r.get("next_open") else ""))
    if res.get("small"):
        lines += ["", "【小盘版本：市值前 3000，1001–3000 名用 small_cap_costs_v1 成本】"]
        for k, x in res["benchmarks_small"].items():
            lines.append(f"基准 {k}：年化 {_p(x['all']['cagr'])}，最大回撤 {_p(x['all']['max_dd'], 0)}")
        for r in res["small"]:
            a, ref = r["all"], r["vs_ref"] or {}
            lines.append(f"· {r['name']}：年化 {_p(a['cagr'])}（高成本 {_p((r['high_cost'] or {}).get('cagr'))}），最大回撤 {_p(a['max_dd'], 0)}；"
                         f"每月超额 {_p(ref.get('mean'), 2)}（t {ref.get('t', float('nan')):.1f}）；交易 {r['trades']} 笔")
    if res["not_run"]:
        lines += ["", "没有回测：" + "；".join(f"{k}：{v}" for k, v in res["not_run"].items())]
    return "\n".join(lines)
