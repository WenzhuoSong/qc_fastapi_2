"""The gate (v3 P2): how an idea becomes knowledge.

G0  pre-registration: a spec (claim, mechanism, rule, primary control, expected failure)
    is written before anything runs and stored on a `hypothesis` entry; every run of a
    variant is counted in its family.
G1  backtest (Python): the rule's own signals, fills and exits against benchmarks known at
    the signal (SPY, equal-weighted universe) and two paired controls with the same level
    formula and exit (timing: same stock, random other dates; selection: same date, random
    other stocks); t on monthly means; per half and per market regime; overlap with other
    rules.
G2  critique (AI): a fixed checklist answered from the G1 report, numbers checked.
Pass: the primary control's edge is positive in both halves, its full-sample t reaches the
family's bar (3, higher when many variants were tried) and it does not duplicate a rule that
already passed; and no checklist item fails. Then the entry moves to `backtested`; a failure
moves it to `rejected` (kept as negative knowledge).

The thresholds were agreed with the user on 2026-10-28, before any spec was run. They are
not tuned to results.
"""
from __future__ import annotations

import json
import logging
import zlib
from datetime import date
from pathlib import Path
from statistics import NormalDist
from typing import Literal

import numpy as np
import pandas as pd
import yaml
from pydantic import BaseModel, Field

from scout import base_rates, db, learned
from scout.detectors import Detector, registry
from scout.simulate import ROUND_TRIP_COST, simulate
from scout.universe import BENCHMARK, SECTOR_ETFS

logger = logging.getLogger("scout.gate")

SPECS = Path(__file__).parent / "knowledge" / "specs"
DRAWS = 3                 # control draws per signal
MAX_TRIES = 20            # attempts per draw before giving up on a control
MIN_GAP = 10              # timing control: sessions away from the signal
SEED = 7
T_MIN = 3.0               # full-sample t on the primary control (Harvey, Liu & Zhu 2016)
ALPHA = 0.05              # family-wise level behind the bar for many trials
MAX_CORR = 0.5            # monthly-return correlation with a passed rule above this = the same idea
OVERLAP_DAYS = 5          # a fill within this many sessions of another rule's fill on the same stock is shared
CONTROLS = ("timing", "select")
BENCHMARKS = ("spy", "ew")
LABEL_ZH = {"spy": "同期 SPY", "ew": "同期等权股票池", "timing": "时机对照（同股随机日）", "select": "选股对照（同日随机股）",
            "peers": "同行业对照（同日同行业其他股）"}
REPORTED = ("peers",)    # ranking mode only: shown in the report, not part of the verdict (the pass rule is the user's)
MIN_SECTOR_PEERS = 5
FORWARD_MIN_MONTHS = 6    # G3 (P3)
FORWARD_MIN_SAMPLES = 30
RECHECK_MONTHS = 6
REGIMES = ("uptrend", "mixed", "downtrend")
VOL_STATES = ("high", "low")
VOL_ZH = {"high": "高波动", "low": "低波动"}
WHEN_KEYS = ("regime", "vol", "sectors")     # a spec's `when`: the contexts the claim is about (rows outside are dropped)
EXITS = ("hold", "plan")   # a spec's `exit`: the rule's own (hold = rank close-to-close, detector bracket) or the holdings plan
EXIT_ZH = {"plan": "持仓计划：初始止损（结构位下方，至少 1 ATR）；到目标 1 不卖，止损改为吊灯止损（买入以来最高收盘 − 3×ATR，只升不降）；"
                   "不分批；最长 60 个交易日；提前卖出后到窗口结束不再投资"}


# ── G0: the spec ────────────────────────────────────────────────────────

SPEC_FIELDS = ("key", "family", "title", "claim", "mechanism", "primary_control", "expected_failure", "registered_on")
RANK_FIELDS = ("ranker", "hold", "every")        # ranking mode: hold the ranker's picks for `hold` sessions, every `every` sessions


def load_spec(path: Path | str) -> dict:
    spec = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    spec["registered_on"] = str(spec.get("registered_on") or "")
    return spec


def validate_spec(spec: dict) -> list[str]:
    from scout import rankers

    issues = [f"规格缺少 {f}" for f in SPEC_FIELDS if not spec.get(f)]
    if spec.get("primary_control") and spec["primary_control"] not in CONTROLS:
        issues.append(f"主对照必须是 {'/'.join(CONTROLS)}")
    when = spec.get("when") or {}
    if set(when) - set(WHEN_KEYS):
        issues.append(f"when 只能包含 {'/'.join(WHEN_KEYS)}")
    for k, allowed in (("regime", REGIMES), ("vol", VOL_STATES), ("sectors", tuple(SECTOR_ETFS))):
        if k in when and (not isinstance(when[k], list) or not when[k] or set(when[k]) - set(allowed)):
            issues.append(f"when.{k} 必须是非空列表，取值 {'/'.join(allowed)}")
    if spec.get("exit", "hold") not in EXITS:
        issues.append(f"exit 必须是 {'/'.join(EXITS)}")
    if spec.get("type") == "rank":
        issues += [f"排名规格缺少 {f}" for f in RANK_FIELDS if not spec.get(f)]
        if spec.get("ranker") and spec["ranker"] not in rankers.RANKERS:
            issues.append(f"排名方法 {spec['ranker']} 不存在（先在 scout.rankers 里实现）")
    elif not spec.get("detector"):
        issues.append("规格缺少 detector")
    elif spec["detector"] not in {d.name for d in registry(include_candidates=True)}:
        issues.append(f"规则 {spec['detector']} 不存在（先在 rules.DETECTORS 里加为候选）")
    return issues


def applies(spec: dict, regime: str | None, vol: str | None, sector: str | None) -> bool:
    """Whether a signal's context is inside the spec's `when` (no `when` = every context)."""
    when = spec.get("when") or {}
    return ((not when.get("regime") or regime in when["regime"]) and (not when.get("vol") or vol in when["vol"])
            and (not when.get("sectors") or sector in when["sectors"]))


def when_zh(spec: dict) -> str | None:
    from scout.universe import SECTOR_ZH

    when = spec.get("when") or {}
    parts = []
    if when.get("regime"):
        parts.append("市场状态 " + "/".join(when["regime"]))
    if when.get("vol"):
        parts.append("波动环境 " + "/".join(VOL_ZH[v] for v in when["vol"]))
    if when.get("sectors"):
        parts.append("行业 " + "/".join(SECTOR_ZH.get(x, x) for x in when["sectors"]))
    return "；".join(parts) or None


def vol_states(spy_close: np.ndarray) -> np.ndarray:
    """Market volatility per day, known at its close: SPY's 20-day realised volatility above its own
    trailing one-year median = high, else low ('' before there is a year of history)."""
    r = pd.Series(spy_close).pct_change()
    v = r.rolling(20, min_periods=20).std()
    med = v.rolling(252, min_periods=126).median()
    return np.where(med.isna() | v.isna(), "", np.where(v > med, "high", "low"))


def required_t(trials: int) -> float:
    """The bar for a family that has been run `trials` times: 3, or a Bonferroni bar if higher."""
    z = NormalDist().inv_cdf(1 - ALPHA / (2 * max(trials, 1)))
    return round(max(T_MIN, z), 2)


# ── G1: the backtest ────────────────────────────────────────────────────

def monthly_t(dates: list[date], diffs: list[float]) -> tuple[float | None, float | None, int]:
    """(mean of per-signal differences, t on monthly means, months)."""
    if not diffs:
        return None, None, 0
    s = pd.Series(diffs, index=pd.to_datetime(dates))
    m = s.groupby(s.index.to_period("M")).mean()
    if len(m) < 3 or m.std(ddof=1) == 0:
        return float(s.mean()), None, len(m)
    return float(s.mean()), float(m.mean() / (m.std(ddof=1) / np.sqrt(len(m)))), len(m)


def trade(p, det: Detector, ticker: str, i: int, span: int, plan: bool = False) -> tuple[float, int, int] | None:
    """(gross return, entry index, exit index) of the rule's bracket placed on day i, if it fills.
    plan: the rule's entry, then the holdings plan's exit (`scout.plan_exit`) within the rule's holding
    limit; the exit index is the window's last session."""
    try:
        lv = det.levels(p.row(ticker, p.dates[i]))
    except KeyError:                    # a control day without the formula's inputs (early history): draw again
        return None
    if not lv.valid():
        return None
    res = simulate(base_rates._bars(p, ticker, i + 1, i + 1 + span), lv, det.horizon_days, det.trigger_window, det.exit)
    if res.entry_date is None or (not plan and (res.return_pct is None or res.exit_date is None)):
        return None
    a = p.dates.get_loc(res.entry_date)
    if plan:
        from scout import plan_exit

        sessions = plan_exit.cap(det.exit.holding_limit(det.horizon_days))
        got = plan_exit.outcome(_stock(p, ticker), a, float(res.entry_price), sessions)
        return None if got is None else (got[0], a, a + sessions - 1)
    return res.return_pct, a, p.dates.get_loc(res.exit_date)


_STOCKS: dict = {}


def _stock(p, ticker: str) -> dict:
    """Per-stock arrays for the plan replay, cached per panel."""
    from scout import plan_exit

    key = (id(p), ticker)
    if key not in _STOCKS:
        _STOCKS[key] = plan_exit.stock_arrays(p, ticker)
    return _STOCKS[key]


class Market:
    """Panel-wide arrays shared by every backtest on one panel."""

    def __init__(self, p):
        close = p["close"]
        self.stocks = [t for t in p.stocks if t in close.columns]
        self.valid = (close[self.stocks].notna() & p["atr14"][self.stocks].notna() & p["sma50"][self.stocks].notna()).to_numpy()
        ew = close[self.stocks].pct_change().mean(axis=1, skipna=True).fillna(0.0).to_numpy()
        self.ew_cum = np.concatenate([[1.0], np.cumprod(1 + ew)])
        self.spy_open, self.spy_close = p["open"][BENCHMARK].to_numpy(), p["close"][BENCHMARK].to_numpy()
        self.col = {t: j for j, t in enumerate(self.stocks)}
        self.regime = p.regime.reindex(p.dates).astype(str).to_numpy()
        self.vol = vol_states(self.spy_close)
        self.sector = {t: (getattr(p, "sectors", None) or {}).get(t) for t in self.stocks}


def rng_for(det: Detector) -> np.random.Generator:
    """One stream per rule, so adding or reordering rules never changes another rule's controls."""
    return np.random.default_rng([SEED, zlib.crc32(det.name.encode())])


def fills(p, det: Detector, m: Market, plan: bool = False) -> list[dict]:
    """The rule's own filled trades (no controls)."""
    span = det.trigger_window + det.exit.holding_limit(det.horizon_days)
    n = len(p.dates)
    out = []
    for ticker, i in base_rates.hit_positions(det.mask(p)):
        if i + span >= n or ticker not in m.col:
            continue
        got = trade(p, det, ticker, i, span, plan)
        if got:
            ret, a, b = got
            out.append({"ticker": ticker, "i": i, "date": p.dates[i], "ret": ret, "a": a, "b": b, "regime": m.regime[i],
                        "vol": m.vol[i], "sector": m.sector.get(ticker)})
    return out


def backtest(p, det: Detector, m: Market, draws: int = DRAWS, plan: bool = False) -> list[dict]:
    """Every filled signal with its paired benchmarks and controls (each traded the same way)."""
    rng = rng_for(det)
    span = det.trigger_window + det.exit.holding_limit(det.horizon_days)
    n = len(p.dates)
    rows = []
    for r in fills(p, det, m, plan):
        ticker, i = r["ticker"], r["i"]
        days = np.flatnonzero(m.valid[: n - span - 1, m.col[ticker]])
        timing = []
        for _ in range(MAX_TRIES * draws):
            if len(timing) == draws or not days.size:
                break
            j = int(rng.choice(days))
            if abs(j - i) < MIN_GAP:
                continue
            c = trade(p, det, ticker, j, span, plan)
            if c:
                timing.append(c[0])
        names = [m.stocks[k] for k in np.flatnonzero(m.valid[i]) if m.stocks[k] != ticker]
        select = []
        for _ in range(MAX_TRIES * draws):
            if len(select) == draws or not names:
                break
            c = trade(p, det, names[int(rng.integers(len(names)))], i, span, plan)
            if c:
                select.append(c[0])
        a, b = r["a"], r["b"]
        rows.append({**r, "spy": float(m.spy_close[b] / m.spy_open[a] - 1), "ew": float(m.ew_cum[b + 1] / m.ew_cum[a] - 1),
                     "timing": float(np.mean(timing)) if timing else None,
                     "select": float(np.mean(select)) if select else None})
    return rows


def rank_backtest(p, spec: dict, m: Market, angle: dict | None = None) -> list[dict]:
    """Ranking mode: every `every` sessions take the ranker's picks, buy at the next open, sell at the
    close `hold` sessions later. Selection control = the mean holding return of every other stock in
    the universe that day (the expectation of a same-day random stock); timing control = the same
    stock held from random other rebalance days (at least `hold` sessions away). With `exit: plan` every
    holding (picks and controls alike) is managed by the holdings plan inside the same window."""
    from scout import rankers

    a, stocks = rankers.arrays(p)
    a["angle"] = angle or {}
    fn, hold, every, frac = rankers.RANKERS[spec["ranker"]], int(spec["hold"]), int(spec["every"]), float(spec.get("top_frac", 0.1))
    o, c, n = a["open"], a["close"], len(p.dates)
    rng = np.random.default_rng([SEED, zlib.crc32(spec["key"].encode())])
    dates = rankers.rebalance_dates(n, spec)
    managed = plan_matrix(p, stocks, hold) if spec.get("exit") == "plan" else None
    picks, rets, mean_ret, members, sector_sum = {}, {}, {}, {}, {}
    for i in dates:
        uni = rankers._universe(a, i)
        r = managed[i, uni] if managed is not None else c[i + hold, uni] / o[i + 1, uni] - 1
        ok = np.isfinite(r)
        rets[i] = dict(zip(uni[ok].tolist(), r[ok].tolist()))
        mean_ret[i] = (float(r[ok].sum()), int(ok.sum()))
        by_sector: dict = {}
        for j, x in rets[i].items():
            by_sector.setdefault(m.sector.get(stocks[j]), []).append(x)
        sector_sum[i] = {g: (sum(xs), len(xs)) for g, xs in by_sector.items() if g}
        picks[i] = [j for j in fn(a, i, frac) if j in rets[i]]
        for j in rets[i]:
            members.setdefault(j, []).append(i)
    rows = []
    for i in dates:
        total, cnt = mean_ret[i]
        for j in picks[i]:
            r = rets[i][j]
            others = [d for d in members.get(j, []) if abs(d - i) >= hold]
            timing = [rets[d][j] for d in rng.choice(others, size=min(DRAWS, len(others)), replace=False)] if others else []
            b = i + hold
            rows.append({"ticker": stocks[j], "i": i, "date": p.dates[i], "ret": r, "a": i + 1, "b": b, "regime": m.regime[i],
                         "vol": m.vol[i], "sector": m.sector.get(stocks[j]),
                         "spy": float(m.spy_close[b] / m.spy_open[i + 1] - 1), "ew": float(m.ew_cum[b + 1] / m.ew_cum[i + 1] - 1),
                         "timing": float(np.mean(timing)) if timing else None,
                         "select": (total - r) / (cnt - 1) if cnt > 1 else None,
                         "peers": _peers(sector_sum[i].get(m.sector.get(stocks[j])), r)})
    return rows


PLAN_HOLDS = (20, 21, 63)        # the registered windows, computed together (one managed path per buy)
_PLANS: dict = {}


def plan_matrix(p, stocks: list[str], hold: int) -> np.ndarray:
    """(dates × stocks) returns of next-open buys managed by the holdings plan over `hold` sessions,
    for every day; computed once per panel (several specs in one process share it)."""
    from scout import plan_exit, rankers

    key = (id(p), tuple(stocks))
    if key not in _PLANS or hold not in _PLANS[key]:
        holds = tuple(sorted({*PLAN_HOLDS, hold, *(_PLANS.get(key) or {})}))
        days = list(range(rankers.YEAR, len(p.dates) - 1))
        _PLANS[key] = plan_exit.matrix(p, stocks, days, holds)
    return _PLANS[key][hold]


def _peers(sec: tuple[float, int] | None, r: float) -> float | None:
    """Mean of the other stocks of the pick's sector that day (None with fewer than MIN_SECTOR_PEERS)."""
    if not sec or sec[1] - 1 < MIN_SECTOR_PEERS:
        return None
    return (sec[0] - r) / (sec[1] - 1)


def _edge(rows: list[dict], key: str) -> dict:
    rs = [r for r in rows if r[key] is not None]
    mean, t, months = monthly_t([r["date"] for r in rs], [r["ret"] - r[key] for r in rs])
    return {"edge": None if mean is None else round(mean, 5), "t": None if t is None else round(t, 2), "n": len(rs), "months": months}


def summarize(rows: list[dict]) -> dict:
    """Edges vs every benchmark and control: overall, per half (split at the median signal date), per regime."""
    if not rows:
        return {"n": 0}
    dates = sorted(r["date"] for r in rows)
    mid = dates[len(dates) // 2]
    halves = {"first": [r for r in rows if r["date"] < mid], "second": [r for r in rows if r["date"] >= mid]}
    net = np.array([r["ret"] for r in rows]) - ROUND_TRIP_COST
    out = {"n": len(rows), "first": str(dates[0]), "split": str(mid), "last": str(dates[-1]),
           "avg_net": round(float(net.mean()), 5), "win_rate_net": round(float((net > 0).mean()), 3)}
    for k in (*BENCHMARKS, *CONTROLS, *(k for k in REPORTED if any(k in r for r in rows))):
        out[f"vs_{k}"] = _edge(rows, k)
        for h, rs in halves.items():
            out[f"vs_{k}_{h}"] = _edge(rs, k)
    out["regimes"] = _split(rows, "regime", REGIMES)
    # Exploratory context splits (not part of the verdict): market volatility and sector.
    out["vol"] = _split(rows, "vol", VOL_STATES)
    out["sectors"] = _split(rows, "sector", sorted({r.get("sector") for r in rows} - {None, ""}))
    return out


def _split(rows: list[dict], field: str, values) -> dict:
    out = {}
    for g in values:
        rs = [r for r in rows if r.get(field) == g]
        out[g] = {**{f"vs_{k}": _edge(rs, k) for k in CONTROLS}, "n": len(rs)}
    return out


def monthly_returns(rows: list[dict]) -> pd.Series:
    s = pd.Series([r["ret"] for r in rows], index=pd.to_datetime([r["date"] for r in rows]))
    return s.groupby(s.index.to_period("M")).mean()


def overlap(rows: list[dict], other: list[dict]) -> dict:
    """Share of this rule's fills that another rule also filled on the same stock within a few
    sessions, and the correlation of the two rules' monthly mean returns."""
    by_ticker: dict[str, list[int]] = {}
    for r in other:
        by_ticker.setdefault(r["ticker"], []).append(r["a"])
    shared = sum(any(abs(r["a"] - a) <= OVERLAP_DAYS for a in by_ticker.get(r["ticker"], ())) for r in rows)
    a, b = monthly_returns(rows), monthly_returns(other)
    both = pd.concat([a, b], axis=1, join="inner").dropna()
    corr = float(both.iloc[:, 0].corr(both.iloc[:, 1])) if len(both) >= 12 else None
    return {"shared": round(shared / len(rows), 3) if rows else None, "corr": None if corr is None or not np.isfinite(corr) else round(corr, 2),
            "months": len(both)}


def criteria(summary: dict, spec: dict, trials: int, overlaps: dict[str, dict], passed_rules: set[str]) -> list[dict]:
    """The pass conditions of G1, each with its value and bar."""
    k = spec["primary_control"]
    bar = required_t(trials)
    f, s, a = summary.get(f"vs_{k}_first") or {}, summary.get(f"vs_{k}_second") or {}, summary.get(f"vs_{k}") or {}
    dup = {name: o for name, o in overlaps.items() if name in passed_rules and (o.get("corr") or 0) > MAX_CORR}
    return [
        {"name": "前半段超额为正", "value": f.get("edge"), "bar": "> 0", "ok": (f.get("edge") or 0) > 0},
        {"name": "后半段超额为正", "value": s.get("edge"), "bar": "> 0", "ok": (s.get("edge") or 0) > 0},
        {"name": f"全样本 t 值（本族第 {trials} 次检验）", "value": a.get("t"), "bar": f"≥ {bar}", "ok": (a.get("t") or 0) >= bar},
        {"name": "不与已通过的规则重复", "value": "、".join(dup) or "无", "bar": f"月度收益相关 ≤ {MAX_CORR}", "ok": not dup},
    ]


# ── G2: the critique ────────────────────────────────────────────────────

CHECKS = {
    "look_ahead": "前视偏差：信号、价位和对照是否只用了当时能得到的数据",
    "survivorship": "幸存者偏差：股票池是当前指数成分股，结论是否因此偏乐观；对照能否抵消",
    "data_snooping": "数据挖掘：本族试过多少次，参数是否像是挑出来的，机制是否先于结果",
    "crowding": "拥挤与衰减：这是不是广为人知的做法，后半段是否明显衰减",
    "costs": "成本与容量：扣费后是否仍有优势，成交额是否足以容纳",
    "single_period": "单一时期：优势是否只来自某一段行情或某一种市场状态",
    "duplicate": "重复：是否只是已有规则或已有知识的另一种说法",
}


class CheckItem(BaseModel):
    check: Literal[tuple(CHECKS)] = Field(description="检查项 id")
    verdict: Literal["通过", "有疑问", "不通过"]
    reason: str = Field(description="1–2 句，依据检验报告里的数字或事实")


class Critique(BaseModel):
    items: list[CheckItem] = Field(description="每个检查项各一条，一个都不能少")
    counter_argument: str = Field(description="最有力的反方论证：为什么这个优势可能不是真的，2–3 句")
    summary: str = Field(description="一句话结论")


CRITIQUE_PROMPT = """你是一名严格的量化研究审稿人，任务是挑出一项回测结论可能不成立的地方，而不是为它辩护。

输入：spec（研究者事先登记的假设、机制、规则和预期失效情形）、report（Python 计算的回测结果：与同期 SPY、等权股票池、
时机对照、选股对照的配对超额，前后两半、各市场状态，t 值按月度均值计算）、facts（关于数据和检验方法的已知事实）。

逐项回答 checks 里的每一个检查项：通过 / 有疑问 / 不通过，并写出理由。
- 判断优势是否成立，看 facts.主对照 对应的那一列（report 里 vs_<主对照>）；其他对照只作参考。
- "不通过"只用于报告或事实明确显示的问题：主对照在某半段或某种市场状态（样本 ≥ 30）为负；与 facts.已通过的规则
  高度相关（月度收益相关 > 0.5）。与已否定的规则重合不算重复。
- 证据不足以排除的风险写"有疑问"。
最后写出最有力的反方论证。

硬性规则：数字只能原样引用 report 或 facts 里的数字，不要自己计算；不编造检验之外的事实；中文、简洁。"""


def critique_facts(spec: dict, trials: int, det: Detector | None, rule_status: dict[str, str] | None = None) -> dict:
    rule_status = rule_status or {}
    hold = det.exit.holding_limit(det.horizon_days) if det else int(spec["hold"])
    return {
        "主对照": f"{spec['primary_control']}（{LABEL_ZH[spec['primary_control']]}）",
        "已通过的规则": [k for k, v in rule_status.items() if v == "passed"] or "无",
        "已否定的规则": [k for k, v in rule_status.items() if v == "rejected"] or "无",
        "股票池": "当前 S&P 500 / 400 与 Nasdaq-100 成分股（含幸存者偏差）；时机和选股对照来自同一股票池，偏差在配对差值里基本抵消",
        "数据": "Yahoo 日线，复权价；信号用当日收盘后可得的特征，次日起才能成交",
        "成本": f"每笔往返成本 {ROUND_TRIP_COST * 100:.1f}%（对照同样扣除）",
        "本族检验次数": trials,
        "持有期上限（交易日）": hold,
        **({"检验方式": f"排名：每 {spec['every']} 个交易日取排名靠前的股票，次日开盘买入，"
                    + (f"在 {spec['hold']} 个交易日的窗口内按卖出规则管理" if spec.get("exit") == "plan"
                       else f"持有 {spec['hold']} 个交易日后按收盘卖出") + "；选股对照 = 当天股票池里其他所有股票的平均收益（同样的买卖方式）"}
           if det is None else {}),
        **({"卖出规则": EXIT_ZH["plan"] + "。选股和时机对照用同样的卖出规则；SPY 与等权股票池是买入持有，只作背景，不可与本策略直接比较"}
           if spec.get("exit") == "plan" else {}),
        **({"适用条件": f"只统计这些情境下的信号：{when_zh(spec)}"} if spec.get("when") else {}),
        "市场状态最小样本": MIN_REGIME_N,
        **({"同行业对照": "report 里的 vs_peers = 选中股票减去同一天同行业其他股票的平均收益，用来看优势是不是来自行业偏向；仅供参考，不参与判定"}
           if det is None else {}),
        "情境拆分": "report 里的 vol（市场波动高 / 低）和 sectors（行业）拆分是探索性的，没有做多重检验校正，"
                "不能单独作为通过或不通过的依据；只用于指出优势可能集中在哪里",
        "事先登记日期": spec.get("registered_on"),
        **({"登记说明": spec["registration_note"]} if spec.get("registration_note") else {}),
    }


MIN_REGIME_N = 30


def critique_misreads(c: Critique, report: dict, primary: str) -> list[str]:
    """'Fail' verdicts the report does not support, on the checks Python can see."""
    s, out = report.get("summary") or {}, []
    verdict = {i.check: i.verdict for i in c.items}
    if verdict.get("single_period") == "不通过":
        weak = [h for h in ("first", "second") if ((s.get(f"vs_{primary}_{h}") or {}).get("edge") or 0) <= 0]
        weak += [g for g, v in (s.get("regimes") or {}).items()
                 if v.get("n", 0) >= MIN_REGIME_N and ((v.get(f"vs_{primary}") or {}).get("edge") or 0) <= 0]
        if not weak:
            out.append(f"单一时期判了不通过，但主对照（vs_{primary}）在两个半段和样本 ≥ {MIN_REGIME_N} 的每种市场状态都为正；"
                       "其他对照的 t 值偏低只能写有疑问")
    if verdict.get("duplicate") == "不通过":
        dup = next((c_ for c_ in report.get("criteria") or [] if c_["name"] == "不与已通过的规则重复"), None)
        if dup and dup["ok"]:
            out.append("重复判了不通过，但它与已通过的规则都不高度相关；与已否定的规则重合不算重复")
    return out


def validate_critique(c: Critique, payload: dict) -> list[str]:
    from scout.thesis import allowed_numbers, unsupported_numbers

    issues = []
    got = [i.check for i in c.items]
    if sorted(got) != sorted(CHECKS):
        issues.append(f"必须且只能回答这些检查项各一次：{', '.join(CHECKS)}")
    bad = unsupported_numbers("\n".join([*(i.reason for i in c.items), c.counter_argument, c.summary]), allowed_numbers(payload))
    if bad:
        issues.append(f"以下数字不在报告中：{', '.join(sorted(set(bad)))}")
    primary = str((payload.get("facts") or {}).get("主对照", "")).split("（")[0]
    if primary in CONTROLS:
        issues += critique_misreads(c, payload.get("report") or {}, primary)
    return issues


async def critique(spec: dict, report: dict, facts: dict) -> dict:
    """G2. Without a model (or when the answer fails validation twice) the gate stops: no fallback passes."""
    from scout.config import get_scout_settings

    settings = get_scout_settings()
    payload = {"spec": {k: spec.get(k) for k in ("title", "claim", "mechanism", "expected_failure")},
               "report": report, "facts": facts, "checks": CHECKS}
    if not settings.openai_api_key:
        return {"source": "none", "issues": ["没有配置 AI 模型，批判关无法完成"]}
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=settings.openai_api_key, timeout=120)
    messages = [{"role": "system", "content": CRITIQUE_PROMPT},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False, default=str)}]
    history = []
    for attempt in (1, 2):
        resp = await client.beta.chat.completions.parse(model=settings.scout_insight_model, messages=messages,
                                                        response_format=Critique, temperature=0.2)
        parsed = resp.choices[0].message.parsed
        issues = validate_critique(parsed, payload)
        history.append({"attempt": attempt, "issues": issues})
        if not issues:
            return {"source": "llm", "model": settings.scout_insight_model, **parsed.model_dump(), "history": history}
        messages += [{"role": "assistant", "content": parsed.model_dump_json()},
                     {"role": "user", "content": "校验未通过，请修正后完整重写：\n" + "\n".join(issues)}]
    return {"source": "failed", "issues": history[-1]["issues"], "history": history}


def critique_ok(c: dict) -> tuple[bool, list[str]]:
    """(passed, items to show the user). A critique that did not complete does not pass."""
    if c.get("source") != "llm":
        return False, c.get("issues") or ["批判关没有完成"]
    failed = [f"{CHECKS[i['check']].split('：')[0]}：{i['reason']}" for i in c["items"] if i["verdict"] == "不通过"]
    doubts = [f"{CHECKS[i['check']].split('：')[0]}：{i['reason']}" for i in c["items"] if i["verdict"] == "有疑问"]
    return not failed, failed or doubts


# ── report text (what the knowledge entry keeps as evidence) ────────────

def _fmt(e: dict) -> str:
    if not e or e.get("edge") is None:
        return "—"
    return f"{e['edge'] * 100:+.2f}%" + (f"（t {e['t']:.2f}）" if e.get("t") is not None else "")


def evidence_summary(summary: dict, spec: dict) -> str:
    k = spec["primary_control"]
    return (f"{summary['first']} 至 {summary['last']}，{summary['n']} 笔成交，扣费后平均 {summary['avg_net'] * 100:+.2f}%；"
            f"相对{LABEL_ZH[k]} {_fmt(summary[f'vs_{k}_first'])} / {_fmt(summary[f'vs_{k}_second'])}（前半 / 后半），"
            f"全样本 {_fmt(summary[f'vs_{k}'])}；相对同期 SPY {_fmt(summary['vs_spy'])}，相对等权股票池 {_fmt(summary['vs_ew'])}。")


# ── orchestration ───────────────────────────────────────────────────────

async def register(spec: dict) -> dict:
    """G0: store the spec on a new `hypothesis` entry (or attach it to the entry with that key)."""
    issues = validate_spec(spec)
    if issues:
        raise learned.KnowledgeError("；".join(issues))
    old = await learned.get(spec["key"])
    if old and old.get("spec"):
        raise learned.KnowledgeError("这条知识已经登记过规格；改规格要换一个 key（并放进同一族）")
    base = {"key": spec["key"], "spec": spec}
    if old:                                     # attaching a spec to an existing entry (registered after its results)
        return await learned.write(base, "system", "补登规格")
    applies = {"rankers": [spec["ranker"]]} if spec.get("type") == "rank" else {"detectors": [spec["detector"]]}
    return await learned.write({**base, "kind": "rule", "polarity": "positive", "status": "hypothesis", "title": spec["title"],
                                "claim": spec["claim"], "mechanism": spec["mechanism"], "context": {"kind": ["stock"]},
                                "applies": applies, "evidence": []}, "system", "预登记")


async def trials_in(family: str) -> int:
    row = await db.fetch_one("SELECT count(*) AS n FROM scout.gate_runs WHERE family = :f", {"f": family})
    return int((row or {}).get("n") or 0)


async def run(key: str, p=None) -> dict:
    """G1 + G2 for one registered entry; records the run and moves the entry."""
    from scout.pipeline import load_panel

    entry = await learned.get(key)
    if not entry or not entry.get("spec"):
        raise learned.KnowledgeError(f"{key} 没有登记规格，先 register")
    spec = entry["spec"]
    det = None if spec.get("type") == "rank" else next(d for d in registry(include_candidates=True) if d.name == spec["detector"])
    p = p if p is not None else await load_panel(lookback_days=None)
    m = Market(p)
    if det is None:
        from scout import rankers

        rows = rank_backtest(p, spec, m, await rankers.prepare(spec, p, rankers.arrays(p)[1]))
    else:
        rows = backtest(p, det, m, plan=spec.get("exit") == "plan")
    rows = [r for r in rows if applies(spec, r["regime"], r["vol"], r["sector"])]
    summary = summarize(rows)
    entries = await learned.current()
    passed_rules = {learned.rule_of(en) for en in entries if en["key"] != key and en["polarity"] == "positive"
                    and en["status"] in ("backtested", "forward", "confirmed")} - {None}
    overlaps = {d.name: overlap(rows, fills(p, d, m)) for d in registry(include_candidates=True) if det is None or d.name != det.name}
    rule_status = {}
    for en in entries:
        rule = learned.rule_of(en)
        if rule and (det is None or rule != det.name):
            rule_status[rule] = "passed" if rule in passed_rules else "rejected" if en["status"] == "rejected" else "open"
    trials = await trials_in(spec["family"]) + 1
    crit = criteria(summary, spec, trials, overlaps, passed_rules)
    report = {"summary": summary, "criteria": crit, "overlaps": overlaps, "trials": trials, "required_t": required_t(trials)}
    g1 = all(c["ok"] for c in crit)
    review = await critique(spec, report, critique_facts(spec, trials, det, rule_status))
    g2, notes = critique_ok(review)
    passed = g1 and g2
    await db.execute(
        "INSERT INTO scout.gate_runs (key, family, spec, backtest, critique, passed) VALUES "
        "(:k, :f, CAST(:s AS JSONB), CAST(:b AS JSONB), CAST(:c AS JSONB), :ok)",
        {"k": key, "f": spec["family"], "s": db.jsonb(spec), "b": db.jsonb(report), "c": db.jsonb(review), "ok": passed})
    evidence = {"source": "gate", "date": str(date.today()), "summary": evidence_summary(summary, spec),
                "stats": {"criteria": crit, "trials": trials}}
    failed = [c["name"] for c in crit if not c["ok"]]
    note = ("检验关通过" if passed else ("检验关未通过：" if not g1 else "批判关退回：") + "；".join(failed + ([] if g2 else notes))) \
        + (f"。批判关提示：{'；'.join(notes)}" if passed and notes else "")
    # Only the measured backtest rejects. A failed critique sends the idea back (status kept,
    # the issues in the note) for the researcher and the user to answer; so does an unfinished one.
    if passed:
        status = "backtested"
    elif not g1:
        status = "rejected" if entry["status"] in learned.OPEN else entry["status"]
    else:
        status = entry["status"]
    update = {"key": key, "evidence": [*entry.get("evidence", []), evidence], "status": status}
    if status == "rejected":
        update["polarity"] = "negative"
    elif passed:
        update["polarity"] = "positive"
    await learned.write(update, "system", note[:900])
    return {"key": key, "spec": spec, "passed": passed, "g1": g1, "g2": g2, "status": status, "report": report, "critique": review, "note": note}


def report_text(res: dict) -> str:
    s, rep = res["report"]["summary"], res["report"]
    lines = [f"检验关：{res['key']} → {'通过' if res['passed'] else '未通过'}（状态 {learned.STATUS_ZH[res['status']]}）",
             *([f"适用条件：{when_zh(res['spec'])}"] if (res.get("spec") or {}).get("when") else []),
             *([f"卖出规则：{EXIT_ZH['plan']}（SPY、等权两行是买入持有，仅作背景）"] if (res.get("spec") or {}).get("exit") == "plan" else []),
             f"样本 {s['first']} 至 {s['last']}，{s['n']} 笔成交，按 {s['split']} 分前后两半；本族第 {rep['trials']} 次检验，t 门槛 {rep['required_t']}",
             "对照 | 全样本 | 前半 | 后半"]
    for k in (*BENCHMARKS, *CONTROLS, *(k for k in REPORTED if f"vs_{k}" in s)):
        lines.append(f"{LABEL_ZH[k]}{'（仅参考）' if k in REPORTED else ''} | {_fmt(s[f'vs_{k}'])} | {_fmt(s[f'vs_{k}_first'])} | "
                     f"{_fmt(s[f'vs_{k}_second'])}")
    lines.append("市场状态（时机 / 选股对照）：" + "；".join(
        f"{g} {v['n']} 笔 {_fmt(v['vs_timing'])} / {_fmt(v['vs_select'])}" for g, v in s["regimes"].items()))
    lines.append("波动环境（探索性）：" + "；".join(
        f"{VOL_ZH[g]} {v['n']} 笔 {_fmt(v['vs_timing'])} / {_fmt(v['vs_select'])}" for g, v in (s.get("vol") or {}).items()))
    from scout.universe import SECTOR_ZH

    lines.append("行业（探索性，选股对照）：" + "；".join(
        f"{SECTOR_ZH.get(g, g)} {v['n']} 笔 {_fmt(v['vs_select'])}" for g, v in (s.get("sectors") or {}).items() if v["n"]))
    lines.append("与其他规则：" + "；".join(f"{k} 重合 {o['shared'] * 100:.0f}% 相关 {o['corr']}" for k, o in rep["overlaps"].items()
                                       if o.get("shared") is not None))
    lines += [f"{'✓' if c['ok'] else '✗'} {c['name']}：{c['value']}（要求 {c['bar']}）" for c in rep["criteria"]]
    cr = res["critique"]
    if cr.get("source") == "llm":
        lines += [f"批判 · {CHECKS[i['check']].split('：')[0]}：{i['verdict']} — {i['reason']}" for i in cr["items"]]
        lines.append(f"反方论证：{cr['counter_argument']}")
    else:
        lines.append(f"批判关未完成：{'；'.join(cr.get('issues') or [])}")
    return "\n".join(lines)
