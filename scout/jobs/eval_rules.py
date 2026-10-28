"""Fair benchmarks for the rules: `python -m scout.jobs.eval_rules`.

The base-rate table judges a rule against "holding the same stock at its average drift",
and that drift is the stock's mean over the whole stored history: it uses the future, and
with today's index members (yesterday's winners) it is a very high bar. This study keeps
every rule, signal, fill and exit exactly as the replay has them and compares each filled
trade with benchmarks that use nothing the signal could not have known:

- SPY and the equal-weighted universe over the same entry → exit window;
- timing control: the same stock entered on random other dates, same level formula and exit;
- selection control: random other stocks entered on the signal date, same formula and exit.

Each control is the mean of `DRAWS` filled draws (fixed seed). Differences are paired per
signal; t-statistics are on monthly means (signals cluster in time). Stored as a brief
(`rule_benchmarks`).
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from scout import base_rates, db
from scout.detectors import registry
from scout.jobs._run import run
from scout.pipeline import load_panel
from scout.simulate import ROUND_TRIP_COST, simulate
from scout.universe import BENCHMARK

DRAWS = 3
MAX_TRIES = 20
SEED = 7


def monthly_t(dates: list[date], diffs: list[float]) -> tuple[float | None, float | None, int]:
    """(mean of per-signal differences, t on monthly means, months)."""
    if not diffs:
        return None, None, 0
    s = pd.Series(diffs, index=pd.to_datetime(dates))
    m = s.groupby(s.index.to_period("M")).mean()
    if len(m) < 3 or m.std(ddof=1) == 0:
        return float(s.mean()), None, len(m)
    return float(s.mean()), float(m.mean() / (m.std(ddof=1) / np.sqrt(len(m)))), len(m)


def trade(p, det, ticker: str, i: int, span: int) -> tuple[float, int, int] | None:
    """(gross return, entry index, exit index) of the rule's bracket placed on day i, if it fills."""
    try:
        lv = det.levels(p.row(ticker, p.dates[i]))
    except KeyError:                    # a control day without the formula's inputs (early history): draw again
        return None
    if not lv.valid():
        return None
    res = simulate(base_rates._bars(p, ticker, i + 1, i + 1 + span), lv, det.horizon_days, det.trigger_window, det.exit)
    if res.entry_date is None or res.return_pct is None or res.exit_date is None:
        return None
    return res.return_pct, p.dates.get_loc(res.entry_date), p.dates.get_loc(res.exit_date)


async def study() -> dict:
    p = await load_panel(lookback_days=None)
    rng = np.random.default_rng(SEED)
    close = p["close"]
    n = len(p.dates)
    stocks = [t for t in p.stocks if t in close.columns]
    valid = close[stocks].notna() & p["atr14"][stocks].notna() & p["sma50"][stocks].notna()
    ew = close[stocks].pct_change().mean(axis=1, skipna=True).fillna(0.0).to_numpy()
    ew_cum = np.concatenate([[1.0], np.cumprod(1 + ew)])
    spy_open, spy_close = p["open"][BENCHMARK].to_numpy(), p["close"][BENCHMARK].to_numpy()
    valid_np = valid.to_numpy()
    col = {t: j for j, t in enumerate(stocks)}

    out: dict = {"detectors": {}}
    lines = ["规则的公平基准（12 年回放，信号、成交、出场与回放完全相同；差值按每个信号配对，t 值按月度均值计算）",
             "规则 | 成交笔数 | 平均净收益 | 超额：SPY / 等权股票池 | 时机对照（同股随机日） 前半 / 后半 | 选股对照（同日随机股） 前半 / 后半"]
    for det in registry(include_candidates=True):
        span = det.trigger_window + det.exit.holding_limit(det.horizon_days)
        rows = []
        for ticker, i in base_rates.hit_positions(det.mask(p)):
            if i + span >= n or ticker not in col:
                continue
            got = trade(p, det, ticker, i, span)
            if not got:
                continue
            ret, a, b = got
            # timing control: same stock, random other dates
            days = np.flatnonzero(valid_np[: n - span - 1, col[ticker]])
            timing = []
            for _ in range(MAX_TRIES * DRAWS):
                if len(timing) == DRAWS or not days.size:
                    break
                j = int(rng.choice(days))
                if abs(j - i) < 10:
                    continue
                c = trade(p, det, ticker, j, span)
                if c:
                    timing.append(c[0])
            # selection control: same date, random other stocks
            names = [stocks[k] for k in np.flatnonzero(valid_np[i]) if stocks[k] != ticker]
            select = []
            for _ in range(MAX_TRIES * DRAWS):
                if len(select) == DRAWS or not names:
                    break
                c = trade(p, det, names[int(rng.integers(len(names)))], i, span)
                if c:
                    select.append(c[0])
            rows.append({
                "date": p.dates[i], "ret": ret,
                "spy": float(spy_close[b] / spy_open[a] - 1),
                "ew": float(ew_cum[b + 1] / ew_cum[a] - 1),
                "timing": float(np.mean(timing)) if timing else None,
                "select": float(np.mean(select)) if select else None,
            })
        if not rows:
            continue
        dates = sorted(r["date"] for r in rows)
        mid = dates[len(dates) // 2]

        def edge(key: str, part=None) -> dict:
            rs = [r for r in rows if r[key] is not None and (part is None or (r["date"] < mid) == (part == "first"))]
            mean, t, months = monthly_t([r["date"] for r in rs], [r["ret"] - r[key] for r in rs])
            return {"edge": None if mean is None else round(mean, 5), "t": None if t is None else round(t, 2), "n": len(rs),
                    "months": months}

        net = np.array([r["ret"] for r in rows]) - ROUND_TRIP_COST
        res = {"label": det.label, "live": det.live, "n": len(rows), "avg_net": round(float(net.mean()), 5), "split": str(mid),
               **{f"vs_{k}": edge(k) for k in ("spy", "ew", "timing", "select")},
               **{f"vs_{k}_{h}": edge(k, h) for k in ("timing", "select") for h in ("first", "second")}}
        out["detectors"][det.name] = res

        def f(e: dict) -> str:
            return "—" if e["edge"] is None else f"{e['edge']:+.2%}" + (f"（t {e['t']:+.1f}）" if e["t"] is not None else "")

        lines.append(f"{det.label}{'' if det.live else '（候选）'} | {res['n']} | {res['avg_net']:+.2%} | "
                     f"{f(res['vs_spy'])} / {f(res['vs_ew'])} | {f(res['vs_timing_first'])} / {f(res['vs_timing_second'])} | "
                     f"{f(res['vs_select_first'])} / {f(res['vs_select_second'])}")
    text = "\n".join(lines)
    print(text)
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('rule_benchmarks', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": text, "p": db.jsonb(out)})
    return {k: v["n"] for k, v in out["detectors"].items()}


if __name__ == "__main__":
    run("eval_rules", study, alert=False)
