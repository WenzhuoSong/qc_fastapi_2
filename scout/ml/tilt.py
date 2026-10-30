"""Tilt-portfolio test (pre-registered in `scout/knowledge/external_specs/ml_tilt_v1.yaml`): the composite
score used the way professionals use a weak signal, as long-only tilts over the whole universe with
turnover control, against the equal-weight universe; daily simulation on the local Sharadar prices.
The confirmation result file doubles as its run-once lock.
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from scout import sharadar_research as sr
from scout.ml import baselines as B
from scout.ml import features as F
from scout.ml import operators as op
from scout.simulate import ROUND_TRIP_COST

ONE_WAY = ROUND_TRIP_COST / 2
BAND = 0.5                                     # × 1 / N
VARIANTS = {"m_full": (4, False), "m_band": (4, True), "w_band": (1, True)}   # (sample steps, band)
CONFIRM_START = "2016-01-01"
RESULT = F.OUT / "tilt_confirm_v1.json"
DISCOVERY_RESULT = F.OUT / "tilt_discovery_v1.json"


def composite(df: pd.DataFrame) -> pd.Series:
    return (-df["share_growth"] + df["gp_assets"] + df["ear"] + df["mom_12_1"]) / 4


def daily_returns(p: sr.Prices) -> np.ndarray:
    """(dates, stocks) close-to-close returns: 0 while halted (made up on resumption); the session after a
    stock's last close carries its delisting adjustment, 0 afterwards."""
    c = pd.DataFrame(p.closeadj).ffill().to_numpy()
    with np.errstate(all="ignore"):
        r = c[1:] / c[:-1] - 1
    r = np.vstack([np.zeros((1, c.shape[1])), np.nan_to_num(r, nan=0.0)])
    ok = np.isfinite(p.closeadj)
    for j in range(c.shape[1]):
        idx = np.flatnonzero(ok[:, j])
        if idx.size and idx[-1] < len(r) - 1:
            r[idx[-1] + 1, j] = p.haircut[j]
    return r


def targets(cols: np.ndarray, s: np.ndarray, n_stocks: int) -> tuple[np.ndarray, np.ndarray]:
    """Tilt and benchmark weights for one decision: (1 + s) / N and 1 / N over the day's members."""
    n = len(cols)
    tilt, bench = np.zeros(n_stocks), np.zeros(n_stocks)
    tilt[cols] = (1 + np.nan_to_num(s, nan=0.0)) / n
    bench[cols] = 1.0 / n
    return tilt, bench


def rebalance(w: np.ndarray, target: np.ndarray, n: int, band: bool) -> np.ndarray:
    if not band:
        return target.copy()
    move = (np.abs(target - w) > BAND / n) | ((target == 0) & (w > 0)) | ((w == 0) & (target > 0))
    new = np.where(move, target, w)
    return new / new.sum()


def simulate(R: np.ndarray, decisions: list[tuple[int, np.ndarray, np.ndarray]], end: int, band: bool) -> dict:
    """decisions = [(day i, member columns, scores)] in order; each trades at the close of day i + 1.
    Returns daily portfolio / benchmark returns from the first trade to `end` and the turnover per trade."""
    S = R.shape[1]
    trade_on = {i + 1: (cols, s) for i, cols, s in decisions}
    start = min(trade_on)
    w, b = np.zeros(S), np.zeros(S)
    port, bench, days, turns = [], [], [], []
    for t in range(start, end + 1):
        if t > start:
            pw, pb = w * (1 + R[t]), b * (1 + R[t])
            rp, rb = pw.sum() - 1, pb.sum() - 1
            w, b = pw / pw.sum(), pb / pb.sum()
        else:
            rp = rb = 0.0
        if t in trade_on:
            cols, s = trade_on[t]
            tgt, bt = targets(cols, s, S)
            new = rebalance(w, tgt, len(cols), band) if w.sum() > 0 else tgt
            turn = float(np.abs(new - w).sum())
            rp = (1 + rp) * (1 - ONE_WAY * turn) - 1
            w, b = new, bt
            turns.append(turn)
        if t > start:
            port.append(rp)
            bench.append(rb)
            days.append(t)
    return {"days": days, "port": np.array(port), "bench": np.array(bench), "turnover": turns}


def stats(sim: dict, dates: pd.DatetimeIndex, spy: np.ndarray, regime: pd.Series) -> dict:
    idx = dates[sim["days"]]
    d = pd.DataFrame({"p": sim["port"], "b": sim["bench"]}, index=idx)
    m = (1 + d).groupby(idx.to_period("M")).prod() - 1
    act = m["p"] - m["b"]
    half = len(act) // 2

    def part(x: pd.Series) -> dict:
        sd = x.std()
        return {"ann_active": round(float(x.mean() * 12), 5), "te": round(float(sd * np.sqrt(12)), 5),
                "ir": round(float(x.mean() * 12 / (sd * np.sqrt(12))), 3) if sd > 0 else None,
                "t": round(float(x.mean() / sd * np.sqrt(len(x))), 2) if sd > 0 else None, "months": len(x)}

    rel = np.cumprod(1 + d["p"]) / np.cumprod(1 + d["b"])
    years = len(d) / 252
    ann = lambda r: float(np.prod(1 + r) ** (1 / years) - 1)  # noqa: E731
    sharpe = lambda r: float(r.mean() / r.std() * np.sqrt(252))  # noqa: E731
    sp = spy[sim["days"]]
    reg = act.groupby(regime.reindex(act.index).to_numpy()).mean() * 12
    return {"period": [str(idx[0].date()), str(idx[-1].date())], "all": part(act), "first": part(act.iloc[:half]),
            "second": part(act.iloc[half:]), "split": str(act.index[half]),
            "turnover_one_way_per_year": round(float(np.sum(sim["turnover"]) / 2 / years), 3),
            "max_relative_drawdown": round(float((rel / np.maximum.accumulate(rel) - 1).min()), 4),
            "portfolio": {"ann": round(ann(d["p"]), 4), "sharpe": round(sharpe(d["p"]), 3)},
            "benchmark": {"ann": round(ann(d["b"]), 4), "sharpe": round(sharpe(d["b"]), 3)},
            "spy_ann": round(ann(pd.Series(sp)), 4),
            "active_by_regime": {str(k): round(float(v), 5) for k, v in reg.items()}}


def load(lo: str, hi: str | None) -> tuple[pd.DataFrame, sr.Prices, np.ndarray, np.ndarray, pd.DatetimeIndex]:
    """Decision rows in [lo, hi] and prices up to hi (the data end when None); nothing later is read."""
    cols = ["date", "ticker", "i", "regime", "share_growth", "gp_assets", "ear", "mom_12_1"]
    df = pd.read_parquet(F.OUT / "features_v1.parquet", columns=cols)
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= lo) & ((df["date"] <= hi) if hi else True)].reset_index(drop=True)
    df["composite"] = composite(df)
    dates = sr.trading_days()
    if hi:
        dates = dates[dates <= pd.Timestamp(hi)]
    p = sr.prices(sorted(df["ticker"].unique()), dates)
    funds = sr._read("funds", ["ticker", "date", "closeadj"], filters=[("ticker", "in", ["SPY"])])
    funds["date"] = pd.to_datetime(funds["date"])
    spy_px = funds.set_index("date")["closeadj"].reindex(dates).ffill().to_numpy(dtype=float)
    spy = np.concatenate([[0.0], spy_px[1:] / spy_px[:-1] - 1])
    df["j"] = df["ticker"].map(p.col).astype(int)
    return df, p, daily_returns(p), spy, dates


def decisions(df: pd.DataFrame, score: str, step: int) -> list[tuple[int, np.ndarray, np.ndarray]]:
    out = []
    for k, (i, g) in enumerate(df.groupby("i", sort=True)):
        if k % step == 0:
            out.append((int(i), g["j"].to_numpy(), op.rank_pm1(g[score]).to_numpy()))
    return out


def regimes(df: pd.DataFrame) -> pd.Series:
    """The market regime of each month's first decision date (labels from features.ORDINAL)."""
    first = df.groupby("date")["regime"].first()
    first.index = first.index.to_period("M")
    first = first[~first.index.duplicated()]
    names = F.ORDINAL["regime"]
    return first.map(lambda k: names[int(k)] if 0 <= k < len(names) else "unknown")


def run_discovery() -> dict:
    df, p, R, spy, dates = load(B.DISCOVERY[0], B.DISCOVERY[1])
    b4 = pd.read_parquet(F.OUT / "b4_scores_y21.parquet")
    df = df.merge(b4, on=["date", "ticker"], how="left")
    reg, end = regimes(df), len(dates) - 1
    res = {"composite": {}, "b4_descriptive": {}}
    for name, (step, band) in VARIANTS.items():
        res["composite"][name] = stats(simulate(R, decisions(df, "composite", step), end, band), dates, spy, reg)
        res["b4_descriptive"][name] = stats(simulate(R, decisions(df, "b4", step), end, band), dates, spy, reg)
    res["chosen"] = max(VARIANTS, key=lambda k: res["composite"][k]["all"]["ir"] or -np.inf)
    DISCOVERY_RESULT.write_text(json.dumps(res, ensure_ascii=False, indent=2))
    return res


def run_confirmation() -> dict:
    if RESULT.exists():
        raise SystemExit(f"{RESULT} exists: the confirmation runs once (ml_tilt_v1.yaml)")
    chosen = json.loads(DISCOVERY_RESULT.read_text())["chosen"]
    step, band = VARIANTS[chosen]
    df, p, R, spy, dates = load(CONFIRM_START, None)
    s = stats(simulate(R, decisions(df, "composite", step), len(dates) - 1, band), dates, spy, regimes(df))
    s["passed"] = bool(s["all"]["t"] is not None and s["all"]["t"] >= 3 and s["first"]["ann_active"] > 0
                       and s["second"]["ann_active"] > 0)
    res = {"spec": "ml_tilt_v1", "variant": chosen, "second_use_of_2016_plus": True, "composite": s}
    RESULT.write_text(json.dumps(res, ensure_ascii=False, indent=2))
    return res


def _line(name: str, s: dict) -> str:
    q = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731
    a, f, g = s["all"], s["first"], s["second"]
    return (f"{name} | {q(a['ann_active'])} | {a['te']:.2%} | {a['ir']}（t {a['t']}） | {q(f['ann_active'])} / {q(g['ann_active'])} | "
            f"{s['turnover_one_way_per_year']:.0%} | {s['max_relative_drawdown']:.1%} | "
            f"{s['portfolio']['ann']:.1%} / {s['benchmark']['ann']:.1%} / SPY {s['spy_ann']:.1%}")


def report_text(disc: dict, conf: dict | None = None) -> str:
    head = "变体 | 年化主动收益（扣成本） | 跟踪误差 | 信息比率（t） | 前半 / 后半 | 年单边换手 | 相对最大回撤 | 组合 / 基准 / SPY 年化"
    lines = [f"倾斜组合 · 挖掘期 {B.DISCOVERY[0][:7]} 至 {B.DISCOVERY[1][:7]}（只做多，等权基准，单边成本 {ONE_WAY:.2%}）", head]
    lines += [_line(f"综合分 {k}", v) for k, v in disc["composite"].items()]
    lines += [_line(f"b4（只作描述）{k}", v) for k, v in disc["b4_descriptive"].items()]
    lines.append(f"按预登记选中：{disc['chosen']}")
    if conf:
        c = conf["composite"]
        lines += [f"\n确认期 {c['period'][0]} 至 {c['period'][1]}（2016 年以后数据的第二次使用），变体 {conf['variant']}", head,
                  _line("综合分", c), f"通过：{'是' if c['passed'] else '否'}"]
    return "\n".join(lines)
