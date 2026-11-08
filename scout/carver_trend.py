"""Carver-style multi-asset trend following (pre-registered in `scout/knowledge/external_specs/carver_trend_v1.yaml`).

Local only: the 12 asset-class ETFs of `scout.cross_asset` (Sharadar closeadj, French RF). The method follows Rob
Carver's Systematic Trading / pysystemtrade (GPL-3; principles only, nothing copied): six EWMAC speeds scaled
with the book's fixed forecast scalars and capped at ±20; a speed limit drops, per asset and month, rules whose
annual cost in Sharpe units exceeds 0.13; allowed rules are equally weighted and multiplied by the forecast
diversification multiplier; positions target 12 % annual volatility with handcrafted asset-class weights and the
instrument diversification multiplier, traded at the next close outside a 10 % buffer. Every estimate uses data
up to the day it is used.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from scout import cross_asset as ca
from scout import gate

KEY = "carver_trend_v1"
SPEEDS = ((2, 8), (4, 16), (8, 32), (16, 64), (32, 128), (64, 256))
RULES = [f"{a}_{b}" for a, b in SPEEDS]
SCALARS = dict(zip(RULES, (10.6, 7.5, 5.3, 3.75, 2.65, 1.87)))
CAP, AVG_ABS = 20.0, 10.0
VOL_SPAN, VOL_SLOW, VOL_FAST_SHARE, DAYS = 35, 2520, 0.7, 256
SPEED_LIMIT = 0.13
DM_MAX, DM_SMOOTH, DM_MIN_WEEKS = 2.5, 125, 52
VOL_TARGET = 0.12
BUFFER = 0.10
COST, COST_HIGH = 0.0005, 0.002
FINANCING = 0.005
MIN_ASSETS = 6
TRIALS = 4                                     # family cross_asset_trend: the three trials of cross_asset_trend_v1, then this
GROUPS = {"equity": ("SPY", "IWM", "EFA", "EEM", "VNQ"), "bonds": ("TLT", "IEF", "LQD", "TIP"),
          "commodities": ("GLD", "DBC"), "dollar": ("UUP",)}
LABELS = {"system": "完整系统（多速度合成 + 速度上限）", "hold": "对照：同样资产一直做多（风险平价）", "no_speed_limit": "6 个速度全用（无速度上限）",
          "long_only": "只做多版本", "cost_high": "单边成本 0.2%", **{f"single_{a}_{b}": f"只用 EWMAC {a}/{b}" for a, b in SPEEDS}}


# ── signals (each value uses data up to its own day) ──

def mixed_vol(px: pd.DataFrame) -> pd.DataFrame:
    """Annualised volatility: 70 % of the 35-day exponentially weighted std of daily returns, 30 % of its own
    average over the last ten years (pysystemtrade's mixed_vol_calc defaults)."""
    fast = px.pct_change().ewm(span=VOL_SPAN, min_periods=10).std() * math.sqrt(DAYS)
    slow = fast.rolling(VOL_SLOW, min_periods=1).mean()
    return VOL_FAST_SHARE * fast + (1 - VOL_FAST_SHARE) * slow


def forecasts(px: pd.DataFrame, vol: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Scaled, capped EWMAC forecasts; NaN until an asset has 256 prices."""
    price_vol = vol / math.sqrt(DAYS) * px                       # daily volatility in price units
    seen = px.notna().cumsum()
    out = {}
    for (a, b), name in zip(SPEEDS, RULES):
        raw = (px.ewm(span=a, min_periods=1).mean() - px.ewm(span=b, min_periods=1).mean()) / price_vol
        out[name] = (raw * SCALARS[name]).clip(-CAP, CAP).where(seen >= DAYS)
    return out


def turnover(f: pd.DataFrame) -> pd.DataFrame:
    """Per asset, round trips per year of a forecast so far: Σ|Δf| ÷ years ÷ 20."""
    d = f.diff().abs()
    n = f.notna().cumsum()
    return (d.cumsum() / (n / DAYS) / (2 * AVG_ABS)).where(n >= DAYS)


def allowed(fc: dict, vol: pd.DataFrame, rows: list[int], cost: float) -> dict[int, pd.DataFrame]:
    """Month-end row → (asset × rule) True where the rule's annual cost in Sharpe units is within the speed limit.
    Turnover is pooled: the median over the assets with a value that day."""
    turn = {s: turnover(f).median(axis=1) for s, f in fc.items()}
    out = {}
    for i in rows:
        per_trade = 2 * cost / vol.iloc[i]
        out[i] = pd.DataFrame({s: turn[s].iloc[i] * per_trade <= SPEED_LIMIT for s in fc})
    return out


def weekly_rows(index: pd.DatetimeIndex) -> np.ndarray:
    w = index.to_period("W")
    return np.flatnonzero(np.r_[w[1:] != w[:-1], True])


def floor_corr(x: pd.DataFrame, cols: list) -> np.ndarray:
    """Correlation floored at 0; pairs without data count as 1 (no diversification credit)."""
    c = x.corr(min_periods=DM_MIN_WEEKS).reindex(index=cols, columns=cols).to_numpy()
    c = np.where(np.isfinite(c), np.clip(c, 0.0, 1.0), 1.0)
    np.fill_diagonal(c, 1.0)
    return c


def div_mult(w: np.ndarray, h: np.ndarray) -> float:
    if w.sum() <= 0:
        return 1.0
    risk = math.sqrt(max(float(w @ h @ w), 1e-12))
    return min(1.0 / risk, DM_MAX)


def smooth(points: dict[int, pd.Series], index: pd.DatetimeIndex, cols) -> pd.DataFrame:
    """Month-end values carried forward, then a 125-session exponential smoothing; 1 before the first value."""
    df = pd.DataFrame(np.nan, index=index, columns=cols)
    for i, v in points.items():
        df.iloc[i] = v.reindex(cols).to_numpy()
    return df.ffill().fillna(1.0).ewm(span=DM_SMOOTH).mean()


def combined(fc: dict, rows: list[int], rules: dict[int, pd.DataFrame] | None, index, assets) -> pd.DataFrame:
    """FDM × mean of the allowed rules (`rules` None = all six), capped; NaN before the first month end."""
    wk = weekly_rows(index)
    names = list(fc)
    fdm_pts, allow_daily = {}, {}
    for i in rows:
        rows_w = wk[wk <= i]
        pooled = pd.concat([pd.DataFrame({s: fc[s].iloc[rows_w][a].to_numpy() for s in names}) for a in assets], ignore_index=True)
        enough = pooled.dropna().shape[0] >= DM_MIN_WEEKS
        h = floor_corr(pooled, names) if enough else None
        ok = rules[i] if rules is not None else pd.DataFrame(True, index=assets, columns=names)
        vals = {}
        for a in assets:
            w = ok.loc[a].to_numpy(dtype=float) if a in ok.index else np.zeros(len(names))
            w = w / w.sum() if w.sum() else w
            vals[a] = div_mult(w, h) if enough and w.sum() else 1.0
        fdm_pts[i] = pd.Series(vals)
        allow_daily[i] = ok
    fdm = smooth(fdm_pts, index, assets)
    out = pd.DataFrame(np.nan, index=index, columns=assets)
    month_rows = sorted(allow_daily)
    for k, i in enumerate(month_rows):
        end = month_rows[k + 1] if k + 1 < len(month_rows) else len(index) - 1
        sl = slice(i, end)                      # the month-end decision applies from that close to the next month end
        ok = allow_daily[i]
        num = sum(fc[s].iloc[sl].fillna(0.0) * ok[s].reindex(assets).fillna(False).astype(float) for s in names)
        cnt = sum(ok[s].reindex(assets).fillna(False).astype(float) for s in names)
        avail = fc[names[-1]].iloc[sl].notna()
        mean = num.div(cnt.replace(0, np.nan), axis=1).where(avail)
        out.iloc[sl] = (mean * fdm.iloc[sl]).clip(-CAP, CAP).to_numpy()
    return out


def single(fc: dict, s: str, rows: list[int]) -> pd.DataFrame:
    f = fc[s].copy()
    f.iloc[: rows[0]] = np.nan
    return f


# ── positions and simulation ──

def instrument_weights(avail: pd.DataFrame) -> pd.DataFrame:
    """Handcrafted: equal across the asset-class groups with an eligible asset, equal within a group."""
    w = pd.DataFrame(0.0, index=avail.index, columns=avail.columns)
    groups = [[a for a in g if a in avail.columns] for g in GROUPS.values()]
    live = sum((avail[g].any(axis=1)).astype(float) for g in groups if g)
    for g in groups:
        if not g:
            continue
        n = avail[g].sum(axis=1).replace(0, np.nan)
        for a in g:
            w[a] = (avail[a].astype(float) / n / live.replace(0, np.nan)).fillna(0.0)
    return w


def positions(f: pd.DataFrame, px: pd.DataFrame, rf: pd.Series, vol: pd.DataFrame, rows: list[int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(target position, buffer) as fractions of capital, with the IDM from subsystem returns."""
    avail = f.notna()
    w = instrument_weights(avail)
    ex = px.pct_change().sub(rf, axis=0)
    sub = (f / AVG_ABS * VOL_TARGET / vol).shift(1) * ex
    wk_sub = sub.groupby(sub.index.to_period("W")).sum(min_count=1)
    wk_end = sub.index.to_period("W")
    cols = list(px.columns)
    pts = {}
    for i in rows:
        hist = wk_sub[wk_sub.index <= wk_end[i]]
        wi = w.iloc[i].to_numpy()
        pts[i] = pd.Series({"idm": div_mult(wi, floor_corr(hist, cols)) if hist.dropna(how="all").shape[0] >= DM_MIN_WEEKS else 1.0})
    idm = smooth(pts, px.index, ["idm"])["idm"]
    avg = w.mul(idm, axis=0) * VOL_TARGET / vol
    return (avg * f / AVG_ABS).fillna(0.0), (BUFFER * avg.abs()).fillna(0.0)


def simulate(px: pd.DataFrame, rf: pd.Series, target: pd.DataFrame, buf: pd.DataFrame, start: int, cost: float) -> dict:
    """Daily returns from row start + 1: decided at close t, traded at close t + 1 to the buffer's edge;
    notional positions drift with prices; cost on traded notional, financing on shorts and leverage above 1."""
    r = px.pct_change().to_numpy()
    rfa = rf.to_numpy()
    tg, bf = target.to_numpy(), buf.to_numpy()
    n = np.zeros(px.shape[1])
    v = 1.0
    rets, lev, traded = [], [], []
    for t in range(start + 1, len(px)):
        rt = np.nan_to_num(r[t])
        fee = FINANCING / 252 * (np.abs(n[n < 0]).sum() + max(0.0, n[n > 0].sum() - v))
        v_new = v * (1 + rfa[t]) + float(n @ (rt - rfa[t])) - fee
        n = n * (1 + rt)
        cur = n / v_new
        goal = tg[t - 1]
        lo, hi = goal - bf[t - 1], goal + bf[t - 1]
        new = np.where(cur < lo, lo, np.where(cur > hi, hi, cur))
        new = np.where(np.isfinite(px.iloc[t].to_numpy()), new, 0.0)
        trade = float(np.abs(new - cur).sum())
        v_new -= cost * trade * v_new
        n = new * v_new
        rets.append(v_new / v - 1)
        lev.append(float(np.abs(new).sum()))
        traded.append(trade)
        v = v_new
    idx = px.index[start + 1:]
    return {"ret": pd.Series(rets, index=idx), "leverage": float(np.mean(lev)), "turnover": float(np.sum(traded) / (len(traded) / 252))}


# ── run ──

def run() -> dict:
    px, rf = ca.load()
    assets = list(px.columns)
    vol = mixed_vol(px)
    fc = forecasts(px, vol)
    elig = fc[RULES[-1]].notna().sum(axis=1)
    first = int(np.argmax(elig.to_numpy() >= MIN_ASSETS))
    rows = [i for i in ca.month_end_rows(px.index) if i >= first]
    start = rows[0]
    out, extra = {}, {}

    def go(name: str, f: pd.DataFrame, cost: float = COST) -> None:
        tgt, buf = positions(f, px, rf, vol, rows)
        out[name] = simulate(px, rf, tgt, buf, start, cost)

    limits = allowed(fc, vol, rows, COST)
    system_f = combined(fc, rows, limits, px.index, assets)
    go("system", system_f)
    hold_f = pd.DataFrame(AVG_ABS, index=px.index, columns=assets).where(fc[RULES[-1]].notna())
    hold_f.iloc[:start] = np.nan
    go("hold", hold_f)
    go("no_speed_limit", combined(fc, rows, None, px.index, assets))
    go("long_only", system_f.clip(lower=0.0))
    go("cost_high", combined(fc, rows, allowed(fc, vol, rows, COST_HIGH), px.index, assets), COST_HIGH)
    for s in RULES:
        go(f"single_{s}", single(fc, s, rows))
    extra["excluded"] = {str(px.index[i].year): [f"{a}:{s.replace('_', '/')}" for a in assets for s in RULES
                                                  if pd.notna(vol.iloc[i][a]) and not bool(limits[i].loc[a, s])]
                         for i in rows if i == max(j for j in rows if px.index[j].year == px.index[i].year)}
    extra["avg_abs_forecast"] = {s.replace("_", "/"): float(fc[s].iloc[start:].abs().stack().mean()) for s in RULES}
    extra["avg_abs_combined"] = float(system_f.iloc[start:].abs().stack().mean())
    sch = ca.schedules(px, rf, list(ca.BENCHES), ca.MIN_ASSETS)
    bench = {k: ca.simulate(px, rf, sch[k], ca.COST) for k in ("core_now", "spy", "sixty_forty")}
    return {"px_end": str(px.index[-1].date()), "start": str(px.index[start].date()), "results": out, "bench": bench, "extra": extra, "rf": rf}


# ── evaluation ──

def evaluate(raw: dict) -> dict:
    rf = raw["rf"]
    rets = {k: v["ret"] for k, v in raw["results"].items()} | {k: v["ret"] for k, v in raw["bench"].items()}
    common = max(r.index[0] for r in rets.values())
    rets = {k: r[r.index >= common] for k, r in rets.items()}
    months = ca.monthly(rets["system"]).index
    mid = months[len(months) // 2]
    first = lambda r: r[r.index.to_period("M") < mid]  # noqa: E731
    second = lambda r: r[r.index.to_period("M") >= mid]  # noqa: E731
    spy_m = ca.monthly(rets["spy"])
    worst = spy_m[spy_m <= spy_m.quantile(0.10)].index
    out = {"from": str(common.date()), "split": str(mid), "variants": {}}
    for k, r in rets.items():
        m = ca.monthly(r)
        s = ca.stats(r, rf)
        s.update({"sharpe_first": ca.stats(first(r), rf)["sharpe"], "sharpe_second": ca.stats(second(r), rf)["sharpe"],
                  "years": {y: float((1 + r[r.index.year == y]).prod() - 1) for y in (2008, 2020, 2022)},
                  "crisis": float(m.reindex(worst).mean()), "corr_spy": float(m.corr(spy_m))})
        if k in raw["results"]:
            s.update({"leverage": raw["results"][k]["leverage"], "turnover": raw["results"][k]["turnover"]})
        out["variants"][k] = s
    diff = ca.monthly(rets["system"]) - ca.monthly(rets["hold"])
    out["system_vs_hold"] = {"all": ca.tstat(diff), "first": ca.tstat(diff[diff.index < mid]), "second": ca.tstat(diff[diff.index >= mid])}
    core = ca.monthly(rets["system"]) - ca.monthly(rets["core_now"])
    out["system_vs_core"] = {"all": ca.tstat(core), "first": ca.tstat(core[core.index < mid]), "second": ca.tstat(core[core.index >= mid])}
    return out


def decide(ev: dict) -> dict:
    v = ev["variants"]
    req = gate.required_t(TRIALS)
    d = ev["system_vs_hold"]
    singles = [k for k in v if k.startswith("single_")]
    half = lambda k, h: v[k][f"sharpe_{h}"]  # noqa: E731
    system = {"sharpe_both_halves": all(half("system", h) > half("hold", h) for h in ("first", "second")),
              "diff_both_halves": bool(d["first"]["mean"] > 0 and d["second"]["mean"] > 0),
              "t": bool(d["all"]["t"] is not None and d["all"]["t"] >= req)}
    combination = all(half("no_speed_limit", h) > np.mean([half(s, h) for s in singles]) for h in ("first", "second"))
    speed = all(half("system", h) > half("no_speed_limit", h) for h in ("first", "second"))
    return {"passed": all(system.values()), "system": system, "combination_helps": bool(combination),
            "speed_limit_helps": bool(speed), "required_t": req}


def _p(x: float | None, d: int = 1) -> str:
    return "—" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x:+.{d}%}"


def report_text(res: dict) -> str:
    ev, dec = res["evaluation"], res["decision"]
    order = ["system", "hold", "no_speed_limit", *[f"single_{s}" for s in RULES], "long_only", "cost_high", "core_now", "spy", "sixty_forty"]
    names = {**LABELS, "core_now": "现行核心规则（SPY + 大盘过滤）", "spy": "SPY 买入持有", "sixty_forty": "60% SPY + 40% IEF"}
    lines = [f"Carver 式多品种趋势（carver_trend_v1），{ev['from']} 起，前后两半以 {ev['split']} 分界；本族（cross_asset_trend）第 {TRIALS} 次，t 门槛 {dec['required_t']}",
             "方案 | 年化 | 波动 | 夏普 全部/前半/后半 | 最大回撤 | 最差 12 个月 | 2008 / 2020 / 2022 | SPY 最差 10% 月份 | 与 SPY 相关 | 杠杆 | 年换手"]
    for k in order:
        s = ev["variants"].get(k)
        if not s:
            continue
        y = {int(k): x for k, x in s["years"].items()}          # JSON keys come back as strings
        lines.append(f"{names[k]} | {_p(s['cagr'])} | {s['vol']:.1%} | {s['sharpe']:.2f} / {s['sharpe_first']:.2f} / {s['sharpe_second']:.2f} | "
                     f"{_p(s['max_dd'])} | {_p(s['worst_12m'])} | {_p(y[2008])} / {_p(y[2020])} / {_p(y[2022])} | {_p(s['crisis'], 2)} | "
                     f"{s['corr_spy']:.2f} | {s.get('leverage', float('nan')):.2f} | {s.get('turnover', float('nan')):.1f}")
    d, c = ev["system_vs_hold"], ev["system_vs_core"]
    lines.append(f"完整系统 − 一直做多：每月 {_p(d['all']['mean'], 2)}（t {d['all']['t']:.2f}；前半 {_p(d['first']['mean'], 2)} / 后半 {_p(d['second']['mean'], 2)}）")
    lines.append(f"完整系统 − 现行核心规则：每月 {_p(c['all']['mean'], 2)}（t {c['all']['t']:.2f}；前半 {_p(c['first']['mean'], 2)} / 后半 {_p(c['second']['mean'], 2)}）")
    lines.append(f"判定：系统 {'通过' if dec['passed'] else '未通过'} {dec['system']}；多速度合成有用 = {dec['combination_helps']}；速度上限有用 = {dec['speed_limit_helps']}")
    ex = res["extra"]
    lines.append("实际平均绝对预测（书中缩放值的目标是 10）：" + "，".join(f"{k} {v:.1f}" for k, v in ex["avg_abs_forecast"].items())
                 + f"；合成后 {ex['avg_abs_combined']:.1f}")
    lines.append("速度上限剔除（各年最后一个月末，资产:规则）：" + "；".join(f"{y} {len(v)} 个" for y, v in ex["excluded"].items()))
    last = list(ex["excluded"])[-1] if ex["excluded"] else None
    if last:
        lines.append(f"  {last} 年剔除：" + "、".join(ex["excluded"][last]))
    return "\n".join(lines)
