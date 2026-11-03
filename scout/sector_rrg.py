"""Sector rotation quadrants as a trading rule (pre-registered in `scout/knowledge/external_specs/sector_rrg_v1.yaml`).

Local only. Scout's own RRG (`scout.sectors`: rs_ratio over a 50-day mean, momentum over 10 sessions) is computed
daily for each sector ETF against SPY; at each week's last close a sector entering "improving" is bought and one
entering "weakening" (variant b: or falling back to "lagging") is sold, both at the next session's close. Trades
are compared with the other sectors over the same window; portfolios hold 1 / N per sector held, the rest in cash
or SPY. The same rule on the French 12 industries 1926–1998 is an outside check.
"""
from __future__ import annotations

import math
from datetime import date

import numpy as np
import pandas as pd

from scout import cross_asset as ca
from scout.sectors import MOMENTUM_LAG, RATIO_WINDOW, quadrant

KEY = "sector_rrg_v1"
SECTORS = ("XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY", "XLRE", "XLC")
VARIANTS = {"a": "进入改善买入，进入转弱卖出（你的规则）", "b": "同上，掉回落后也卖出"}
ROUND_TRIP = 0.0010
COST, COST_HIGH = 0.0005, 0.0020
T_BAR = 3.46
FRENCH_END = date(1998, 12, 21)


# ── data ──

def load_etf() -> tuple[pd.DataFrame, pd.Series, str]:
    px, rf = ca.load(SECTORS + ("SPY",))
    return px, rf, "SPY"


def load_french() -> tuple[pd.DataFrame, pd.Series, str]:
    path = ca.FRENCH / "12_Industry_Portfolios_Daily.csv"
    lines = path.read_text().splitlines()
    start = next(i for i, l in enumerate(lines) if "Value Weighted" in l) + 1
    rows = []
    for l in lines[start + 1:]:
        parts = [x.strip() for x in l.split(",")]
        if not parts[0].isdigit():
            break
        rows.append(parts)
    head = [x.strip() for x in lines[start].split(",")][1:]
    d = pd.DataFrame([r[1:] for r in rows], index=pd.to_datetime([r[0] for r in rows], format="%Y%m%d"), columns=head).astype(float)
    d = d.where(d > -99) / 100
    ff = pd.read_csv(ca.FRENCH / "F-F_Research_Data_Factors_daily.csv", skiprows=4, index_col=0)
    ff = ff[pd.to_numeric(ff.index, errors="coerce").notna()]
    ff.index = pd.to_datetime(ff.index.astype(str).str.strip(), format="%Y%m%d")
    ff = ff.astype(float) / 100
    d["MKT"] = ff["Mkt-RF"] + ff["RF"]
    d = d[d.index <= pd.Timestamp(FRENCH_END)]
    px = (1 + d.fillna(0.0)).cumprod()
    return px, ff["RF"].reindex(px.index).fillna(0.0), "MKT"


# ── signals (data up to row i only) ──

def rrg(px: pd.DataFrame, bench: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    rs = px.div(px[bench], axis=0)
    ratio = 100 * rs / rs.rolling(RATIO_WINDOW, min_periods=RATIO_WINDOW).mean()
    return ratio, 100 * ratio / ratio.shift(MOMENTUM_LAG)


def week_end_rows(index: pd.DatetimeIndex) -> list[int]:
    """Last session of each week; the final row only once its week is known to be complete (never here)."""
    w = index.to_period("W")
    return [i for i in range(len(index) - 1) if w[i] != w[i + 1]]


def signals(ratio: pd.DataFrame, mom: pd.DataFrame, ends: list[int], assets: list[str], variant: str
            ) -> tuple[list[dict], dict[int, set[str]]]:
    """Trades {asset, signal, entry, exit} (entry / exit = the row traded, the next session) and the set held
    after each week-end decision."""
    held: dict[str, int] = {}
    trades, sets = [], {}
    prev: dict[str, str | None] = {a: None for a in assets}
    r, m = ratio[assets].to_numpy(), mom[assets].to_numpy()
    n = len(ratio)
    for i in ends:
        for j, a in enumerate(assets):
            if not (np.isfinite(r[i, j]) and np.isfinite(m[i, j])):
                prev[a] = None
                continue
            q = quadrant(float(r[i, j]), float(m[i, j]))
            if a in held:
                if q == "weakening" or (variant == "b" and q == "lagging"):
                    trades.append({"asset": a, "signal": held.pop(a), "entry": None, "exit": i + 1})
            elif q == "improving" and prev[a] is not None and prev[a] != "improving" and i + 1 < n:
                held[a] = i
            prev[a] = q
        sets[i] = set(held)
    for a, s in held.items():                       # still open at the end: valued at the last row
        trades.append({"asset": a, "signal": s, "entry": None, "exit": None})
    for t in trades:
        t["entry"] = t["signal"] + 1
        t["exit"] = t["exit"] if t["exit"] is not None and t["exit"] < n else n - 1
    return sorted(trades, key=lambda t: t["entry"]), sets


# ── evaluation ──

def trade_table(px: pd.DataFrame, trades: list[dict], bench: str, assets: list[str]) -> pd.DataFrame:
    p = px.to_numpy()
    col = {a: k for k, a in enumerate(px.columns)}
    rows = []
    for t in trades:
        e, x = t["entry"], t["exit"]
        if x <= e:
            continue
        ret = p[x, col[t["asset"]]] / p[e, col[t["asset"]]] - 1 - ROUND_TRIP
        others = [p[x, col[o]] / p[e, col[o]] - 1 for o in assets
                  if o != t["asset"] and np.isfinite(p[e, col[o]]) and np.isfinite(p[x, col[o]])]
        rows.append({"asset": t["asset"], "entry": px.index[e], "exit": px.index[x], "days": x - e, "ret": ret,
                     "vs_others": ret - float(np.mean(others)) if others else np.nan,
                     "vs_bench": ret - (p[x, col[bench]] / p[e, col[bench]] - 1)})
    return pd.DataFrame(rows)


def trade_stats(tt: pd.DataFrame) -> dict:
    if tt.empty:
        return {"n": 0}
    split = tt["entry"].sort_values().iloc[len(tt) // 2]
    out = {"n": int(len(tt)), "median_days": float(tt["days"].median()), "win_vs_others": float((tt["vs_others"] > 0).mean()),
           "split": str(split.date())}
    for k in ("vs_others", "vs_bench", "ret"):
        m = tt.groupby(tt["entry"].dt.to_period("M"))[k].mean()
        out[k] = {**ca.tstat(m), "per_trade": float(tt[k].mean()),
                  "first": float(tt.loc[tt["entry"] < split, k].mean()), "second": float(tt.loc[tt["entry"] >= split, k].mean())}
    return out


def schedules(ratio: pd.DataFrame, mom: pd.DataFrame, ends: list[int], assets: list[str], sets: dict[int, set[str]],
              bench: str) -> dict[str, dict[int, dict]]:
    """Weekly targets: cash and tilt versions of the rule, and the equal-weight sectors held."""
    out: dict[str, dict[int, dict]] = {"cash": {}, "tilt": {}, "ew": {}}
    for i in ends:
        avail = [a for a in assets if np.isfinite(ratio.at[ratio.index[i], a]) and np.isfinite(mom.at[mom.index[i], a])]
        if len(avail) < 5:
            continue
        w = {a: 1 / len(avail) for a in sets.get(i, set())}
        out["cash"][i] = w
        out["tilt"][i] = {**w, bench: 1 - sum(w.values())} if sum(w.values()) < 1 else w
        out["ew"][i] = {a: 1 / len(avail) for a in avail}
    return out


def halves(ret: pd.Series, rf: pd.Series) -> dict:
    mid = ret.index[len(ret) // 2]
    return {"all": ca.stats(ret, rf), "first": ca.stats(ret[ret.index < mid], rf), "second": ca.stats(ret[ret.index >= mid], rf),
            "split": str(mid.date())}


def run_sample(px: pd.DataFrame, rf: pd.Series, bench: str, with_core: bool) -> dict:
    assets = [a for a in px.columns if a != bench]
    ratio, mom = rrg(px, bench)
    ends = week_end_rows(px.index)
    out: dict = {"start": str(px.index[0].date()), "end": str(px.index[-1].date()), "assets": assets, "variants": {}}
    for v in VARIANTS:
        trades, sets = signals(ratio, mom, ends, assets, v)
        tt = trade_table(px, trades, bench, assets)
        sch = schedules(ratio, mom, ends, assets, sets, bench)
        start = min(sch["ew"])
        res: dict = {"trades": trade_stats(tt), "portfolios": {}}
        for name in ("cash", "tilt"):
            sim = ca.simulate(px, rf, sch[name], COST)
            res["portfolios"][name] = {**halves(sim["ret"], rf), "invested": float(1 - sim["cash"].mean()),
                                       "turnover_year": float(sum(sim["turnover"]) / (len(sim["ret"]) / ca.YEAR)),
                                       "cagr_high_cost": ca.stats(ca.simulate(px, rf, sch[name], COST_HIGH)["ret"], rf)["cagr"]}
        out["variants"][v] = res
        if v == "a":
            benches = {"ew": sch["ew"], bench.lower(): {i: {bench: 1.0} for i in sch["ew"]}}
            if with_core:
                core = ca.schedules(px, rf, ["core_now"], 1)["core_now"]
                benches["core_now"] = {i: w for i, w in core.items() if i >= start}
            out["benches"] = {k: halves(ca.simulate(px, rf, s, COST)["ret"], rf) for k, s in benches.items()}
    return out


def decide(main: dict) -> dict:
    bench = main["benches"]["spy"]
    out = {}
    for v, res in main["variants"].items():
        tr, tilt = res["trades"], res["portfolios"]["tilt"]
        vo = tr.get("vs_others") or {}
        checks = {"both_halves": bool(vo and vo["first"] > 0 and vo["second"] > 0),
                  "t": bool(vo and vo.get("t") is not None and vo["t"] >= T_BAR),
                  "tilt_sharpe": all(tilt[h]["sharpe"] > bench[h]["sharpe"] for h in ("first", "second"))}
        out[v] = {**checks, "pass": all(checks.values())}
    return out


def run() -> dict:
    px, rf, bench = load_etf()
    main = run_sample(px, rf, bench, with_core=True)
    fpx, frf, fb = load_french()
    outside = run_sample(fpx, frf, fb, with_core=False)
    return {"key": KEY, "main": main, "french": outside, "decision": decide(main)}


# ── report ──

def _p(x: float | None, d: int = 1) -> str:
    return "—" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x * 100:+.{d}f}%"


def report_text(res: dict) -> str:
    lines = [f"板块轮动象限测试（预登记 {KEY}，只跑一次）"]
    for label, s in (("SPDR 板块 ETF", res["main"]), ("French 12 行业（外部印证）", res["french"])):
        b = "spy" if "spy" in s["benches"] else "mkt"
        lines.append(f"\n【{label}】{s['start']} 到 {s['end']}")
        for v, r in s["variants"].items():
            tr = r["trades"]
            if not tr.get("n"):
                lines.append(f"{VARIANTS[v]}：没有交易")
                continue
            vo, vb = tr["vs_others"], tr["vs_bench"]
            lines.append(f"{VARIANTS[v]}：{tr['n']} 笔，持有中位数 {tr['median_days']:.0f} 个交易日；"
                         f"相对同期其他板块每笔 {_p(vo['per_trade'], 2)}（月聚类 t {vo['t']:.2f}；前半 {_p(vo['first'], 2)}，后半 {_p(vo['second'], 2)}），"
                         f"跑赢其他板块的比例 {tr['win_vs_others'] * 100:.0f}%；相对基准每笔 {_p(vb['per_trade'], 2)}（t {vb['t']:.2f}）")
            for name, zh in (("cash", "现金版"), ("tilt", "倾斜版")):
                pf = r["portfolios"][name]
                lines.append(f"  {zh}：年化 {_p(pf['all']['cagr'])}，夏普 {pf['all']['sharpe']:.2f}（前 {pf['first']['sharpe']:.2f} / 后 {pf['second']['sharpe']:.2f}），"
                             f"最大回撤 {_p(pf['all']['max_dd'], 0)}，平均仓位 {pf['invested'] * 100:.0f}%，年换手 {pf['turnover_year']:.1f} 倍，"
                             f"成本 0.2% 时年化 {_p(pf['cagr_high_cost'])}")
        for k, zh in (("ew", "板块等权一直持有"), (b, "基准买入持有"), ("core_now", "SPY + 大盘过滤")):
            if k in s["benches"]:
                x = s["benches"][k]
                lines.append(f"  对照·{zh}：年化 {_p(x['all']['cagr'])}，夏普 {x['all']['sharpe']:.2f}（前 {x['first']['sharpe']:.2f} / 后 {x['second']['sharpe']:.2f}），"
                             f"最大回撤 {_p(x['all']['max_dd'], 0)}")
    lines.append("\n判定（主样本）：" + "；".join(
        f"{VARIANTS[v]} → {'通过' if d['pass'] else '不通过'}（两半为正 {'✓' if d['both_halves'] else '✗'}，t ≥ {T_BAR} {'✓' if d['t'] else '✗'}，"
        f"倾斜版两半夏普高于 SPY {'✓' if d['tilt_sharpe'] else '✗'}）" for v, d in res["decision"].items()))
    return "\n".join(lines)
