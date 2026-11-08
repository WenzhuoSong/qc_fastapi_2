"""Core rule plus a 20 % risk-parity sleeve on unseen years (pre-registered in
`scout/knowledge/external_specs/risk_parity_sleeve_v1.yaml`).

Local only: the US market from Kenneth French, 10-year Treasury total returns built from FRED DGS10 month-end yields
(Swinkels 2019: buy a 10-year par bond at last month's yield, price it at this month's yield with 10 − 1/12 years
left), cash at French RF. The sleeve holds equal risk in stocks and bonds (1 / 36-month volatility), scaled to 12 %
ex-ante volatility from the 36-month covariance, decided at each month end for the next month; leverage above 1 pays
RF + 0.5 %. Judged on 1965-01..2003-08 (2003-09 onward was seen in trend_sleeve_v1).
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from scout import cross_asset as ca
from scout import trend_sleeve as ts

KEY = "risk_parity_sleeve_v1"
WINDOW, VOL_TARGET, FINANCING, COST = 36, 0.12, 0.005, 0.0005
WEIGHT = 0.2
SMA_MONTHS = 10
JUDGED = (pd.Period("1965-01", "M"), pd.Period("2003-08", "M"))
MIN_DD_GAIN, MIN_T = 0.03, -2.0
LABELS = {"core_rp": "80% 核心 + 20% 风险平价", "core_now": "现行核心规则（美股 + 10 个月均线）", "core_cash": "80% 核心 + 20% 现金",
          "core_bonds": "80% 核心 + 20% 国债（不加杠杆）", "rp": "风险平价配置单独", "equity": "美股买入持有", "sixty_forty": "60% 股 / 40% 债"}


# ── data ──

def month_end_yields(daily: list[tuple]) -> pd.Series:
    s = pd.Series({pd.Timestamp(d): v / 100 for d, v in daily}).sort_index()
    return s.groupby(s.index.to_period("M")).last()


def bond_price(coupon: float, y: float, years: float) -> float:
    """Price per 1 of face of a bond paying `coupon` semi-annually, `years` to maturity, at yield y."""
    n, c, k = years * 2, coupon / 2, y / 2
    if abs(k) < 1e-12:
        return c * n + 1
    return c * (1 - (1 + k) ** -n) / k + (1 + k) ** -n


def bond_returns(y: pd.Series) -> pd.Series:
    """Monthly total return of a 10-year par bond bought at last month's yield."""
    prev, cur = y.shift(1), y
    r = [bond_price(p, c, 10 - 1 / 12) - 1 + p / 12 if np.isfinite(p) and np.isfinite(c) else np.nan for p, c in zip(prev, cur)]
    return pd.Series(r, index=y.index).dropna()


# ── strategies (weights from data up to each month end, held the next month) ──

def core(eq: pd.Series, rf: pd.Series) -> pd.Series:
    idx = (1 + eq).cumprod()
    held = (idx >= idx.rolling(SMA_MONTHS).mean()).shift(1)
    ok = held.notna() & idx.rolling(SMA_MONTHS).mean().shift(1).notna()
    held = held[ok].astype(bool)
    switch = held.astype(int).diff().abs().fillna(0)
    return pd.Series(np.where(held, eq[held.index], rf[held.index]), index=held.index) - COST * switch


def sleeve(rets: pd.DataFrame, rf: pd.Series) -> tuple[pd.Series, pd.Series]:
    """(monthly returns, gross leverage) of the risk-parity sleeve."""
    ex = rets.sub(rf, axis=0)
    out, lev, prev = {}, {}, None
    for k in range(WINDOW, len(rets)):
        hist = rets.iloc[k - WINDOW:k]
        vol = hist.std(ddof=1) * math.sqrt(12)
        w = (1 / vol) / (1 / vol).sum()
        cov = hist.cov() * 12
        w = w * VOL_TARGET / math.sqrt(float(w @ cov @ w))
        m = rets.index[k]
        gross = float(w.sum())
        turn = float((w - prev).abs().sum()) if prev is not None else gross
        out[m] = rf.iloc[k] + float(w @ ex.iloc[k]) - FINANCING / 12 * max(0.0, gross - 1) - COST * turn
        lev[m] = gross
        prev = w * (1 + rets.iloc[k]) / (1 + out[m])          # drifted weights at the month's end
    return pd.Series(out), pd.Series(lev)


def portfolios(eq: pd.Series, bd: pd.Series, rf: pd.Series) -> tuple[dict[str, pd.Series], pd.Series]:
    rets = pd.DataFrame({"equity": eq, "bonds": bd}).dropna()
    rf = rf.reindex(rets.index).fillna(0.0)
    rp, lev = sleeve(rets, rf)
    c = core(rets["equity"], rf)
    idx = rp.index.intersection(c.index)
    c, rp = c.reindex(idx), rp.reindex(idx)
    sixty = 0.6 * rets["equity"] + 0.4 * rets["bonds"]
    p = {"core_rp": ts.blend(c, rp, WEIGHT), "core_now": c, "core_cash": ts.blend(c, rf.reindex(idx), WEIGHT),
         "core_bonds": ts.blend(c, rets["bonds"].reindex(idx), WEIGHT), "rp": rp, "equity": rets["equity"].reindex(idx),
         "sixty_forty": sixty.reindex(idx)}
    return p, lev.reindex(idx)


# ── evaluation ──

def evaluate(p: dict[str, pd.Series], rf: pd.Series, lo: pd.Period, hi: pd.Period) -> dict:
    p = {k: v[(v.index >= lo) & (v.index <= hi)] for k, v in p.items()}
    months = p["core_now"].index
    mid = months[len(months) // 2]
    out = {"from": str(months[0]), "to": str(months[-1]), "split": str(mid), "variants": {}, "vs_core": {}}
    for k, r in p.items():
        s = ts.monthly_stats(r, rf)
        s.update({"sharpe_first": ts.monthly_stats(r[r.index < mid], rf)["sharpe"], "sharpe_second": ts.monthly_stats(r[r.index >= mid], rf)["sharpe"]})
        out["variants"][k] = s
        d = r - p["core_now"]
        out["vs_core"][k] = {"all": ca.tstat(d), "first": ca.tstat(d[d.index < mid]), "second": ca.tstat(d[d.index >= mid])}
    return out


def decide(ev: dict) -> dict:
    v, vs = ev["variants"], ev["vs_core"]
    s, c, z = v["core_rp"], v["core_now"], v["core_cash"]
    checks = {"sharpe_vs_core": all(s[f"sharpe_{h}"] > c[f"sharpe_{h}"] for h in ("first", "second")),
              "sharpe_vs_cash": all(s[f"sharpe_{h}"] > z[f"sharpe_{h}"] for h in ("first", "second")),
              "drawdown": bool(s["max_dd"] - c["max_dd"] >= MIN_DD_GAIN),
              "not_behind": all(vs["core_rp"][h]["t"] is None or vs["core_rp"][h]["t"] >= MIN_T for h in ("first", "second"))}
    return {"passed": all(checks.values()), "checks": checks}


def table(ev: dict, lev: pd.Series | None = None) -> list[str]:
    lines = [f"{ev['from']} 至 {ev['to']}，前后两半以 {ev['split']} 分界",
             "组合 | 年化 | 波动 | 夏普 全部/前半/后半 | 最大回撤 | 最差 12 个月 | 对现行规则每月（t）"]
    for k in LABELS:
        if k not in ev["variants"]:
            continue
        s, d = ev["variants"][k], ev["vs_core"][k]["all"]
        t = "—" if d["t"] is None else f"{d['t']:.2f}"
        lines.append(f"{LABELS[k]} | {ts._p(s['cagr'])} | {s['vol']:.1%} | {s['sharpe']:.2f} / {s['sharpe_first']:.2f} / {s['sharpe_second']:.2f} | "
                     f"{ts._p(s['max_dd'])} | {ts._p(s['worst_12m'])} | {ts._p(d['mean'], 2)}（{t}）")
    return lines


def report_text(res: dict) -> str:
    j, dec = res["judged"], res["decision"]
    lines = ["核心仓 + 20% 风险平价（risk_parity_sleeve_v1）· 判定样本（未见）", *table(j),
             f"风险平价配置平均杠杆 {res['leverage']['judged_mean']:.2f}，最高 {res['leverage']['judged_max']:.2f}",
             f"判定：{'通过' if dec['passed'] else '未通过'} {dec['checks']}",
             f"数据检查：构造的 10 年国债与 IEF 月收益相关 {res['data_check']['corr']:.2f}，年化差 {ts._p(res['data_check']['ann_diff'])}（{res['data_check']['months']} 个月）",
             "只报告 · 2003-09 起（已看过，构造的国债）：", *table(res["later"])]
    if res.get("later_etf"):
        lines += ["只报告 · 2003-09 起（已看过，SPY + IEF 实际 ETF）：", *table(res["later_etf"])]
    return "\n".join(lines)
