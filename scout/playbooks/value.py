"""Value and growth playbooks (catalogue sections 1–2), from financials as filed (`Ctx.fund`).

Scout's financials have no current assets / current liabilities, dividends or depreciation, so Graham's current
ratio and dividend record, Greenblatt's working capital, Piotroski's current-ratio signal and EBITDA are left out
or replaced; each version line says what was replaced. Financials and utilities are left out of the ratio screens
(their balance sheets do not fit EV / EBIT), as Greenblatt does.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from scout.playbooks.base import BUY, HOLD, SELL, Ctx, Verdict, num, pct, ranks
from scout.playbooks.technical import ranked

SKIP_SECTORS = ("Financials", "Utilities")
MIN_CAP = 1e9


def ratios(ctx: Ctx) -> pd.DataFrame:
    """Valuation and quality ratios of the liquid stocks with a market value."""
    if "ratios" in ctx.cache:
        return ctx.cache["ratios"]
    f = ctx.fund.reindex([t for t in ctx.stocks if t in ctx.fund.index])
    if f.empty or "mcap" not in f:
        ctx.cache["ratios"] = pd.DataFrame()
        return ctx.cache["ratios"]
    f = f[f["mcap"].notna()]
    num_ = lambda a: pd.to_numeric(f.get(a), errors="coerce")  # noqa: E731
    mcap, ev = num_("mcap"), num_("ev")
    ni, ebit, rev, gp, ocf = (num_(k) for k in ("net_income", "operating_income", "revenue", "gross_profit", "operating_cash_flow"))
    assets, equity, cash = num_("total_assets"), num_("equity"), num_("cash").fillna(0)
    debt = num_("long_term_debt").fillna(0) + num_("current_debt").fillna(0)
    r = pd.DataFrame(index=f.index)
    r["mcap"] = mcap
    r["pe"] = (mcap / ni).where(ni > 0)
    r["pb"] = (mcap / equity).where(equity > 0)
    r["ps"] = (mcap / rev).where(rev > 0)
    r["pcf"] = (mcap / ocf).where(ocf > 0)
    r["ebit_ev"] = (ebit / ev).where(ev > 0)
    r["roc"] = (ebit / (assets - cash)).where(assets - cash > 0)          # Greenblatt: EBIT / tangible capital (proxy)
    r["gpa"] = gp / assets
    r["roe"] = (ni / equity).where(equity > 0)
    r["de"] = (debt / equity).where(equity > 0)
    r["bb_yield"] = num_("buybacks").abs() / mcap
    years = f["ni_years"] if "ni_years" in f else pd.Series(index=f.index, dtype=object)
    at = lambda k: pd.Series([y[k] if isinstance(y, list) and y[k] is not None else np.nan for y in years], index=f.index)  # noqa: E731
    n0, n3, n4 = at(0), at(3), at(4)
    r["g3"] = ((n0 / n3) ** (1 / 3) - 1).where((n0 > 0) & (n3 > 0))          # net income, not EPS (see data.py)
    r["g4_total"] = (n0 / n4 - 1).where((n0 > 0) & (n4 > 0))
    r["eps_q_yoy"] = (num_("eps_q") / num_("eps_q_prev_year") - 1).where(num_("eps_q_prev_year") > 0)
    r["sector"] = [ctx.sectors.get(t) for t in r.index]
    r["ret_126"] = ctx.close[r.index].iloc[-1] / ctx.close[r.index].iloc[-127] - 1
    r["vol_63"] = ctx.f["vol_63"].iloc[-1].reindex(r.index)
    r["beta"] = ctx.f["beta"].iloc[-1].reindex(r.index)
    ctx.cache["ratios"] = r
    return r


def _screen(r: pd.DataFrame) -> pd.DataFrame:
    return r[(~r["sector"].isin(SKIP_SECTORS)) & (r["mcap"] >= MIN_CAP)]


def magic_formula(ctx: Ctx) -> dict[str, Verdict]:
    r = _screen(ratios(ctx)).dropna(subset=["ebit_ev", "roc"])
    if r.empty:
        return {}
    combined = r["ebit_ev"].rank(ascending=False) + r["roc"].rank(ascending=False)
    order = combined.sort_values()
    out = {}
    for k, t in enumerate(order.index[:30]):
        out[t] = Verdict(BUY, f"盈利收益率 EBIT/EV {pct(r.at[t, 'ebit_ev'])}、资本回报 {pct(r.at[t, 'roc'], 0)}，合计排名第 {k + 1} / {len(order)}",
                         score=-k)
    for t in order.index[int(len(order) * 0.8):]:
        out[t] = Verdict(SELL, f"神奇公式合计排名在后 20%（EBIT/EV {pct(r.at[t, 'ebit_ev'])}，资本回报 {pct(r.at[t, 'roc'], 0)}）")
    return out


def piotroski_score(ctx: Ctx) -> pd.DataFrame:
    """Eight of Piotroski's nine signals (no current ratio in Scout's data) → score 0–8 and the signal list."""
    f = ctx.fund
    g = lambda a: pd.to_numeric(f.get(a), errors="coerce")  # noqa: E731
    roa, roa0 = g("net_income") / g("total_assets"), g("net_income_prev") / g("total_assets_prev")
    lev, lev0 = g("long_term_debt") / g("total_assets"), g("long_term_debt_prev") / g("total_assets_prev")
    gm, gm0 = g("gross_profit") / g("revenue"), g("gross_profit_prev") / g("revenue_prev")
    turn, turn0 = g("revenue") / g("total_assets"), g("revenue_prev") / g("total_assets_prev")
    sig = pd.DataFrame({"盈利为正": roa > 0, "经营现金流为正": g("operating_cash_flow") > 0, "ROA 提高": roa > roa0,
                        "现金流高于利润": g("operating_cash_flow") > g("net_income"), "长期负债率下降": lev.fillna(0) <= lev0.fillna(0),
                        "没有增发": g("shares") <= g("shares_prev") * 1.005, "毛利率提高": gm > gm0, "资产周转率提高": turn > turn0})
    known = pd.DataFrame({"ROA 提高": roa0.notna(), "毛利率提高": gm0.notna(), "资产周转率提高": turn0.notna(),
                          "没有增发": g("shares_prev").notna()}).reindex(columns=sig.columns, fill_value=True)
    sig = sig.where(known)
    return pd.DataFrame({"score": sig.fillna(False).sum(axis=1), "n": sig.notna().sum(axis=1),
                         "passed": sig.apply(lambda row: [k for k, v in row.items() if v is True], axis=1)})


def piotroski(ctx: Ctx) -> dict[str, Verdict]:
    r = ratios(ctx).dropna(subset=["pb"])
    if r.empty:
        return {}
    cheap = r[r["pb"] <= r["pb"].quantile(0.2)]                       # the high book-to-market fifth
    sc = piotroski_score(ctx).reindex(cheap.index)
    out = {}
    for t, row in sc.iterrows():
        if row["n"] < 8:
            continue
        if row["score"] >= 7:
            out[t] = Verdict(BUY, f"市净率 {num(r.at[t, 'pb'])} 在最便宜的 20%，F 分 {int(row['score'])} / 8：{'、'.join(row['passed'])}",
                             score=float(row["score"]))
        elif row["score"] <= 2:
            out[t] = Verdict(SELL, f"便宜但 F 分只有 {int(row['score'])} / 8（价值陷阱）")
    return out


def graham(ctx: Ctx) -> dict[str, Verdict]:
    """Graham's defensive criteria that Scout's data covers: market value ≥ $2 B, positive earnings in each of the last
    five years, net income up at least a third over four years, P/E ≤ 15, P/E × P/B ≤ 22.5, long-term debt ≤ equity."""
    r = ratios(ctx)
    f = ctx.fund.reindex(r.index)
    out = {}
    for t in r.index:
        years = f.at[t, "ni_years"] if "ni_years" in f else None
        pe, pb, g5, de = r.at[t, "pe"], r.at[t, "pb"], r.at[t, "g4_total"], r.at[t, "de"]
        if not isinstance(years, list) or any(y is None for y in years) or not np.isfinite(pe) or not np.isfinite(pb):
            continue
        checks = {"市值 ≥ 20 亿美元": r.at[t, "mcap"] >= 2e9, "近 5 年每年盈利": all(y > 0 for y in years),
                  "4 年净利润增长 ≥ 1/3": bool(g5 >= 1 / 3) if np.isfinite(g5) else False, "市盈率 ≤ 15": pe <= 15,
                  "市盈率 × 市净率 ≤ 22.5": pe * pb <= 22.5, "长期负债 ≤ 净资产": bool(de <= 1) if np.isfinite(de) else True}
        failed = [k for k, v in checks.items() if not v]
        if not failed:
            out[t] = Verdict(BUY, f"6 条全部满足：市盈率 {num(pe, 1)}、市净率 {num(pb, 1)}、4 年净利润增长 {pct(g5, 0)}", score=float(-pe * pb))
        elif pe > 40 or pe * pb > 100:
            out[t] = Verdict(SELL, f"远离 Graham 的价格标准：市盈率 {num(pe, 0)}、市盈率 × 市净率 {num(pe * pb, 0)}")
    return out


def trending_value(ctx: Ctx) -> dict[str, Verdict]:
    """O'Shaughnessy: the cheapest tenth by a value composite (P/E, P/B, P/S, P/CF, EBIT/EV, buyback yield; EBIT in
    place of EBITDA, buybacks without dividends), then the 25 with the best six-month return."""
    r = ratios(ctx)
    if r.empty:
        return {}
    parts = pd.DataFrame({"pe": ranks(r["pe"], True), "pb": ranks(r["pb"], True), "ps": ranks(r["ps"], True),
                          "pcf": ranks(r["pcf"], True), "ebit_ev": ranks(r["ebit_ev"]), "bb": ranks(r["bb_yield"])})
    comp = parts.mean(axis=1)[parts.notna().sum(axis=1) >= 4]
    cheap = comp[comp >= comp.quantile(0.9)]
    picks = r.loc[cheap.index, "ret_126"].sort_values(ascending=False).iloc[:25]
    out = {t: Verdict(BUY, f"价值综合分在最便宜的 10%（{num(cheap[t])}），6 个月涨幅 {pct(m)}，第 {k + 1} / 25", score=float(m))
           for k, (t, m) in enumerate(picks.items())}
    for t in comp[comp <= comp.quantile(0.1)].index:
        out[t] = Verdict(SELL, f"价值综合分在最贵的 10%（{num(comp[t])}）")
    return out


def buffett_quality(ctx: Ctx) -> dict[str, Verdict]:
    """Frazzini, Kabiller & Pedersen's description of Buffett's picks: safe (low beta, low volatility), high quality
    (gross profitability, ROE, low leverage) and not expensive (EBIT / EV above the median)."""
    r = ratios(ctx)
    if r.empty:
        return {}
    q = pd.DataFrame({"gpa": ranks(r["gpa"]), "roe": ranks(r["roe"]), "de": ranks(r["de"], True),
                      "beta": ranks(r["beta"], True), "vol": ranks(r["vol_63"], True)}).mean(axis=1)
    cheap = ranks(r["ebit_ev"])
    out = {}
    for t in q.index:
        if q[t] >= q.quantile(0.8) and cheap.get(t, 0) >= 0.5:
            out[t] = Verdict(BUY, f"质量与安全分 {num(q[t])}（前 20%）：毛利 / 资产 {pct(r.at[t, 'gpa'], 0)}、ROE {pct(r.at[t, 'roe'], 0)}、"
                                  f"β {num(r.at[t, 'beta'])}；EBIT/EV {pct(r.at[t, 'ebit_ev'])} 不贵", score=float(q[t]))
        elif q[t] <= q.quantile(0.2):
            out[t] = Verdict(SELL, f"质量与安全分 {num(q[t])} 在后 20%")
    return out


def lynch(ctx: Ctx) -> dict[str, Verdict]:
    """Lynch's categories from three-year EPS growth, and his PEG rule: fast growers (20–50 % a year) with P/E ÷ growth
    below 1 and debt / equity below 0.8 are buys; a PEG above 2 (growth priced too high) is where he would sell."""
    r = ratios(ctx).dropna(subset=["pe", "g3"])
    out = {}
    for t, row in r.iterrows():
        g, pe = row["g3"], row["pe"]
        cat = "快速增长" if 0.2 <= g <= 0.5 else "稳定增长" if 0.1 <= g < 0.2 else "缓慢增长" if 0 <= g < 0.1 else "高速 / 异常" if g > 0.5 else "下滑"
        peg = pe / (g * 100) if g > 0 else np.nan
        if cat == "快速增长" and peg < 1 and (not np.isfinite(row["de"]) or row["de"] < 0.8):
            out[t] = Verdict(BUY, f"{cat}：3 年净利润年增 {pct(g, 0)}，市盈率 {num(pe, 1)}，PEG {num(peg)}，负债 / 净资产 {num(row['de'])}",
                             score=float(-peg))
        elif np.isfinite(peg) and peg > 2:
            out[t] = Verdict(SELL, f"{cat}：3 年净利润年增 {pct(g, 0)}，PEG {num(peg)}")
    return out


def can_slim(ctx: Ctx) -> dict[str, Verdict]:
    """O'Neil's letters Scout can compute: C quarterly EPS ≥ +25 % year on year, A three-year EPS growth ≥ 25 % a year,
    N within 15 % of the 52-week high, L relative strength ≥ 80th percentile, M the market in a confirmed uptrend
    (the O'Neil market playbook). I (institutions) and S (supply) are left out. Sell: 7–8 % under a buy is a holding's
    rule; for the universe, a close below the 50-day average on heavy volume."""
    from scout.playbooks.market import oneil_state

    r = ratios(ctx)
    m = oneil_state(ctx, "SPY")
    up = m is not None and m["state"] == "确认上升"
    dist, rs = ctx.f["dist_52w_high"].iloc[-1], ctx.f["rs_pct"].iloc[-1]
    vr, c, s50 = ctx.f["vol_ratio"].iloc[-1], ctx.close.iloc[-1], ctx.f["sma50"].iloc[-1]
    out = {}
    for t in r.index:
        cq, a = r.at[t, "eps_q_yoy"], r.at[t, "g3"]
        letters = {"C": bool(cq >= 0.25) if np.isfinite(cq) else False, "A": bool(a >= 0.25) if np.isfinite(a) else False,
                   "N": bool(dist.get(t, -1) >= -0.15), "L": bool(rs.get(t, 0) >= 0.8)}
        if all(letters.values()):
            why = f"C 当季 EPS {pct(cq, 0)}、A 3 年净利润年增 {pct(a, 0)}、N 距 52 周高点 {pct(dist[t])}、L 相对强度 {num(rs[t] * 100, 0)}"
            out[t] = Verdict(BUY, why + "；M 大盘确认上升", score=float(rs[t])) if up else \
                Verdict(HOLD, why + f"；M 大盘不在确认上升（{m['state'] if m else '无数据'}），O'Neil 这时不买")
        elif c.get(t, np.nan) < s50.get(t, np.nan) and vr.get(t, 0) >= 1.5 and letters["L"]:
            out[t] = Verdict(SELL, f"强势股放量（{num(vr[t], 1)} 倍）跌破 50 日均线")
    return out


# ── the tested fundamental rankers (scout.rankers, same inputs as the gate) ──

def gross_profitability(ctx: Ctx) -> dict[str, Verdict]:
    return ranked(ctx, "gross_profitability", 0.1, "毛利 / 总资产")


def low_accruals(ctx: Ctx) -> dict[str, Verdict]:
    return ranked(ctx, "low_accruals", 0.1, "利润质量（应计最低）")


def low_asset_growth(ctx: Ctx) -> dict[str, Verdict]:
    return ranked(ctx, "low_asset_growth", 0.1, "资产增长最慢")


def net_buyback(ctx: Ctx) -> dict[str, Verdict]:
    return ranked(ctx, "net_buyback", 0.1, "净回购")
