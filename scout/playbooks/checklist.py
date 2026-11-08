"""Question lists for the authors who left no fixed rule (Fisher, Buffett's moat, Soros): the data points Scout has
beside each question, for the holdings and watchlist. No action: these help think, they never signal."""
from __future__ import annotations

import numpy as np

from scout.playbooks.base import Ctx, Verdict, pct
from scout.playbooks.value import ratios

ASK = "问题"


def _names(ctx: Ctx) -> list[str]:
    return [t for t in dict.fromkeys([x["ticker"] for x in ctx.holdings] + ctx.watchlist) if t in ctx.fund.index]


def _g(ctx: Ctx, t: str, k: str) -> float:
    v = ctx.fund.at[t, k] if k in ctx.fund.columns else np.nan
    return float(v) if v is not None and np.isfinite(v) else np.nan


def fisher(ctx: Ctx) -> dict[str, Verdict]:
    out = {}
    for t in _names(ctx):
        rev, rev0 = _g(ctx, t, "revenue"), _g(ctx, t, "revenue_prev")
        rnd, ebit = _g(ctx, t, "rnd"), _g(ctx, t, "operating_income")
        ebit0 = _g(ctx, t, "operating_income_prev")
        sh, sh0 = _g(ctx, t, "shares"), _g(ctx, t, "shares_prev")
        out[t] = Verdict(ASK, "；".join([
            f"未来几年收入还有多大空间？（近一年收入 {pct(rev / rev0 - 1)}）",
            f"研发投入能否转成新产品？（研发 / 收入 {pct(rnd / rev, 0)[1:]}）",
            f"利润率是否在改善？（营业利润率 {pct(ebit0 / rev0, 0)[1:]} → {pct(ebit / rev, 0)[1:]}）",
            f"增长是否靠增发股票？（稀释后股数 {pct(sh / sh0 - 1)}）",
            "管理层是否坦诚讲坏消息、客户和供应商怎么评价它？（看公司画像和年报）"]))
    return out


def buffett_moat(ctx: Ctx) -> dict[str, Verdict]:
    r = ratios(ctx)
    out = {}
    for t in _names(ctx):
        if t not in r.index:
            continue
        x = r.loc[t]
        out[t] = Verdict(ASK, "；".join([
            f"高回报能否持续、对手为什么抢不走？（毛利 / 资产 {pct(x['gpa'], 0)[1:]}，ROE {pct(x['roe'], 0)[1:]}）",
            f"不靠借钱也能赚？（负债 / 净资产 {x['de']:.2f}）" if np.isfinite(x["de"]) else "不靠借钱也能赚？",
            f"现在的价格是否合理？（EBIT/EV {pct(x['ebit_ev'])}，市盈率 {x['pe']:.1f}）" if np.isfinite(x["pe"]) else "现在的价格是否合理？",
            "十年后这家公司还在做同样的生意、而且更强吗？"]))
    return out


def soros(ctx: Ctx) -> dict[str, Verdict]:
    r63, rs = ctx.f["ret_63d"].iloc[-1], ctx.f["rs_pct"].iloc[-1]
    out = {}
    for t in dict.fromkeys([x["ticker"] for x in ctx.holdings] + ctx.watchlist):
        if t not in ctx.close:
            continue
        out[t] = Verdict(ASK, "；".join([
            f"现在的走势（3 个月 {pct(r63.get(t, np.nan))}，相对强度 {pct(rs.get(t, np.nan), 0)[1:]} 分位）是否在反过来改善它的基本面"
            "（股价高 → 融资便宜 / 人才 / 客户信心）？",
            "市场相信的故事里，哪一点是错的？什么时候会被发现？",
            "如果故事破了，最先出现的信号是什么？"]))
    return out
