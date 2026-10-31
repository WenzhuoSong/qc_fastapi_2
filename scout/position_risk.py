"""Risk of a stock, a position and the holdings: the method chapters of Edwards & Magee
(Technical Analysis of Stock Trends, 11th ed.) on the evidence of the risk study.

- ch. 24, probable move: a stock's normal range over a week / a month / three months from
  its own past volatility (`move_range`);
- ch. 31 / 38 / 40–42, diversification, capital use and portfolio risk: a relative risk tier
  (`tier`), a volatility cap on position size so that each position's typical monthly move costs
  about the same share of the account (Magee's "composite leverage", `vol_cap_shares`), and the
  holdings' volatility with their correlations (`portfolio_vol`);
- ch. 43, stick to your guns: did the journal follow the plan (`adherence`)?

The risk study (ml_risk_v1, pre-registered 2026-10-31, 2004–2021, top 1000 by market cap incl.
delisted stocks): no model beat the stock's own 63-day volatility at forecasting volatility, and
the level of a 20 % drawdown probability was unstable (the riskiest decile: 39 % predicted, 21 %
realized in 2013–2021). So risk is shown as a tier with the historical range of outcomes, never
as one probability. Weighting by 1 / past volatility had a Sharpe of 0.57 vs 0.49 for equal weight
(descriptive). No number here is a forecast that passed a test; the page says so.
"""
from __future__ import annotations

import math
from datetime import date

import numpy as np

# Share of buys (next open) whose close fell ≥ 20 % below the buy price within 63 sessions, by quintile
# of 63-day volatility on the day (exports/ml/risk_v1, 2004-01..2021-12, every 5th session):
# all months / without the crisis months (2008-06..2009-03, 2020-01..03) / the worst 10 % of months.
TIERS = (
    {"tier": 1, "label": "最稳的 20%", "all": 0.049, "calm": 0.022, "bad": 0.094},
    {"tier": 2, "label": "较稳", "all": 0.077, "calm": 0.046, "bad": 0.212},
    {"tier": 3, "label": "中间", "all": 0.109, "calm": 0.077, "bad": 0.380},
    {"tier": 4, "label": "较高", "all": 0.152, "calm": 0.119, "bad": 0.454},
    {"tier": 5, "label": "波动最大的 20%", "all": 0.254, "calm": 0.224, "bad": 0.620},
)
BASE_RATE = 0.129            # all stocks, all months
HORIZONS = ((5, "1 周"), (21, "1 个月"), (63, "3 个月"))
VOL_BUDGET = 0.02            # default: a position's typical (1σ) monthly move costs at most 2 % of the account
ADHERENCE_GRACE = 1          # sessions after a sell signal within which a sell counts as on plan


def tier(pct: float | None) -> dict | None:
    """The risk tier of a 63-day volatility percentile (0 = calmest)."""
    if pct is None or not math.isfinite(pct):
        return None
    return TIERS[min(4, int(pct * 5))]


def tier_text(t: dict | None) -> str | None:
    if not t:
        return None
    return (f"风险第 {t['tier']} 档（{t['label']}）：2004–2021 年同档股票买入后 63 个交易日内跌破买入价 20% 的比例，"
            f"平常年份约 {t['calm'] * 100:.0f}%，全部年份 {t['all'] * 100:.0f}%，最差的一成月份超过 {t['bad'] * 100:.0f}%"
            f"（全部股票平均 {BASE_RATE * 100:.0f}%）。只看相对高低，不是概率预测")


def move_range(close: float | None, vol: float | None) -> list[dict]:
    """Normal range per horizon from annualised volatility: ±1σ (about two times in three) and ±2σ.
    Log-normal, so the band is wider above than below; real returns have fatter tails."""
    if not close or not vol or vol <= 0:
        return []
    out = []
    for n, label in HORIZONS:
        s = vol * math.sqrt(n / 252)
        out.append({"sessions": n, "label": label, "sigma": round(s, 4),
                    "lo1": round(close * math.exp(-s), 2), "hi1": round(close * math.exp(s), 2),
                    "lo2": round(close * math.exp(-2 * s), 2), "hi2": round(close * math.exp(2 * s), 2)})
    return out


def monthly_sigma(vol: float) -> float:
    return vol * math.sqrt(21 / 252)


def vol_cap_shares(entry: float, vol: float | None, account: float, budget: float) -> int | None:
    """Most shares whose typical monthly move (1σ) costs `budget` of the account."""
    if not vol or vol <= 0 or entry <= 0:
        return None
    return math.floor(account * budget / (monthly_sigma(vol) * entry))


def portfolio_vol(values: dict[str, float], returns: dict[str, list[float]], account: float | None = None) -> dict | None:
    """Annualised volatility of the holdings in dollars, from the last daily returns of each stock
    (aligned, same length) and their correlations; each position's share of the risk.
    values: ticker → market value of the position."""
    names = [t for t in values if values[t] > 0 and len(returns.get(t) or []) >= 20]
    if not names:
        return None
    n = min(len(returns[t]) for t in names)
    r = np.array([returns[t][-n:] for t in names], dtype=float)
    r = np.nan_to_num(r - np.nanmean(r, axis=1, keepdims=True))
    cov = r @ r.T / max(n - 1, 1) * 252
    v = np.array([values[t] for t in names])
    var = float(v @ cov @ v)
    if var <= 0:
        return None
    total = math.sqrt(var)
    alone = float(np.sum(v * np.sqrt(np.diag(cov))))
    share = v * (cov @ v) / var
    invested = float(v.sum())
    out = {"usd": total, "pct_invested": total / invested, "month_usd": total * math.sqrt(21 / 252),
           "diversification": alone / total, "sessions": n,
           "contrib": sorted(({"ticker": t, "share": float(s)} for t, s in zip(names, share)), key=lambda x: -x["share"])}
    if account:
        out["pct_account"] = total / account
        out["month_pct_account"] = out["month_usd"] / account
    return out


def _after_sessions(start: date, n: int) -> date:
    from scout.market_calendar import is_us_equity_trading_day

    d = start
    while n > 0:
        d = date.fromordinal(d.toordinal() + 1)
        n -= is_us_equity_trading_day(d)
    return d


def _signals(t: dict) -> list[tuple[date, str, bool]]:
    """(date, reason, hard) when the plan said to sell everything: the stop traded or the holding
    period ended (hard); the time stop passed before target 1 (soft: the page says 'consider')."""
    plan = t.get("plan") or {}
    out = [(date.fromisoformat(plan[key]), text, True)
           for key, text in (("stop_hit", "止损触发"), ("hold_limit_hit", "持有期满")) if plan.get(key)]
    if plan.get("time_stop_days") and t.get("opened_on"):
        d = _after_sessions(t["opened_on"], plan["time_stop_days"])
        if not plan.get("target1_hit") or date.fromisoformat(plan["target1_hit"]) > d:
            out.append((d, "时间止损", False))
    return sorted(out)


def adherence(t: dict, sells: list[dict], today: date) -> dict:
    """How the trade was handled against its plan.

    Open: whether a sell signal is outstanding and for how many sessions. Closed (or partly sold):
    each sell is on plan (within ADHERENCE_GRACE sessions of a signal), late (after that), or early
    (no signal yet: the plan said hold). The time stop counts for sells but is not nagged about.
    Sells under the earlier plan (a third at target 1, before 2026-10-27) count as early too; the
    page shows the date. A trade closed before sells were recorded counts as one sell at its exit."""
    from scout.intraday import sessions_since

    sig = _signals(t)
    if not sells and t.get("closed_on"):
        sells = [{"sold_on": t["closed_on"]}]
    out: dict = {"signal": None, "sells": []}
    for s in sells:
        before = [d for d in sig if d[0] <= s["sold_on"]]
        if not before:
            out["sells"].append({"date": s["sold_on"], "kind": "early", "text": "计划未要求卖出时卖出（提前离场）"})
            continue
        d, why, hard = next((x for x in before if x[2]), before[0])
        late = sessions_since(d, s["sold_on"])
        slow = hard and late > ADHERENCE_GRACE
        out["sells"].append({"date": s["sold_on"], "kind": "late" if slow else "on_plan",
                             "text": f"{why}后 {late} 个交易日卖出" + ("（拖延）" if slow else "")})
    hard = [x for x in sig if x[2] and x[0] <= today]
    if not t.get("closed_on") and hard:
        d, why, _ = hard[0]
        waited = sessions_since(d, today)
        if waited > ADHERENCE_GRACE:
            out["signal"] = {"date": d, "why": why, "sessions": waited,
                             "text": f"{d:%m-%d} {why}，按计划应卖出全部，已过 {waited} 个交易日仍未记录卖出"}
    kinds = [s["kind"] for s in out["sells"]]
    out["verdict"] = ("拖延" if "late" in kinds or out["signal"] else "提前" if "early" in kinds
                      else "按计划" if kinds else None)
    return out


def adherence_summary(items: list[dict]) -> dict:
    """Counts over closed trades' verdicts."""
    v = [a["verdict"] for a in items if a.get("verdict")]
    return {"n": len(v), "on_plan": v.count("按计划"), "early": v.count("提前"), "late": v.count("拖延")}
