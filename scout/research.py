"""v2 phase 3: research features behind the new lenses, point in time.

Each block is a pure function over already-loaded data (testable), plus one
loader (`build`) that reads only what was known on `asof`: financials filed
on or before it, the newest snapshot on or before it. Values use the keys
defined in scout/knowledge/features.yaml; tables for the page and the AI go
under `context`.
"""
from __future__ import annotations

import math
from datetime import date, timedelta

import numpy as np
import pandas as pd

from scout import db, universe

YEAR = 365


# ── helpers ──

def _near(rows: list[tuple[date, float]], target: date, tol: int = 25) -> float | None:
    best = min(rows, key=lambda r: abs((r[0] - target).days), default=None)
    return best[1] if best and abs((best[0] - target).days) <= tol else None


def _ratio(a, b):
    return a / b if a is not None and b not in (None, 0) else None


def _chg(a, b):
    return a / b - 1 if a is not None and b not in (None, 0) and b > 0 else None


# ── 1. fundamental trend ──

def fundamental_trend(series: dict[str, list[tuple[date, float]]], unit: str = "USD") -> tuple[dict, list[dict]]:
    """Growth, margins, cash generation and dilution from quarterly (or annual) filings."""
    rev = sorted(series.get("revenue") or [])
    if len(rev) < 2:
        return {}, []
    quarterly = (rev[-1][0] - rev[-2][0]).days <= 120
    step = 91 if quarterly else YEAR
    gp = dict(series.get("gross_profit") or [])
    cost = dict(series.get("cost_of_revenue") or [])
    op = dict(series.get("operating_income") or [])
    ocf, capex = dict(series.get("operating_cash_flow") or []), dict(series.get("capex") or [])

    def gross(d, r):
        if d in gp:
            return gp[d] / r
        return (r - cost[d]) / r if d in cost else None

    table = []
    for d, r in rev[-8:]:
        prev = _near(rev, d - timedelta(days=YEAR))
        table.append({"period": d.isoformat(), "revenue": r, "yoy": _chg(r, prev),
                      "gross_margin": gross(d, r) if r else None, "op_margin": op[d] / r if d in op and r else None,
                      "fcf": ocf[d] - capex.get(d, 0.0) if d in ocf else None})
    last = table[-1]
    out: dict = {"fin_rev_yoy": last["yoy"], "fin_gross_margin": last["gross_margin"], "fin_op_margin": last["op_margin"],
                 "fin_quarters": len(rev) if quarterly else None}
    if quarterly and len(table) >= 2 and last["yoy"] is not None and table[-2]["yoy"] is not None:
        out["fin_rev_accel"] = last["yoy"] - table[-2]["yoy"]
    year_ago = next((t for t in table if abs((date.fromisoformat(t["period"]) - (rev[-1][0] - timedelta(days=YEAR))).days) <= 25), None)
    if year_ago:
        if last["gross_margin"] is not None and year_ago["gross_margin"] is not None:
            out["fin_gross_margin_chg"] = last["gross_margin"] - year_ago["gross_margin"]
        if last["op_margin"] is not None and year_ago["op_margin"] is not None:
            out["fin_op_margin_chg"] = last["op_margin"] - year_ago["op_margin"]
    n = 4 if quarterly else 1
    recent = rev[-n:]
    if len(recent) == n and (recent[-1][0] - recent[0][0]).days <= step * (n - 1) + 30:
        rev_ttm = sum(v for _, v in recent)
        fcfs = [ocf[d] - capex.get(d, 0.0) for d, _ in recent if d in ocf]
        if len(fcfs) == n:
            out["fin_fcf_margin"] = _ratio(sum(fcfs), rev_ttm)
            if unit == "USD":
                out["fin_fcf_ttm"] = sum(fcfs)
        out["_rev_ttm"] = rev_ttm
    cash = sorted(series.get("cash") or [])
    if cash and unit == "USD":
        d = cash[-1][0]
        debt = sum(dict(series.get(k) or []).get(d, 0.0) for k in ("long_term_debt", "current_debt"))
        out["fin_net_cash"] = cash[-1][1] + dict(series.get("short_term_investments") or []).get(d, 0.0) - debt
    shares = sorted(series.get("shares_diluted") or [])
    if shares:
        out["fin_share_change"] = _chg(shares[-1][1], _near(shares, shares[-1][0] - timedelta(days=YEAR)))
        out["_shares"] = shares[-1][1]
    out["fin_period"] = f"{rev[-1][0].isoformat()}（{'季度' if quarterly else '年度'}）"
    return {k: v for k, v in out.items() if v is not None}, table


# ── 2. valuation ──

def valuation(price: float, series: dict, trend: dict, unit: str, own: dict, closes: pd.Series | None,
              peers: dict[str, list[float]]) -> dict:
    """Multiples now, against their own 3-year range and against sector peers."""
    out: dict = {"val_fwd_pe": own.get("forwardPE"), "val_ev_ebitda": own.get("enterpriseToEbitda"),
                 "val_ev_sales": own.get("enterpriseToRevenue")}
    eps = sorted(series.get("eps_diluted") or [])
    if unit == "USD" and len(eps) >= 4 and (eps[-1][0] - eps[-4][0]).days <= 300:
        ttm = sum(v for _, v in eps[-4:])
        out["val_pe_ttm"] = price / ttm if ttm > 0 else None
    else:
        out["val_pe_ttm"] = own.get("trailingPE")
    shares = trend.get("_shares")
    if unit == "USD" and shares and trend.get("_rev_ttm"):
        mcap = price * shares
        out["val_ps_ttm"] = mcap / trend["_rev_ttm"]
        if trend.get("fin_fcf_ttm", 0) > 0:
            out["val_pfcf"] = mcap / trend["fin_fcf_ttm"]
        hist = ps_history(series, closes) if closes is not None else []
        if len(hist) >= 6:
            out["val_ps_pctile"] = float((np.array(hist) < out["val_ps_ttm"]).mean())
            out["val_ps_hist_n"] = len(hist)
    for key, own_key in (("val_ev_sales_vs_sector", "val_ev_sales"), ("val_fwd_pe_vs_sector", "val_fwd_pe")):
        group = [v for v in peers.get(own_key, []) if v and v > 0]
        if out.get(own_key) and out[own_key] > 0 and len(group) >= 3:
            out[key] = out[own_key] / float(np.median(group))
    return {k: v for k, v in out.items() if v is not None and not (isinstance(v, float) and math.isnan(v))}


def ps_history(series: dict, closes: pd.Series) -> list[float]:
    """Price/sales at each past quarter end (TTM revenue, diluted shares of that quarter)."""
    rev = sorted(series.get("revenue") or [])
    shares = dict(series.get("shares_diluted") or [])
    out = []
    for i in range(3, len(rev)):
        d = rev[i][0]
        window = rev[i - 3:i + 1]
        if (window[-1][0] - window[0][0]).days > 300 or d not in shares:
            continue
        px = closes[closes.index <= d]
        if px.empty or (d - px.index[-1]).days > 7:
            continue
        out.append(float(px.iloc[-1]) * shares[d] / sum(v for _, v in window))
    return out


# ── 3. expectations ──

def _row(table: dict, period: str) -> dict:
    return table.get(period) or {}


def expectations(est: dict, price: float, actions: list[dict], asof: date, finnhub: dict) -> tuple[dict, list[dict]]:
    out: dict = {}
    trend = est.get("eps_trend") or {}
    fy = _row(trend, "0y")
    if fy.get("current") is not None:
        out["est_eps_rev_30d"] = _chg(fy["current"], fy.get("30daysAgo")) if (fy.get("30daysAgo") or 0) > 0 else None
        out["est_eps_rev_90d"] = _chg(fy["current"], fy.get("90daysAgo")) if (fy.get("90daysAgo") or 0) > 0 else None
    rev = _row(est.get("eps_revisions") or {}, "0y")
    out["est_up_30d"], out["est_down_30d"] = rev.get("upLast30days"), rev.get("downLast30days")
    ee = est.get("earnings_estimate") or {}
    out["est_analysts"] = _row(ee, "0y").get("numberOfAnalysts")
    cur, nxt = _row(ee, "0y").get("avg"), _row(ee, "+1y").get("avg")
    out["est_eps_growth_next"] = _chg(nxt, cur) if cur and cur > 0 else None
    pt = est.get("price_targets") or {}
    if pt.get("mean") and price:
        out["est_target_upside"] = pt["mean"] / price - 1
        if pt.get("high") and pt.get("low"):
            out["est_target_dispersion"] = (pt["high"] - pt["low"]) / pt["mean"]
    since = asof - timedelta(days=30)
    recent = [a for a in actions if a["action_at"].date() >= since]
    out["est_upgrades_30d"] = sum(1 for a in recent if (a.get("action") or "") == "up")
    out["est_downgrades_30d"] = sum(1 for a in recent if (a.get("action") or "") == "down")
    out["est_pt_raises_30d"] = sum(1 for a in recent if (a.get("pt_action") or "").lower().startswith("raise"))
    out["est_pt_cuts_30d"] = sum(1 for a in recent if (a.get("pt_action") or "").lower().startswith("lower"))
    for k in ("analyst_buy_share", "analyst_buy_change"):
        if finnhub.get(k) is not None:
            out[k] = finnhub[k]
    table = [{"date": a["action_at"].date().isoformat(), "firm": a["firm"], "from": a.get("from_grade"), "to": a.get("to_grade"),
              "action": a.get("action"), "target": a.get("price_target"), "prior_target": a.get("prior_target")}
             for a in sorted(actions, key=lambda a: a["action_at"], reverse=True)[:6]]
    return {k: v for k, v in out.items() if v is not None}, table


# ── 4. earnings ──

def earnings_reactions(report_dates: list[date], closes: pd.Series, spy: pd.Series) -> list[dict]:
    """Close-to-close move over the report (day before → day after), raw and against SPY."""
    out = []
    for d in sorted(set(report_dates)):
        before = closes[closes.index < d]
        after = closes[closes.index > d]
        if before.empty or after.empty or (after.index[0] - d).days > 5:
            continue
        b, a = before.index[-1], after.index[0]
        ret = float(after.iloc[0] / before.iloc[-1] - 1)
        s = spy.reindex([b, a])
        excess = ret - float(s.iloc[1] / s.iloc[0] - 1) if s.notna().all() else None
        out.append({"date": d.isoformat(), "move": ret, "excess": excess})
    return out


def earnings_block(last: dict | None, nxt: dict | None, reactions: list[dict], asof: date, opt: dict) -> dict:
    out: dict = {}
    if last:
        out["earn_days_since"] = (asof - date.fromisoformat(last["date"])).days
        out["earn_eps_surprise"] = last.get("eps_surprise_pct")
        out["earn_rev_surprise"] = last.get("revenue_surprise_pct")
    if nxt:
        out["earn_days_to_next"] = (date.fromisoformat(nxt["date"]) - asof).days
    recent = reactions[-8:]
    if recent:
        out["earn_last_move"] = recent[-1]["move"]
        if len(recent) >= 3:
            out["earn_avg_abs_move"] = float(np.mean([abs(r["move"]) for r in recent]))
            out["earn_reactions_n"] = len(recent)
    move = opt.get("opt_move_front")
    if move and out.get("earn_avg_abs_move") and nxt and opt.get("_front_expiry") and nxt["date"] <= opt["_front_expiry"]:
        out["earn_implied_vs_hist"] = move / out["earn_avg_abs_move"]
    return {k: v for k, v in out.items() if v is not None}


# ── 5. ownership ──

def ownership_block(own: dict, finnhub: dict) -> tuple[dict, list[dict]]:
    out = {"own_inst_pct": own.get("heldPercentInstitutions"), "own_insider_pct": own.get("heldPercentInsiders"),
           "own_short_float": own.get("shortPercentOfFloat"), "own_short_days": own.get("shortRatio"),
           "own_short_change": _chg(own.get("sharesShort"), own.get("sharesShortPriorMonth"))}
    holders = own.get("top_institutions") or []
    changes = [h.get("pctChange") for h in holders if isinstance(h.get("pctChange"), (int, float))]
    if changes:
        out["own_top_holder_change"] = float(np.mean(changes))
    for k in ("insider_net_value_90d", "insider_buyers_90d", "insider_sellers_90d"):
        if finnhub.get(k) is not None:
            out[k] = finnhub[k]
    table = [{"holder": h.get("Holder"), "pct": h.get("pctHeld"), "change": h.get("pctChange"), "reported": h.get("Date Reported")}
             for h in holders[:5]]
    return {k: v for k, v in out.items() if v is not None}, table


# ── 6. options ──

def options_block(snap: dict | None, vol_20: float | None, iv_history: list[float]) -> dict:
    if not snap or snap.get("quality") != "ok":
        return {}
    ex = snap.get("expiries") or {}
    front, m1, m2 = ex.get("front") or {}, ex.get("m1") or {}, ex.get("m2") or {}
    iv30 = m1.get("atm_iv") or front.get("atm_iv")
    out = {"opt_iv_front": front.get("atm_iv"), "opt_iv_30": iv30, "opt_move_front": front.get("straddle_move"),
           "opt_put_skew": m1.get("put_skew") or front.get("put_skew"), "opt_pc_volume": snap.get("put_call_volume"),
           "opt_unusual_count": len(snap.get("unusual") or []), "opt_iv_history_days": len(iv_history),
           "_front_expiry": front.get("expiry")}
    if iv30 and vol_20:
        out["opt_iv_rv_ratio"] = iv30 / vol_20
    far = m2.get("atm_iv") or m1.get("atm_iv")
    if far and front.get("atm_iv"):
        out["opt_term_slope"] = far - front["atm_iv"]
    if iv30 and len(iv_history) >= 60:
        out["opt_iv_rank"] = float((np.array(iv_history) < iv30).mean())
    return {k: v for k, v in out.items() if v is not None}


# ── 7. macro sensitivity ──

def macro_sensitivity(stock: pd.Series, spy: pd.Series, macro: dict[str, pd.Series], window: int = 252) -> dict:
    """OLS of daily returns on SPY, Δ10y (pp), dollar and oil returns; effects per typical shock with t-stats."""
    df = pd.DataFrame({"r": stock.pct_change(), "m": spy.pct_change()})
    if "DGS10" in macro:
        df["rate"] = macro["DGS10"].reindex(df.index).ffill().diff()
    if "DTWEXBGS" in macro:
        df["usd"] = macro["DTWEXBGS"].reindex(df.index).ffill().pct_change()
    if "DCOILWTICO" in macro:
        oil = macro["DCOILWTICO"].where(macro["DCOILWTICO"] > 0)
        df["oil"] = oil.reindex(df.index).ffill().pct_change()
    df = df.replace([np.inf, -np.inf], np.nan).dropna().iloc[-window:]
    cols = [c for c in ("m", "rate", "usd", "oil") if c in df]
    if len(df) < 120 or len(cols) < 2:
        return {}
    x = np.column_stack([np.ones(len(df))] + [df[c].to_numpy() for c in cols])
    y = df["r"].to_numpy()
    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    resid = y - x @ beta
    sigma2 = resid @ resid / (len(y) - x.shape[1])
    se = np.sqrt(np.diag(sigma2 * np.linalg.inv(x.T @ x)))
    coef = dict(zip(["c"] + cols, beta))
    t = dict(zip(["c"] + cols, beta / se))
    out = {}
    if "rate" in coef:
        out.update(macro_rate_10bp=coef["rate"] * 0.10, macro_rate_t=t["rate"])
    if "usd" in coef:
        out.update(macro_usd_1pct=coef["usd"] * 0.01, macro_usd_t=t["usd"])
    if "oil" in coef:
        out.update(macro_oil_10pct=coef["oil"] * 0.10, macro_oil_t=t["oil"])
    return {k: float(v) for k, v in out.items()}


def macro_now(macro: dict[str, pd.Series]) -> dict:
    """Where rates, the dollar, oil, VIX and credit stand, with 20-day changes."""
    out = {}
    for sid, label, kind in (("DGS10", "10 年期美债", "level"), ("DGS2", "2 年期美债", "level"), ("DTWEXBGS", "美元指数", "pct"),
                             ("DCOILWTICO", "WTI 原油", "pct"), ("VIXCLS", "VIX", "level"), ("BAMLH0A0HYM2", "高收益债利差", "level")):
        s = macro.get(sid)
        if s is None or len(s) < 22:
            continue
        last, prev = float(s.iloc[-1]), float(s.iloc[-21])
        change = f"{last - prev:+.2f}" if kind == "level" else f"{(last / prev - 1) * 100:+.1f}%"
        out[label] = {"最新": f"{last:.2f}", "20 个交易日变化": change, "日期": s.index[-1].isoformat()}
    return out


# ── loader ──

async def _series(ticker: str, asof: date) -> tuple[dict[str, list[tuple[date, float]]], str]:
    rows = await db.fetch_all(
        "SELECT DISTINCT ON (metric, period_end) metric, period_end, value, unit FROM scout.financials "
        "WHERE ticker = :t AND filed <= :d ORDER BY metric, period_end, filed DESC", {"t": ticker, "d": asof})
    out: dict[str, list[tuple[date, float]]] = {}
    for r in rows:
        out.setdefault(r["metric"], []).append((r["period_end"], r["value"]))
    unit = next((r["unit"] for r in rows if r["metric"] == "revenue"), "USD")
    return {k: sorted(v) for k, v in out.items()}, unit


async def _closes(ticker: str, since: date) -> pd.Series:
    rows = await db.fetch_all("SELECT date, close FROM scout.bars_daily WHERE ticker = :t AND date >= :d ORDER BY date",
                              {"t": ticker, "d": since})
    return pd.Series({r["date"]: r["close"] for r in rows}, dtype="float64")


async def _macro(since: date) -> dict[str, pd.Series]:
    rows = await db.fetch_all("SELECT series, date, value FROM scout.macro_series WHERE date >= :d ORDER BY date", {"d": since})
    out: dict[str, dict] = {}
    for r in rows:
        out.setdefault(r["series"], {})[r["date"]] = r["value"]
    return {k: pd.Series(v, dtype="float64") for k, v in out.items()}


async def _peers(sector: str | None, asof: date) -> dict[str, list[float]]:
    if not sector:
        return {}
    rows = await db.fetch_all(
        "SELECT DISTINCT ON (s.ticker) s.payload FROM scout.research_snapshots s JOIN scout.instruments i USING (ticker) "
        "WHERE s.kind = 'ownership' AND i.sector = :sec AND s.asof <= :d ORDER BY s.ticker, s.asof DESC",
        {"sec": sector, "d": asof})
    return {"val_ev_sales": [r["payload"].get("enterpriseToRevenue") for r in rows],
            "val_fwd_pe": [r["payload"].get("forwardPE") for r in rows]}


async def build(ticker: str, asof: date, features: dict, sector: str | None, earnings: dict, finnhub: dict,
                known_by: date | None = None) -> dict:
    """All new lens values for one ticker, plus context tables.

    `asof` is the price data date; `known_by` is the information cut-off for
    filings and snapshots (default today: a live view uses everything known now;
    a replay passes the historical date so nothing from later leaks in)."""
    from scout import research_data

    known = known_by or date.today()
    price = features.get("close")
    series, unit = await _series(ticker, known)
    trend, table = fundamental_trend(series, unit)
    est = (await research_data.latest(ticker, "estimates", known)) or {}
    own = (await research_data.latest(ticker, "ownership", known)) or {}
    opt = (await research_data.latest(ticker, "options", known)) or {}
    closes = await _closes(ticker, asof - timedelta(days=4 * YEAR))
    closes = closes[closes.index <= asof]
    spy = await _closes(universe.BENCHMARK, asof - timedelta(days=4 * YEAR))
    values: dict = {}
    values.update({k: v for k, v in trend.items() if not k.startswith("_")})
    values.update(valuation(price, series, trend, unit, own.get("payload") or {}, closes, await _peers(sector, known))
                  if price else {})
    actions = await db.fetch_all(
        "SELECT action_at, firm, to_grade, from_grade, action, pt_action, price_target, prior_target FROM scout.analyst_actions "
        "WHERE ticker = :t AND action_at <= :d ORDER BY action_at DESC LIMIT 60", {"t": ticker, "d": known + timedelta(days=1)})
    exp, actions_table = expectations(est.get("payload") or {}, price, actions, known, finnhub)
    values.update(exp)
    ivs = [r["iv"] for r in await db.fetch_all(
        "SELECT (payload->'expiries'->'m1'->>'atm_iv')::float AS iv FROM scout.research_snapshots "
        "WHERE ticker = :t AND kind = 'options' AND asof < :d AND payload->>'quality' = 'ok'", {"t": ticker, "d": known})
        if r["iv"]]
    options_vals = options_block(opt.get("payload"), features.get("vol_20"), ivs)
    values.update({k: v for k, v in options_vals.items() if not k.startswith("_")})
    reports = [r["filed"] for r in await db.fetch_all(
        "SELECT filed FROM scout.sec_filings WHERE ticker = :t AND form = '8-K' AND items LIKE '%2.02%' AND filed <= :d",
        {"t": ticker, "d": known})]
    reactions = earnings_reactions(reports, closes, spy)
    values.update(earnings_block((earnings or {}).get("last"), (earnings or {}).get("next"), reactions, asof, options_vals))
    own_vals, holders = ownership_block(own.get("payload") or {}, finnhub)
    values.update(own_vals)
    macro = await _macro(asof - timedelta(days=2 * YEAR))
    macro = {k: s[s.index <= asof] for k, s in macro.items()}
    values.update(macro_sensitivity(closes, spy[spy.index <= asof], macro))
    freshness = {k: {"日期": str(v["asof"]), "已过期": v["stale"]} for k, v in (("预期", est), ("持仓", own), ("期权", opt)) if v}
    return {"values": values, "unit": unit,
            "context": {"quarters": table, "analyst_actions": actions_table, "top_holders": holders,
                        "earnings_reactions": reactions[-6:], "unusual_options": ((opt.get("payload") or {}).get("unusual") or [])[:5],
                        "macro_now": macro_now(macro), "freshness": freshness}}
