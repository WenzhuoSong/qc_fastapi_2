"""Research data from Yahoo via yfinance (free, unofficial): estimates and
revisions, price targets, rating changes, ownership and short interest, and
option chains. Each call is isolated so one missing module never sinks the
rest; everything returned is plain JSON-able data."""
from __future__ import annotations

import logging
import math
from datetime import date, datetime, timezone

import pandas as pd

from scout.data import options as opt

logger = logging.getLogger("scout.data.yahoo_research")

OWNERSHIP_FIELDS = ("heldPercentInsiders", "heldPercentInstitutions", "sharesShort", "sharesShortPriorMonth",
                    "shortPercentOfFloat", "shortRatio", "dateShortInterest", "floatShares", "sharesOutstanding",
                    "recommendationMean", "numberOfAnalystOpinions", "forwardPE", "trailingPE", "enterpriseToEbitda",
                    "enterpriseToRevenue", "enterpriseValue", "marketCap", "beta", "dividendYield")


def _clean(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, (pd.Timestamp, datetime, date)):
        return v.isoformat()
    if hasattr(v, "item"):
        return _clean(v.item())
    return v


def _table(df: pd.DataFrame | None) -> dict:
    """period-indexed frame → {period: {column: value}}."""
    if df is None or getattr(df, "empty", True):
        return {}
    return {str(idx): {str(k): _clean(v) for k, v in row.items()} for idx, row in df.iterrows()}


def _safe(fn, label: str, ticker: str):
    try:
        return fn()
    except Exception as exc:
        logger.info("yahoo %s for %s unavailable: %s", label, ticker, exc)
        return None


def estimates(ticker: str) -> dict:
    """Consensus EPS / revenue, the 7–90-day trend and revision counts, price targets."""
    import yfinance as yf

    t = yf.Ticker(ticker)
    out = {
        "earnings_estimate": _table(_safe(lambda: t.earnings_estimate, "earnings_estimate", ticker)),
        "revenue_estimate": _table(_safe(lambda: t.revenue_estimate, "revenue_estimate", ticker)),
        "eps_trend": _table(_safe(lambda: t.eps_trend, "eps_trend", ticker)),
        "eps_revisions": _table(_safe(lambda: t.eps_revisions, "eps_revisions", ticker)),
        "growth_estimates": _table(_safe(lambda: t.growth_estimates, "growth_estimates", ticker)),
    }
    pt = _safe(lambda: t.analyst_price_targets, "price_targets", ticker)
    if isinstance(pt, dict):
        out["price_targets"] = {k: _clean(v) for k, v in pt.items()}
    return {k: v for k, v in out.items() if v}


def analyst_actions(ticker: str) -> list[dict]:
    import yfinance as yf

    df = _safe(lambda: yf.Ticker(ticker).upgrades_downgrades, "upgrades_downgrades", ticker)
    if df is None or df.empty:
        return []
    rows = []
    for ts, r in df.iterrows():
        at = pd.Timestamp(ts)
        at = at.tz_localize("UTC") if at.tzinfo is None else at.tz_convert("UTC")
        rows.append({"action_at": at.to_pydatetime(), "firm": str(r.get("Firm") or "").strip(),
                     "to_grade": _clean(r.get("ToGrade")) or None, "from_grade": _clean(r.get("FromGrade")) or None,
                     "action": _clean(r.get("Action")), "pt_action": _clean(r.get("priceTargetAction")),
                     "price_target": _clean(r.get("currentPriceTarget")) or None,
                     "prior_target": _clean(r.get("priorPriceTarget")) or None})
    return [r for r in rows if r["firm"]]


def ownership(ticker: str) -> dict:
    """Short interest, insider / institutional share, valuation multiples, top holders."""
    import yfinance as yf

    t = yf.Ticker(ticker)
    info = _safe(lambda: t.info, "info", ticker) or {}
    out = {k: _clean(info.get(k)) for k in OWNERSHIP_FIELDS if info.get(k) is not None}
    if out.get("dateShortInterest"):
        out["dateShortInterest"] = datetime.fromtimestamp(int(out["dateShortInterest"]), timezone.utc).date().isoformat()
    holders = _safe(lambda: t.institutional_holders, "institutional_holders", ticker)
    if holders is not None and not holders.empty:
        out["top_institutions"] = [{str(k): _clean(v) for k, v in row.items()} for _, row in holders.head(10).iterrows()]
    return out


def option_chains(ticker: str, today: date, max_expiries: int = 4) -> tuple[float | None, dict]:
    """(spot, expiry → (calls, puts)) for the expiries the summary needs plus the nearest few."""
    import yfinance as yf

    t = yf.Ticker(ticker)
    expiries = list(_safe(lambda: t.options, "options", ticker) or [])
    if not expiries:
        return None, {}
    spot = _safe(lambda: float(t.fast_info["last_price"]), "spot", ticker)
    if not spot:
        return None, {}
    wanted = set(opt.pick_expiries(expiries, today).values()) | set(expiries[:max_expiries])
    cols = ["strike", "bid", "ask", "lastPrice", "volume", "openInterest"]
    chains = {}
    for exp in sorted(wanted):
        ch = _safe(lambda e=exp: t.option_chain(e), f"chain {exp}", ticker)
        if ch is None:
            continue
        def rows(df: pd.DataFrame) -> list[dict]:
            return [{c: _clean(v) for c, v in r.items()} for r in df[[c for c in cols if c in df.columns]].to_dict("records")]
        chains[exp] = (rows(ch.calls), rows(ch.puts))
    return spot, chains
