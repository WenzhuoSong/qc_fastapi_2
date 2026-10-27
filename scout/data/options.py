"""Daily options snapshot summary from Yahoo option chains (free, delayed).

Yahoo's own implied volatility is unusable outside the session (bid/ask and
open interest read 0, IV collapses to ~0), so the snapshot is taken in the
last half hour of the session and IV is computed here with Black-Scholes from
the bid/ask midpoint; quotes with no market or a wide spread are skipped.
Open interest during the session is the previous day's. Dividends are ignored
(small bias for high-yield names).
"""
from __future__ import annotations

import math
from datetime import date

MIN_DAYS_FRONT = 7
MAX_SPREAD = 0.5            # (ask − bid) / mid
UNUSUAL_MIN_VOLUME = 500
UNUSUAL_OI_MULT = 2.0


def _ncdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def bs_price(spot: float, strike: float, t: float, r: float, vol: float, call: bool) -> float:
    if t <= 0 or vol <= 0:
        return max(0.0, (spot - strike) if call else (strike - spot))
    d1 = (math.log(spot / strike) + (r + 0.5 * vol * vol) * t) / (vol * math.sqrt(t))
    d2 = d1 - vol * math.sqrt(t)
    if call:
        return spot * _ncdf(d1) - strike * math.exp(-r * t) * _ncdf(d2)
    return strike * math.exp(-r * t) * _ncdf(-d2) - spot * _ncdf(-d1)


def implied_vol(price: float, spot: float, strike: float, t: float, r: float, call: bool) -> float | None:
    """Bisection on [1%, 500%]; None when the price is outside no-arbitrage bounds."""
    lo, hi = 0.01, 5.0
    if t <= 0 or price <= 0 or not (bs_price(spot, strike, t, r, lo, call) <= price <= bs_price(spot, strike, t, r, hi, call)):
        return None
    for _ in range(80):
        mid = (lo + hi) / 2
        if bs_price(spot, strike, t, r, mid, call) > price:
            hi = mid
        else:
            lo = mid
    return round((lo + hi) / 2, 5)


def _quotes(rows: list[dict], spot: float, t: float, r: float, call: bool) -> list[dict]:
    out = []
    for q in rows:
        bid, ask = q.get("bid") or 0.0, q.get("ask") or 0.0
        if bid <= 0 or ask <= 0 or ask < bid:
            continue
        mid = (bid + ask) / 2
        if (ask - bid) / mid > MAX_SPREAD:
            continue
        iv = implied_vol(mid, spot, q["strike"], t, r, call)
        if iv:
            out.append({**q, "mid": mid, "iv": iv})
    return out


def _nearest(quotes: list[dict], strike: float) -> dict | None:
    return min(quotes, key=lambda q: abs(q["strike"] - strike)) if quotes else None


def expiry_summary(calls: list[dict], puts: list[dict], spot: float, days: int, r: float) -> dict:
    """ATM IV, straddle-implied move and 10%-OTM skew for one expiry."""
    t = days / 365
    cq, pq = _quotes(calls, spot, t, r, True), _quotes(puts, spot, t, r, False)
    out: dict = {"days": days, "valid_quotes": len(cq) + len(pq)}
    atm_c, atm_p = _nearest(cq, spot), _nearest(pq, spot)
    ivs = [q["iv"] for q in (atm_c, atm_p) if q and abs(q["strike"] / spot - 1) <= 0.05]
    if ivs:
        out["atm_iv"] = round(sum(ivs) / len(ivs), 4)
    if atm_c and atm_p and atm_c["strike"] == atm_p["strike"]:
        out["straddle_move"] = round((atm_c["mid"] + atm_p["mid"]) / spot, 4)
    otm_p, otm_c = _nearest(pq, spot * 0.9), _nearest(cq, spot * 1.1)
    if "atm_iv" in out and otm_p and abs(otm_p["strike"] / spot - 0.9) <= 0.04:
        out["put_skew"] = round(otm_p["iv"] - out["atm_iv"], 4)
    if "atm_iv" in out and otm_c and abs(otm_c["strike"] / spot - 1.1) <= 0.04:
        out["call_skew"] = round(otm_c["iv"] - out["atm_iv"], 4)
    return out


def pick_expiries(expiries: list[str], today: date) -> dict[str, str]:
    """front (≥ 7 days), ~30 and ~60 days."""
    days = [(e, (date.fromisoformat(e) - today).days) for e in expiries]
    days = [(e, d) for e, d in days if d >= MIN_DAYS_FRONT]
    if not days:
        return {}
    out = {"front": days[0][0]}
    for label, target in (("m1", 30), ("m2", 60)):
        e, d = min(days, key=lambda x: abs(x[1] - target))
        if e not in out.values() and abs(d - target) <= 20:
            out[label] = e
    return out


def summarize(chains: dict[str, tuple[list[dict], list[dict]]], spot: float, today: date, r: float) -> dict:
    """chains: expiry → (calls, puts) rows with strike/bid/ask/volume/openInterest."""
    picked = pick_expiries(sorted(chains), today)
    out: dict = {"spot": spot, "expiries": {}}
    for label, exp in picked.items():
        calls, puts = chains[exp]
        out["expiries"][label] = {"expiry": exp, **expiry_summary(calls, puts, spot, (date.fromisoformat(exp) - today).days, r)}
    cv = sum(q.get("volume") or 0 for c, _ in chains.values() for q in c)
    pv = sum(q.get("volume") or 0 for _, p in chains.values() for q in p)
    coi = sum(q.get("openInterest") or 0 for c, _ in chains.values() for q in c)
    poi = sum(q.get("openInterest") or 0 for _, p in chains.values() for q in p)
    out.update({"call_volume": cv, "put_volume": pv, "call_oi": coi, "put_oi": poi,
                "put_call_volume": round(pv / cv, 3) if cv else None, "put_call_oi": round(poi / coi, 3) if coi else None})
    unusual = []
    if coi + poi == 0:                     # Yahoo returns no open interest outside the session
        out.update({"put_call_oi": None, "oi_missing": True, "unusual": [], "quality": "thin"})
        return out
    for exp, (calls, puts) in chains.items():
        for kind, rows in (("call", calls), ("put", puts)):
            for q in rows:
                vol, oi = q.get("volume") or 0, q.get("openInterest") or 0
                if vol >= UNUSUAL_MIN_VOLUME and vol >= UNUSUAL_OI_MULT * max(oi, 1):
                    unusual.append({"type": kind, "expiry": exp, "strike": q["strike"], "volume": int(vol), "oi": int(oi),
                                    "moneyness": round(q["strike"] / spot - 1, 4), "last": q.get("lastPrice")})
    out["unusual"] = sorted(unusual, key=lambda u: -u["volume"])[:8]
    front = out["expiries"].get("front", {})
    out["quality"] = "ok" if front.get("valid_quotes", 0) >= 6 else "thin"
    return out
