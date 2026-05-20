"""
Sector and factor rotation detector.

This is a pure-Python signal layer built from QC ETF features. It intentionally
does not make trading decisions; it summarizes where relative strength appears
to be improving or fading so downstream agents can reason about allocation.
"""
from __future__ import annotations

from typing import Any


SECTOR_LABELS = {
    "XLK": "technology",
    "XLF": "financials",
    "XLV": "healthcare",
    "XLE": "energy",
    "XLY": "consumer_discretionary",
    "XLP": "consumer_staples",
    "XLU": "utilities",
    "XLI": "industrials",
    "XLRE": "real_estate",
    "XLB": "materials",
    "XLC": "communication_services",
}

FACTOR_LABELS = {
    "QQQ": "growth_large_cap",
    "IWM": "small_cap",
    "RSP": "equal_weight_sp500",
    "VUG": "growth",
    "VTV": "value",
    "USMV": "minimum_volatility",
    "SMH": "semiconductors",
    "SOXX": "semiconductors",
    "XSD": "semiconductors_broad",
    "IGV": "software",
    "CIBR": "cybersecurity",
    "HACK": "cybersecurity",
    "AIQ": "ai_thematic",
    "BOTZ": "robotics_ai",
    "GLD": "gold",
    "TLT": "long_treasury",
    "IEF": "intermediate_treasury",
    "BND": "aggregate_bonds",
    "SGOV": "cash_proxy",
}

DEFENSIVE_TICKERS = {"XLP", "XLU", "XLV", "XLRE", "USMV", "TLT", "IEF", "BND", "SGOV", "GLD"}
CYCLICAL_TICKERS = {"XLK", "XLY", "XLC", "XLI", "XLF", "XLE", "XLB", "IWM", "QQQ", "VUG"}
SAFE_HAVEN_TICKERS = {"SGOV", "IEF", "TLT", "BND", "GLD"}


def detect_sector_rotation(holdings: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Compute a compact rotation signal from current ETF feature rows.

    Expected input fields are best-effort: ticker, mom_20d, mom_60d,
    daily_return_pct, return_5d, hist_vol_20d, universe_role.
    Missing fields degrade gracefully.
    """
    rows = [_build_rotation_row(row) for row in holdings]
    rows = [row for row in rows if row]
    if not rows:
        return {
            "has_signal": False,
            "rotation_label": "insufficient_data",
            "risk_appetite_score": None,
            "cyclical_score": None,
            "defensive_score": None,
            "safe_haven_score": None,
            "strongest_ticker": None,
            "weakest_ticker": None,
            "leaders": [],
            "laggards": [],
            "sector_rank": [],
            "factor_rank": [],
            "notes": ["No usable ETF momentum or return fields were available."],
        }

    sector_rows = [row for row in rows if row["ticker"] in SECTOR_LABELS]
    factor_rows = [row for row in rows if row["ticker"] in FACTOR_LABELS]
    ranked = sorted(rows, key=lambda row: row["score"], reverse=True)

    leaders = [_public_row(row) for row in ranked[:5]]
    laggards = [_public_row(row) for row in ranked[-5:]]

    sector_rank = [_public_row(row) for row in sorted(sector_rows, key=lambda row: row["score"], reverse=True)]
    factor_rank = [_public_row(row) for row in sorted(factor_rows, key=lambda row: row["score"], reverse=True)[:8]]

    cyclical_score = _average(row["score"] for row in rows if row["ticker"] in CYCLICAL_TICKERS)
    defensive_score = _average(row["score"] for row in rows if row["ticker"] in DEFENSIVE_TICKERS)
    safe_haven_score = _average(row["score"] for row in rows if row["ticker"] in SAFE_HAVEN_TICKERS)
    risk_appetite_score = _round_or_none(cyclical_score - defensive_score)

    strongest = leaders[0]["ticker"] if leaders else None
    weakest = laggards[0]["ticker"] if laggards else None
    rotation_label = _classify_rotation(risk_appetite_score, safe_haven_score, leaders)

    return {
        "has_signal": bool(rows),
        "rotation_label": rotation_label,
        "risk_appetite_score": risk_appetite_score,
        "cyclical_score": _round_or_none(cyclical_score),
        "defensive_score": _round_or_none(defensive_score),
        "safe_haven_score": _round_or_none(safe_haven_score),
        "strongest_ticker": strongest,
        "weakest_ticker": weakest,
        "leaders": leaders,
        "laggards": laggards,
        "sector_rank": sector_rank,
        "factor_rank": factor_rank,
        "notes": _build_notes(rotation_label, leaders, laggards, risk_appetite_score),
    }


def format_rotation_for_prompt(rotation: dict[str, Any]) -> str:
    """Compact prompt section for Researcher."""
    if not rotation or not rotation.get("has_signal"):
        return "(insufficient ETF feature data for rotation analysis)"

    def _fmt(rows: list[dict[str, Any]]) -> str:
        parts = []
        for row in rows[:5]:
            parts.append(
                f"{row['ticker']}({row.get('label', 'unknown')}, "
                f"score={row.get('score'):+.3f}, "
                f"mom60={_fmt_pct(row.get('mom_60d'))}, "
                f"r5={_fmt_pct(row.get('return_5d'))})"
            )
        return "; ".join(parts) if parts else "(none)"

    return (
        f"Rotation label: {rotation.get('rotation_label')}\n"
        f"Risk appetite score: {rotation.get('risk_appetite_score')}\n"
        f"Leaders: {_fmt(rotation.get('leaders') or [])}\n"
        f"Laggards: {_fmt(rotation.get('laggards') or [])}\n"
        f"Notes: {' '.join(rotation.get('notes') or [])}"
    )


def rotation_signal_strengths(rotation: dict[str, Any] | None) -> dict[str, float]:
    """Return deterministic per-ticker rotation signals in [-1, 1].

    Positive values indicate relative leadership, negative values indicate
    lagging behavior. The scale is cross-sectional and deterministic for a
    given rotation payload.
    """
    if not rotation or not rotation.get("has_signal"):
        return {}
    scores: dict[str, float] = {}
    for bucket in ("leaders", "laggards", "sector_rank", "factor_rank"):
        for row in rotation.get(bucket) or []:
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("ticker") or "").upper().strip()
            score = _to_float(row.get("score"))
            if not ticker or ticker == "CASH" or score is None:
                continue
            scores[ticker] = score
    max_abs = max((abs(value) for value in scores.values()), default=0.0)
    if max_abs <= 1e-12:
        return {ticker: 0.0 for ticker in sorted(scores)}
    return {
        ticker: round(max(min(score / max_abs, 1.0), -1.0), 6)
        for ticker, score in sorted(scores.items())
    }


def _build_rotation_row(raw: dict[str, Any]) -> dict[str, Any] | None:
    ticker = (raw.get("ticker") or "").upper().strip()
    if not ticker or ticker == "CASH":
        return None

    mom60 = _to_float(raw.get("mom_60d"))
    mom20 = _to_float(raw.get("mom_20d"))
    ret5 = _to_float(raw.get("return_5d"))
    ret1 = _to_float(raw.get("daily_return_pct"))
    vol = _to_float(raw.get("hist_vol_20d")) or _to_float(raw.get("atr_pct")) or 0.02

    if all(value is None for value in (mom60, mom20, ret5, ret1)):
        return None

    momentum = 0.50 * (mom60 or 0.0) + 0.25 * (mom20 or 0.0) + 0.15 * (ret5 or 0.0) + 0.10 * (ret1 or 0.0)
    volatility_penalty = min(max(vol, 0.0), 0.08) * 0.35
    score = momentum - volatility_penalty

    return {
        "ticker": ticker,
        "label": SECTOR_LABELS.get(ticker) or FACTOR_LABELS.get(ticker) or raw.get("universe_role") or "other",
        "score": round(score, 6),
        "mom_60d": mom60,
        "mom_20d": mom20,
        "return_5d": ret5,
        "daily_return_pct": ret1,
        "hist_vol_20d": vol,
        "universe_role": raw.get("universe_role"),
    }


def _public_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "ticker": row["ticker"],
        "label": row["label"],
        "score": row["score"],
        "mom_60d": _round_or_none(row.get("mom_60d")),
        "mom_20d": _round_or_none(row.get("mom_20d")),
        "return_5d": _round_or_none(row.get("return_5d")),
        "daily_return_pct": _round_or_none(row.get("daily_return_pct")),
        "universe_role": row.get("universe_role"),
    }


def _classify_rotation(
    risk_appetite_score: float | None,
    safe_haven_score: float | None,
    leaders: list[dict[str, Any]],
) -> str:
    leader_tickers = {row.get("ticker") for row in leaders[:3]}
    safe_haven_led = bool(leader_tickers & SAFE_HAVEN_TICKERS)

    if risk_appetite_score is None:
        return "insufficient_data"
    if safe_haven_led and (safe_haven_score or 0.0) > 0.01:
        return "defensive_rotation"
    if risk_appetite_score > 0.015:
        return "risk_on_rotation"
    if risk_appetite_score < -0.015:
        return "risk_off_rotation"
    return "mixed_rotation"


def _build_notes(
    label: str,
    leaders: list[dict[str, Any]],
    laggards: list[dict[str, Any]],
    risk_appetite_score: float | None,
) -> list[str]:
    notes: list[str] = []
    if leaders:
        notes.append(f"Top leadership is led by {', '.join(row['ticker'] for row in leaders[:3])}.")
    if laggards:
        notes.append(f"Weakest groups are {', '.join(row['ticker'] for row in laggards[:3])}.")
    if risk_appetite_score is not None:
        notes.append(f"Risk appetite spread is {risk_appetite_score:+.3f}.")
    if label == "defensive_rotation":
        notes.append("Safe-haven or defensive ETFs are leading; avoid treating sector strength as broad risk-on confirmation.")
    elif label == "risk_on_rotation":
        notes.append("Cyclical/growth leadership is stronger than defensive leadership.")
    elif label == "risk_off_rotation":
        notes.append("Defensive leadership is stronger than cyclical leadership.")
    return notes


def _average(values) -> float:
    vals = [float(value) for value in values if value is not None]
    if not vals:
        return 0.0
    return sum(vals) / len(vals)


def _to_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _round_or_none(value: Any, digits: int = 6) -> float | None:
    value = _to_float(value)
    return round(value, digits) if value is not None else None


def _fmt_pct(value: Any) -> str:
    value = _to_float(value)
    if value is None:
        return "n/a"
    return f"{value:+.1%}"
