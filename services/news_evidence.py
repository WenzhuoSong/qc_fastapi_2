"""
Deterministic news evidence scoring.

News evidence is an input to agents and style selection. It never writes target
weights directly.
"""
from __future__ import annotations

import time
from collections import Counter, defaultdict
from typing import Any


MIN_EFFECTIVE_CREDIBILITY_FOR_AGENT = 0.10

SOURCE_PRIORS = {
    "reuters": 0.95,
    "bloomberg": 0.95,
    "federal reserve": 0.95,
    "sec": 0.95,
    "cnbc": 0.85,
    "wall street journal": 0.85,
    "wsj": 0.85,
    "marketwatch": 0.85,
    "financial times": 0.85,
    "ft": 0.85,
    "yahoo": 0.65,
    "seeking alpha": 0.65,
}

IMPACT_MULTIPLIERS = {"high": 1.20, "medium": 1.00, "low": 0.70}
RELEVANCE_MULTIPLIERS = {
    "direct": 1.20,
    "sector": 1.00,
    "macro": 1.00,
    "indirect": 0.70,
    "not_relevant": 0.00,
    "noise": 0.00,
}
FRESHNESS_MULTIPLIERS = {
    "fresh": 1.10,
    "usable": 1.00,
    "stale_for_trading": 0.60,
    "stale": 0.20,
    "unknown": 0.50,
}

HARD_RISK_TYPES = {
    "bank_crisis",
    "credit_stress",
    "trading_halt",
    "halt",
    "fraud",
    "lawsuit_material",
    "lawsuit",
    "liquidity_crisis",
    "sanctions",
    "war_escalation",
    "emergency",
    "critical",
}

POSITIVE_BIAS = {"positive", "bullish", "risk_on", "tailwind"}
NEGATIVE_BIAS = {"negative", "bearish", "risk_off", "headwind"}


def build_news_evidence(
    brief: dict[str, Any] | None,
    *,
    now_ts: int | None = None,
) -> dict[str, Any]:
    """
    Build ranked news evidence from the existing market brief.

    Expected brief fields:
      - news_context.macro_signals
      - news_context.ticker_signals
      - per_ticker_news
      - hard_risks_map
    """
    brief = brief or {}
    now = int(now_ts or time.time())
    context = brief.get("news_context") or {}
    per_ticker_news = brief.get("per_ticker_news") or {}
    hard_risks_map = brief.get("hard_risks_map") or {}

    ticker_scores: dict[str, dict[str, Any]] = {}
    ignored_items: list[dict[str, Any]] = []
    hard_risk_events: dict[str, list[str]] = {}

    for ticker, items in sorted(per_ticker_news.items()):
        scored_items = [
            score_news_item(item, ticker=ticker, now_ts=now, hard_risks=hard_risks_map.get(ticker))
            for item in (items or [])
        ]
        visible = [item for item in scored_items if item["action_bias"] != "ignore"]
        ignored_items.extend(
            _compact_item(item, ticker=ticker)
            for item in scored_items
            if item["action_bias"] == "ignore"
        )
        hard_items = [
            item for item in scored_items
            if item["action_bias"] == "block_new_buy" or item["hard_risk_types"]
        ]
        if hard_items:
            hard_risk_events[ticker] = sorted(
                {
                    risk
                    for item in hard_items
                    for risk in (item.get("hard_risk_types") or ["hard_risk_event"])
                }
            )
        ticker_scores[ticker] = _aggregate_ticker_score(ticker, visible, scored_items)

    structured_ticker = context.get("ticker_signals") or {}
    for ticker, signal in sorted(structured_ticker.items()):
        if ticker in ticker_scores:
            continue
        ticker_scores[ticker] = _score_structured_ticker_signal(ticker, signal)

    macro_score = _score_macro_news(context.get("macro_signals") or [], context)
    data_gaps = list(context.get("data_gaps") or [])
    if context.get("_stale_warning"):
        data_gaps.append(str(context.get("_stale_warning")))
    if not per_ticker_news and not structured_ticker:
        data_gaps.append("no ticker news evidence available")
    if not context.get("macro_signals"):
        data_gaps.append("no structured macro news signals available")

    return {
        "macro_news_score": macro_score,
        "ticker_news_scores": ticker_scores,
        "hard_risk_events": hard_risk_events,
        "ignored_items": ignored_items[:50],
        "data_gaps": _unique(data_gaps),
    }


def score_news_item(
    item: dict[str, Any],
    *,
    ticker: str | None = None,
    now_ts: int | None = None,
    hard_risks: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = int(now_ts or time.time())
    source = str(item.get("source") or "").strip()
    source_credibility = _source_credibility(item.get("credibility"), source)
    relevance = _normalize_relevance(item.get("relevance"))
    freshness = _freshness_label(item.get("datetime"), now)
    market_impact = _infer_market_impact(item, hard_risks)
    sentiment = _normalize_sentiment(item.get("sentiment"))

    effective = round(
        source_credibility
        * IMPACT_MULTIPLIERS.get(market_impact, 1.0)
        * RELEVANCE_MULTIPLIERS.get(relevance, 0.70)
        * FRESHNESS_MULTIPLIERS.get(freshness, 0.50),
        4,
    )
    hard_types = _hard_risk_types(item, hard_risks)
    action_bias = _action_bias(
        sentiment=sentiment,
        relevance=relevance,
        effective_credibility=effective,
        hard_risk_types=hard_types,
    )

    return {
        "ticker": (ticker or item.get("ticker") or "").upper(),
        "headline": item.get("headline") or "",
        "source": source,
        "source_credibility": round(source_credibility, 4),
        "effective_credibility": effective,
        "sentiment": sentiment,
        "relevance": relevance,
        "freshness": freshness,
        "market_impact": market_impact,
        "time_horizon": _time_horizon(freshness, market_impact),
        "action_bias": action_bias,
        "hard_risk_types": hard_types,
        "summary": item.get("llm_summary") or item.get("summary") or "",
    }


def _aggregate_ticker_score(
    ticker: str,
    visible_items: list[dict[str, Any]],
    all_items: list[dict[str, Any]],
) -> dict[str, Any]:
    if not all_items:
        return {
            "bias": "neutral",
            "confidence": "low",
            "relevance": "noise",
            "source_credibility": 0.0,
            "freshness": "unknown",
            "market_impact": "low",
            "time_horizon": "medium_term",
            "action_bias": "ignore",
            "supporting_items": [],
            "conflicting_items": [],
        }

    if not visible_items:
        return {
            "bias": "neutral",
            "confidence": "low",
            "relevance": "noise",
            "source_credibility": round(max(i["source_credibility"] for i in all_items), 4),
            "freshness": _best_by_order((i["freshness"] for i in all_items), ["fresh", "usable", "stale_for_trading", "stale", "unknown"]),
            "market_impact": "low",
            "time_horizon": "medium_term",
            "action_bias": "ignore",
            "supporting_items": [],
            "conflicting_items": [],
        }

    if any(i["action_bias"] == "block_new_buy" for i in visible_items):
        action_bias = "block_new_buy"
    else:
        action_bias = _aggregate_action_bias(visible_items)

    bias = _aggregate_bias(visible_items)
    return {
        "bias": bias,
        "confidence": _confidence(visible_items),
        "relevance": _best_by_order((i["relevance"] for i in visible_items), ["direct", "sector", "macro", "indirect", "noise"]),
        "source_credibility": round(max(i["source_credibility"] for i in visible_items), 4),
        "effective_credibility": round(max(i["effective_credibility"] for i in visible_items), 4),
        "freshness": _best_by_order((i["freshness"] for i in visible_items), ["fresh", "usable", "stale_for_trading", "stale", "unknown"]),
        "market_impact": _best_by_order((i["market_impact"] for i in visible_items), ["high", "medium", "low"]),
        "time_horizon": _best_by_order((i["time_horizon"] for i in visible_items), ["intraday", "short_term", "medium_term"]),
        "action_bias": action_bias,
        "supporting_items": [_compact_item(item, ticker=ticker) for item in visible_items[:5]],
        "conflicting_items": [
            _compact_item(item, ticker=ticker)
            for item in visible_items
            if _bias_score(item["sentiment"]) * _bias_score(bias) < 0
        ][:5],
    }


def _score_structured_ticker_signal(ticker: str, signal: Any) -> dict[str, Any]:
    if not isinstance(signal, dict):
        signal = {}
    sentiment = _normalize_sentiment(signal.get("sentiment") or signal.get("overall_signal"))
    relevance = _normalize_relevance(signal.get("relevance") or "direct")
    confidence = str(signal.get("confidence") or "medium")
    action_bias = _action_bias(
        sentiment=sentiment,
        relevance=relevance,
        effective_credibility=0.5,
        hard_risk_types=[],
    )
    return {
        "bias": sentiment,
        "confidence": confidence if confidence in {"high", "medium", "low"} else "medium",
        "relevance": relevance,
        "source_credibility": 0.5,
        "effective_credibility": 0.5,
        "freshness": "unknown",
        "market_impact": "medium",
        "time_horizon": "short_term",
        "action_bias": action_bias,
        "supporting_items": [{"ticker": ticker, "headline": "structured ticker signal", "action_bias": action_bias}],
        "conflicting_items": [],
    }


def _score_macro_news(signals: list[Any], context: dict[str, Any]) -> dict[str, Any]:
    if not signals:
        quality = "missing" if context.get("_fallback") else "limited"
        return {
            "overall_bias": "neutral",
            "confidence": "low",
            "dominant_themes": [],
            "market_impact": "low",
            "time_horizon": "medium_term",
            "data_quality": quality,
            "warnings": list(context.get("data_gaps") or []),
        }

    bias_scores = []
    impacts = []
    horizons = []
    themes = []
    confidences = []
    for raw in signals:
        if not isinstance(raw, dict):
            continue
        direction = raw.get("direction") or raw.get("impact_bias") or raw.get("bias")
        bias_scores.append(_bias_score(_normalize_sentiment(direction)))
        impact = str(raw.get("impact") or raw.get("market_impact") or "medium").lower()
        impacts.append(impact if impact in IMPACT_MULTIPLIERS else "medium")
        horizon = str(raw.get("time_horizon") or "short_term")
        horizons.append(horizon if horizon in {"intraday", "short_term", "medium_term"} else "short_term")
        theme = raw.get("driver") or raw.get("theme") or raw.get("keyword")
        if theme:
            themes.append(str(theme))
        conf = str(raw.get("confidence") or "medium")
        confidences.append(conf if conf in {"high", "medium", "low"} else "medium")

    total = sum(bias_scores)
    if total > 0.25:
        overall = "positive"
    elif total < -0.25:
        overall = "negative"
    elif any(v > 0 for v in bias_scores) and any(v < 0 for v in bias_scores):
        overall = "mixed"
    else:
        overall = "neutral"

    return {
        "overall_bias": overall,
        "confidence": _best_by_order(confidences, ["high", "medium", "low"]) if confidences else "low",
        "dominant_themes": [theme for theme, _ in Counter(themes).most_common(5)],
        "market_impact": _best_by_order(impacts, ["high", "medium", "low"]),
        "time_horizon": _best_by_order(horizons, ["intraday", "short_term", "medium_term"]),
        "data_quality": "stale" if context.get("_stale_warning") else "fresh",
        "warnings": [context["_stale_warning"]] if context.get("_stale_warning") else [],
    }


def _source_credibility(value: Any, source: str) -> float:
    try:
        if value is not None:
            numeric = float(value)
            return round(numeric / 100.0 if numeric > 1 else numeric, 4)
    except (TypeError, ValueError):
        pass
    source_l = source.lower()
    for key, score in SOURCE_PRIORS.items():
        if key in source_l:
            return score
    return 0.40


def _normalize_relevance(value: Any) -> str:
    raw = str(value or "indirect").lower().strip()
    if raw in {"direct", "sector", "macro", "indirect", "noise"}:
        return raw
    if raw in {"not_relevant", "not relevant", "irrelevant"}:
        return "noise"
    return "indirect"


def _normalize_sentiment(value: Any) -> str:
    raw = str(value or "neutral").lower().strip()
    if raw in POSITIVE_BIAS:
        return "positive"
    if raw in NEGATIVE_BIAS:
        return "negative"
    return "neutral"


def _freshness_label(ts_value: Any, now_ts: int) -> str:
    try:
        ts = int(float(ts_value))
    except (TypeError, ValueError):
        return "unknown"
    age = max(now_ts - ts, 0)
    if age < 6 * 3600:
        return "fresh"
    if age < 24 * 3600:
        return "usable"
    if age < 3 * 24 * 3600:
        return "stale_for_trading"
    return "stale"


def _infer_market_impact(item: dict[str, Any], hard_risks: dict[str, Any] | None) -> str:
    if _hard_risk_types(item, hard_risks):
        return "high"
    text = " ".join(
        str(item.get(k) or "")
        for k in ("headline", "llm_summary", "summary", "category")
    ).lower()
    high_terms = ("fed", "cpi", "credit stress", "bank crisis", "war", "halt", "fraud", "sanction")
    medium_terms = ("earnings", "guidance", "policy", "sector", "rates", "inflation", "oil")
    if any(term in text for term in high_terms):
        return "high"
    if any(term in text for term in medium_terms):
        return "medium"
    return "low"


def _time_horizon(freshness: str, impact: str) -> str:
    if impact == "high" and freshness in {"fresh", "usable"}:
        return "intraday"
    if freshness in {"fresh", "usable", "stale_for_trading"}:
        return "short_term"
    return "medium_term"


def _hard_risk_types(item: dict[str, Any], hard_risks: dict[str, Any] | None) -> list[str]:
    risks: set[str] = set()
    if item.get("is_hard_event"):
        risks.add("hard_risk_event")
    if isinstance(hard_risks, dict):
        risks.update(str(k) for k in hard_risks.keys())
    text = " ".join(str(item.get(k) or "") for k in ("headline", "llm_summary", "summary", "category")).lower()
    for risk in HARD_RISK_TYPES:
        if risk.replace("_", " ") in text or risk in text:
            risks.add(risk)
    return sorted(risks)


def _action_bias(
    *,
    sentiment: str,
    relevance: str,
    effective_credibility: float,
    hard_risk_types: list[str],
) -> str:
    if hard_risk_types:
        return "block_new_buy"
    if relevance == "noise" or effective_credibility < MIN_EFFECTIVE_CREDIBILITY_FOR_AGENT:
        return "ignore"
    if sentiment == "negative":
        return "reduce_or_wait"
    if sentiment == "positive" and effective_credibility >= 0.65:
        return "allow_overweight"
    if sentiment == "positive":
        return "confirm_existing_signal"
    return "confirm_existing_signal" if effective_credibility >= 0.50 else "ignore"


def _aggregate_action_bias(items: list[dict[str, Any]]) -> str:
    score = sum(_bias_score(i["action_bias"]) * i["effective_credibility"] for i in items)
    if score <= -0.35:
        return "reduce_or_wait"
    if score >= 0.65:
        return "allow_overweight"
    if score > 0:
        return "confirm_existing_signal"
    return "ignore"


def _aggregate_bias(items: list[dict[str, Any]]) -> str:
    score = sum(_bias_score(i["sentiment"]) * i["effective_credibility"] for i in items)
    if score > 0.20:
        return "positive"
    if score < -0.20:
        return "negative"
    return "neutral"


def _bias_score(value: str) -> float:
    if value in {"positive", "allow_overweight", "confirm_existing_signal"}:
        return 1.0
    if value in {"negative", "reduce_or_wait", "block_new_buy"}:
        return -1.0
    return 0.0


def _confidence(items: list[dict[str, Any]]) -> str:
    best = max((i["effective_credibility"] for i in items), default=0.0)
    if best >= 0.85:
        return "high"
    if best >= 0.45:
        return "medium"
    return "low"


def _best_by_order(values, order: list[str]) -> str:
    clean = [str(v) for v in values if v]
    for item in order:
        if item in clean:
            return item
    return order[-1] if order else ""


def _compact_item(item: dict[str, Any], *, ticker: str | None = None) -> dict[str, Any]:
    return {
        "ticker": ticker or item.get("ticker"),
        "headline": str(item.get("headline") or "")[:160],
        "source": item.get("source"),
        "effective_credibility": item.get("effective_credibility"),
        "sentiment": item.get("sentiment"),
        "relevance": item.get("relevance"),
        "freshness": item.get("freshness"),
        "market_impact": item.get("market_impact"),
        "action_bias": item.get("action_bias"),
        "hard_risk_types": item.get("hard_risk_types") or [],
    }


def _unique(values) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value)
        if text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out
