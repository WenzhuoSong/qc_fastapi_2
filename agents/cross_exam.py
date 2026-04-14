# agents/cross_exam.py
"""
Stage 4c: Cross-examination — after parallel Bull/Bear drafts, each side receives
the opponent's thesis and produces a short targeted rebuttal (no weights).

Bull ← Bear's draft → rebuttal attacking the bear case.
Bear ← Bull's draft → rebuttal attacking the bull case.
"""
from __future__ import annotations

import json
import logging
import time

from openai import AsyncOpenAI

from config import get_settings

logger = logging.getLogger("qc_fastapi_2.cross_exam")
settings = get_settings()

_client: AsyncOpenAI | None = None

BULL_CROSS_EXAM_PROMPT = """You are the Bull analyst. The Bear analyst has published a thesis (JSON below).

Your task: cross-examine the Bear's case. Attack logical gaps, overstated risks, or inconsistencies
with the Stage 3 research_report data. Be precise and evidence-based — not rhetorical fluff.

You receive only key fields from research_report: market_regime, macro_outlook, cross_signal_insights.
Use these authoritative data points to fact-check the Bear's arguments.

Rules:
- Do NOT output portfolio weights or percentages.
- 2–4 sentences OR up to 5 bullet strings in rebuttal_points.
- JSON only."""

BEAR_CROSS_EXAM_PROMPT = """You are the Bear analyst. The Bull analyst has published a thesis (JSON below).

Your task: cross-examine the Bull's case. Attack complacency, stretched valuations, ignored flags,
or conflicts with the Stage 3 research_report data. Be precise and evidence-based.

You receive only key fields from research_report: market_regime, macro_outlook, cross_signal_insights.
Use these authoritative data points to fact-check the Bull's arguments.

Rules:
- Do NOT output portfolio weights or percentages.
- 2–4 sentences OR up to 5 bullet strings in rebuttal_points.
- JSON only."""

MAX_CROSS_EXAM_TOKENS = 500


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=settings.openai_api_key)
    return _client


async def run_bull_cross_exam_async(bear_draft: dict, research_report: dict) -> dict:
    """Bull reads Bear's draft + research_report; returns rebuttal attacking Bear."""
    return await _run_cross_exam(
        role="BULL",
        system=BULL_CROSS_EXAM_PROMPT,
        opponent_draft=bear_draft,
        research_report=research_report,
    )


async def run_bear_cross_exam_async(bull_draft: dict, research_report: dict) -> dict:
    """Bear reads Bull's draft + research_report; returns rebuttal attacking Bull."""
    return await _run_cross_exam(
        role="BEAR",
        system=BEAR_CROSS_EXAM_PROMPT,
        opponent_draft=bull_draft,
        research_report=research_report,
    )


def _extract_key_research_fields(research_report: dict) -> dict:
    """Extract only essential fields for cross-examination to avoid truncation.

    Additionally truncates large fields to keep total JSON size under ~8000 chars.
    """
    market_regime = research_report.get("market_regime", {})
    macro_outlook = research_report.get("macro_outlook", {})
    cross_signal_insights = research_report.get("cross_signal_insights", [])

    # Truncate market_regime evidence if present
    if isinstance(market_regime, dict) and "evidence" in market_regime:
        evidence = market_regime["evidence"]
        if isinstance(evidence, str) and len(evidence) > 300:
            market_regime["evidence"] = evidence[:297] + "..."

    # Truncate macro_outlook summary if present
    if isinstance(macro_outlook, dict) and "summary" in macro_outlook:
        summary = macro_outlook["summary"]
        if isinstance(summary, str) and len(summary) > 200:
            macro_outlook["summary"] = summary[:197] + "..."

    # Limit key_events list to 5 items, truncate description strings
    if isinstance(macro_outlook, dict) and "key_events" in macro_outlook:
        events = macro_outlook["key_events"]
        if isinstance(events, list):
            events = events[:5]
            for event in events:
                if isinstance(event, dict) and "description" in event:
                    desc = event["description"]
                    if isinstance(desc, str) and len(desc) > 80:
                        event["description"] = desc[:77] + "..."
            macro_outlook["key_events"] = events

    # Limit cross_signal_insights list to 5 items, truncate long strings
    if isinstance(cross_signal_insights, list):
        cross_signal_insights = cross_signal_insights[:5]
        truncated = []
        for item in cross_signal_insights:
            if isinstance(item, str) and len(item) > 200:
                truncated.append(item[:197] + "...")
            else:
                truncated.append(item)
        cross_signal_insights = truncated

    return {
        "market_regime": market_regime,
        "macro_outlook": macro_outlook,
        "cross_signal_insights": cross_signal_insights,
    }


def _truncate_opponent_draft(draft: dict) -> dict:
    """Truncate opponent draft to essential fields for cross-examination."""
    if not isinstance(draft, dict):
        return draft
    truncated = draft.copy()
    # Limit core_arguments to 5 items
    if "core_arguments" in truncated:
        args = truncated["core_arguments"]
        if isinstance(args, list):
            truncated["core_arguments"] = args[:5]
    # Limit target_tickers to 5 items, truncate reason strings
    if "target_tickers" in truncated:
        tickers = truncated["target_tickers"]
        if isinstance(tickers, list):
            tickers = tickers[:5]
            for ticker in tickers:
                if isinstance(ticker, dict) and "reason" in ticker:
                    reason = ticker["reason"]
                    if isinstance(reason, str) and len(reason) > 200:
                        ticker["reason"] = reason[:197] + "..."
            truncated["target_tickers"] = tickers
    # Limit risk_acknowledgments to 5 items
    if "risk_acknowledgments" in truncated:
        risks = truncated["risk_acknowledgments"]
        if isinstance(risks, list):
            truncated["risk_acknowledgments"] = risks[:5]
    # Truncate long strings in core_arguments and risk_acknowledgments
    for field in ["core_arguments", "risk_acknowledgments"]:
        if field in truncated and isinstance(truncated[field], list):
            truncated[field] = [
                item[:197] + "..." if isinstance(item, str) and len(item) > 200 else item
                for item in truncated[field]
            ]
    return truncated


async def _run_cross_exam(
    *,
    role: str,
    system: str,
    opponent_draft: dict,
    research_report: dict,
) -> dict:
    key_fields = _extract_key_research_fields(research_report)
    opponent_truncated = _truncate_opponent_draft(opponent_draft)
    user = (
        "## Opponent thesis (full JSON)\n"
        f"{json.dumps(opponent_truncated, ensure_ascii=False, indent=2)}\n\n"
        "## Stage 3 research_report (key fields for fact-checking)\n"
        f"{json.dumps(key_fields, ensure_ascii=False, indent=2)}\n\n"
        "Output JSON: "
        '{"rebuttal_statement": "<2-4 sentences>", "rebuttal_points": ["<optional bullets>"]}'
    )
    client = _get_client()
    model = settings.openai_model_heavy
    last_error: str | None = None
    for attempt in range(2):
        t0 = time.time()
        try:
            messages = [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ]
            if attempt > 0 and last_error:
                messages[1]["content"] = (
                    f"[RETRY {attempt}] Previous error: {last_error}\n\n" + user
                )
            resp = await client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.15,
                max_tokens=MAX_CROSS_EXAM_TOKENS,
                response_format={"type": "json_object"},
            )
            raw = resp.choices[0].message.content or "{}"
            elapsed = round(time.time() - t0, 2)
            logger.info(
                f"[CROSS_EXAM {role}] done in {elapsed}s | "
                f"tokens out={resp.usage.completion_tokens if resp.usage else 0}"
            )
            parsed = json.loads(raw)
            return _normalize_cross_exam(parsed)
        except Exception as e:
            last_error = str(e)
            logger.warning(f"[CROSS_EXAM {role}] attempt {attempt} failed: {e}")

    logger.error(f"[CROSS_EXAM {role}] failed: {last_error}")
    return {
        "rebuttal_statement": "",
        "rebuttal_points":    [],
        "failed":             True,
    }


def _normalize_cross_exam(parsed: dict) -> dict:
    stmt = str(parsed.get("rebuttal_statement", "") or "").strip()
    pts = parsed.get("rebuttal_points") or []
    if not isinstance(pts, list):
        pts = []
    pts = [str(p).strip() for p in pts if str(p).strip()][:5]
    if not stmt and pts:
        stmt = " ".join(pts[:3])
    return {
        "rebuttal_statement": stmt[:1200],
        "rebuttal_points":    pts,
        "failed":             False,
    }
