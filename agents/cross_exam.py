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

CROSS_EXAM_PROMPT_TEMPLATE = """你是{{side}}方，正在审阅对方（{{opponent_side}}）的论点并提出反驳。

## 对方的完整立场（结构化 JSON）
{opponent_output_json}

## 你的任务
针对对方 ticker_views 中 confidence=high 的持仓，提出具体反驳。

输出格式（JSON）：
{{
  "rebuttals": [
    {{
      "ticker": "TICKER",
      "opponent_claim": "对方的 primary_reason（原文）",
      "rebuttal": "你的反驳（50字以内）",
      "rebuttal_strength": "strong" | "moderate" | "weak"
    }}
  ],
  "concessions": [
    {{
      "ticker": "TICKER",
      "conceded_point": "你认为对方说得有道理的点"
    }}
  ],
  "unaddressed_risks": ["对方完全没有提到但你认为重要的风险1", "..."]
}}

只针对对方 confidence=high 的 ticker 反驳，最多5条 rebuttals。
JSON only。"""

BULL_CROSS_EXAM_PROMPT = """You are the Bull analyst. The Bear analyst has published a structured thesis (JSON below).

Your task: cross-examine the Bear's case. Attack logical gaps, overstated risks, or inconsistencies
with the Stage 3 research_report data. Be precise and evidence-based — not rhetorical fluff.

You receive the opponent's structured output with ticker_views. Target rebuttals at Bear's high-confidence tickers.

Rules:
- Do NOT output portfolio weights or percentages.
- Focus on Bear's primary_reason for each high-confidence ticker.
- Provide rebuttal_strength (strong/moderate/weak) for each rebuttal.
- JSON only."""

BEAR_CROSS_EXAM_PROMPT = """You are the Bear analyst. The Bull analyst has published a structured thesis (JSON below).

Your task: cross-examine the Bull's case. Attack complacency, stretched valuations, ignored flags,
or conflicts with the Stage 3 research_report data. Be precise and evidence-based.

You receive the opponent's structured output with ticker_views. Target rebuttals at Bull's high-confidence tickers.

Rules:
- Do NOT output portfolio weights or percentages.
- Focus on Bull's primary_reason for each high-confidence ticker.
- Provide rebuttal_strength (strong/moderate/weak) for each rebuttal.
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
        side="多头",
        opponent_side="空头",
        system=BULL_CROSS_EXAM_PROMPT,
        opponent_draft=bear_draft,
        research_report=research_report,
    )


async def run_bear_cross_exam_async(bull_draft: dict, research_report: dict) -> dict:
    """Bear reads Bull's draft + research_report; returns rebuttal attacking Bull."""
    return await _run_cross_exam(
        role="BEAR",
        side="空头",
        opponent_side="多头",
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

    # For new schema: focus on ticker_views (key fields for cross-exam)
    if "ticker_views" in truncated:
        views = truncated["ticker_views"]
        if isinstance(views, dict):
            # Keep only high-confidence tickers for brevity
            filtered = {}
            for ticker, view in views.items():
                if isinstance(view, dict) and view.get("confidence") == "high":
                    filtered[ticker] = {
                        "direction": view.get("direction", "hold"),
                        "magnitude": view.get("magnitude", "moderate"),
                        "confidence": view.get("confidence", "medium"),
                        "primary_reason": str(view.get("primary_reason", ""))[:60],
                        "key_risk": str(view.get("key_risk", ""))[:60],
                    }
            truncated["ticker_views"] = filtered

    # Legacy support: also truncate old-style fields if present
    for field in ["core_arguments", "risk_acknowledgments", "opportunity_acknowledgments"]:
        if field in truncated and isinstance(truncated[field], list):
            truncated[field] = [
                item[:197] + "..." if isinstance(item, str) and len(item) > 200 else item
                for item in truncated[field][:5]
            ]

    for field in ["target_tickers"]:
        if field in truncated and isinstance(truncated[field], list):
            tickers = truncated[field][:5]
            for ticker in tickers:
                if isinstance(ticker, dict) and "reason" in ticker:
                    ticker["reason"] = str(ticker["reason"])[:197] + "..."
            truncated[field] = tickers

    return truncated


async def _run_cross_exam(
    *,
    role: str,
    side: str,
    opponent_side: str,
    system: str,
    opponent_draft: dict,
    research_report: dict,
) -> dict:
    key_fields = _extract_key_research_fields(research_report)
    opponent_truncated = _truncate_opponent_draft(opponent_draft)
    opponent_json = json.dumps(opponent_truncated, ensure_ascii=False, indent=2)

    user = (
        f"## 对方（{opponent_side}）完整立场\n"
        f"{opponent_json}\n\n"
        "## Stage 3 research_report（用于事实核查）\n"
        f"{json.dumps(key_fields, ensure_ascii=False, indent=2)}\n\n"
        "## 输出格式\n"
        '{"rebuttals": [{"ticker": "...", "opponent_claim": "...", "rebuttal": "...", "rebuttal_strength": "strong|moderate|weak"}], '
        '"concessions": [{"ticker": "...", "conceded_point": "..."}], '
        '"unaddressed_risks": ["..."]}'
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
        "rebuttals": [],
        "concessions": [],
        "unaddressed_risks": [],
        "failed": True,
    }


def _normalize_cross_exam(parsed: dict) -> dict:
    rebuttals = parsed.get("rebuttals") or []
    if not isinstance(rebuttals, list):
        rebuttals = []
    cleaned_rebuttals = []
    for r in rebuttals[:5]:
        if isinstance(r, dict) and r.get("ticker"):
            strength = str(r.get("rebuttal_strength", "moderate")).strip().lower()
            if strength not in ("strong", "moderate", "weak"):
                strength = "moderate"
            cleaned_rebuttals.append({
                "ticker": str(r["ticker"]).upper().strip(),
                "opponent_claim": str(r.get("opponent_claim", ""))[:120],
                "rebuttal": str(r.get("rebuttal", ""))[:100],
                "rebuttal_strength": strength,
            })

    concessions = parsed.get("concessions") or []
    if not isinstance(concessions, list):
        concessions = []
    cleaned_concessions = []
    for c in concessions:
        if isinstance(c, dict) and c.get("ticker"):
            cleaned_concessions.append({
                "ticker": str(c["ticker"]).upper().strip(),
                "conceded_point": str(c.get("conceded_point", ""))[:120],
            })

    unaddressed_risks = parsed.get("unaddressed_risks") or []
    if not isinstance(unaddressed_risks, list):
        unaddressed_risks = []
    unaddressed_risks = [str(r).strip() for r in unaddressed_risks if str(r).strip()][:5]

    return {
        "rebuttals": cleaned_rebuttals,
        "concessions": cleaned_concessions,
        "unaddressed_risks": unaddressed_risks,
        "failed": False,
    }
