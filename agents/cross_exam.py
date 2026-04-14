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

Rules:
- Do NOT output portfolio weights or percentages.
- 2–4 sentences OR up to 5 bullet strings in rebuttal_points.
- JSON only."""

BEAR_CROSS_EXAM_PROMPT = """You are the Bear analyst. The Bull analyst has published a thesis (JSON below).

Your task: cross-examine the Bull's case. Attack complacency, stretched valuations, ignored flags,
or conflicts with the Stage 3 research_report data. Be precise and evidence-based.

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


async def _run_cross_exam(
    *,
    role: str,
    system: str,
    opponent_draft: dict,
    research_report: dict,
) -> dict:
    user = (
        "## Opponent thesis (full JSON)\n"
        f"{json.dumps(opponent_draft, ensure_ascii=False, indent=2)}\n\n"
        "## Stage 3 research_report (for fact-checking)\n"
        f"{json.dumps(research_report, ensure_ascii=False, indent=2)[:12000]}\n\n"
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
