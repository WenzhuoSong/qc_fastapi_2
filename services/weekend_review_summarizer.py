"""Weekend review LLM summarizer contract.

The weekend LLM may explain deterministic metrics, but it may not compute
metrics, change policy, recommend executable trades, or mutate targets. This
module keeps that boundary explicit and dependency-light: PR3 builds prompts
and sanitizes/parses text. PR4 can wire the actual LLM transport.
"""
from __future__ import annotations

import inspect
import json
import re
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from services.json_safety import json_safe
from services.weekend_review_artifacts import serialize_weekly_review_artifact
from services.weekend_review_loader import EXECUTION_AUTHORITY, TARGET_WEIGHT_MUTATION


SUMMARY_CONTRACT_VERSION = "weekend_review_llm_summary_v1"
PROMPT_CONTRACT_VERSION = "weekend_review_summary_prompt_v1"

FORBIDDEN_INPUT_KEYS = {
    "target_weights",
    "target_builder_input",
    "final_target",
    "current_weights",
    "command_payload",
    "qc_response",
    "section_payload",
    "policy_change",
    "config_change",
}

FORBIDDEN_OUTPUT_PATTERNS = {
    "buy_order": re.compile(r"\b(buy|purchase)\b.+\b[A-Z]{2,5}\b", re.I),
    "sell_order": re.compile(r"\b(sell|short|liquidate)\b.+\b[A-Z]{2,5}\b", re.I),
    "set_weights": re.compile(r"\b(set|change|adjust)\b.+\b(weight|weights|target)\b", re.I),
    "submit_command": re.compile(r"\b(submit|send|place)\b.+\b(command|order|trade)\b", re.I),
    "policy_change": re.compile(r"\b(change|update|raise|lower)\b.+\b(policy|threshold|limit|cap)\b", re.I),
    "strategy_promotion": re.compile(r"\b(promote|demote|enable|disable)\b.+\b(strategy|alpha|model)\b", re.I),
    "execution_instruction": re.compile(r"\b(execute|rebalance|trade now|send to qc)\b", re.I),
}

REQUIRED_FOOTER = (
    "This report has execution_authority=none and target_weight_mutation=none. "
    "All quantitative conclusions are computed by deterministic metrics. "
    "LLM text is explanatory only."
)

LLMComplete = Callable[[str], str | Awaitable[str]]


def build_weekend_review_prompt(
    artifacts: list[Any],
    *,
    max_chars: int = 18000,
) -> dict[str, Any]:
    """Build a prompt payload using metrics/rates/evidence only."""
    sanitized = [_artifact_for_prompt(item) for item in artifacts]
    payload = {
        "prompt_contract_version": PROMPT_CONTRACT_VERSION,
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "role": "weekend_review_researcher",
        "instructions": [
            "Explain deterministic metrics only.",
            "Do not compute or override metrics.",
            "Do not give trading instructions.",
            "Do not suggest policy/config/threshold changes as actions.",
            "Use 'insufficient sample' when a rate status says insufficient_sample.",
            "Every recommendation must be phrased as operator review only.",
        ],
        "forbidden_outputs": sorted(FORBIDDEN_OUTPUT_PATTERNS),
        "artifacts": sanitized,
    }
    text = (
        "You are writing an off-hours trading-system review. "
        "Use only the JSON metrics below. Produce concise narrative sections: "
        "execution truth, intent blockers, label maturity, hedge review, debate value, "
        "basket structure, regime/risk, and review-only follow-ups. "
        "Never include executable target weights, trade instructions, config changes, "
        "threshold changes, strategy promotion/demotion, or QC commands.\n\n"
        f"{json.dumps(json_safe(payload), ensure_ascii=False, sort_keys=True)}\n\n"
        f"Required footer: {REQUIRED_FOOTER}"
    )
    if len(text) > max_chars:
        text = text[: max_chars - 80] + "\n...[truncated metrics payload for token budget]\n"
    return {
        "contract_version": PROMPT_CONTRACT_VERSION,
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "prompt": text,
        "artifact_count": len(sanitized),
        "included_schema_versions": sorted({str(item.get("schema_version")) for item in sanitized}),
        "excluded_input_keys": sorted(FORBIDDEN_INPUT_KEYS),
    }


async def summarize_weekend_review(
    artifacts: list[Any],
    *,
    llm_complete: LLMComplete,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    """Call an injected LLM completion function and return sanitized summary metadata."""
    prompt = build_weekend_review_prompt(artifacts)
    raw = llm_complete(prompt["prompt"])
    if inspect.isawaitable(raw):
        raw = await raw
    return build_weekend_review_summary_report(
        raw_summary=str(raw or ""),
        prompt_payload=prompt,
        created_at=created_at,
    )


def build_weekend_review_summary_report(
    *,
    raw_summary: str,
    prompt_payload: dict[str, Any],
    created_at: datetime | None = None,
) -> dict[str, Any]:
    """Parse/sanitize an LLM summary as text and report metadata only."""
    sanitized = sanitize_weekend_review_summary(raw_summary)
    created = created_at or datetime.now(timezone.utc)
    text = sanitized["summary_text"].strip()
    if REQUIRED_FOOTER not in text:
        text = f"{text}\n\n{REQUIRED_FOOTER}".strip()
    return json_safe({
        "schema_version": SUMMARY_CONTRACT_VERSION,
        "created_at": created.isoformat(),
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "prompt_contract_version": prompt_payload.get("contract_version"),
        "artifact_count": prompt_payload.get("artifact_count", 0),
        "included_schema_versions": prompt_payload.get("included_schema_versions", []),
        "summary_text": text,
        "removed_forbidden_line_count": len(sanitized["removed_lines"]),
        "removed_forbidden_actions": sanitized["removed_actions"],
        "llm_summary_is_explanatory_only": True,
    })


def sanitize_weekend_review_summary(text: str) -> dict[str, Any]:
    """Remove lines that look like execution/config instructions."""
    removed_lines: list[str] = []
    removed_actions: list[str] = []
    kept: list[str] = []
    for line in str(text or "").splitlines():
        matches = _forbidden_matches(line)
        if matches:
            removed_lines.append(line)
            removed_actions.extend(matches)
            continue
        kept.append(line)
    return {
        "summary_text": "\n".join(kept).strip(),
        "removed_lines": removed_lines,
        "removed_actions": sorted(set(removed_actions)),
    }


def _artifact_for_prompt(artifact: Any) -> dict[str, Any]:
    payload = serialize_weekly_review_artifact(artifact)
    slim = {
        "schema_version": payload.get("schema_version"),
        "artifact_type": payload.get("artifact_type"),
        "artifact_id": payload.get("artifact_id"),
        "week_start": payload.get("week_start"),
        "week_end": payload.get("week_end"),
        "execution_authority": payload.get("execution_authority"),
        "target_weight_mutation": payload.get("target_weight_mutation"),
        "metrics": payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {},
        "rates": payload.get("rates") if isinstance(payload.get("rates"), dict) else {},
        "evidence_refs": payload.get("evidence_refs") if isinstance(payload.get("evidence_refs"), list) else [],
        "source_counts": payload.get("source_counts") if isinstance(payload.get("source_counts"), dict) else {},
        "exclusion_counts": payload.get("exclusion_counts") if isinstance(payload.get("exclusion_counts"), dict) else {},
        "excluded_input_count": payload.get("excluded_input_count", 0),
    }
    return _strip_forbidden_keys(slim)


def _strip_forbidden_keys(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_forbidden_keys(item)
            for key, item in value.items()
            if key not in FORBIDDEN_INPUT_KEYS
        }
    if isinstance(value, list):
        return [_strip_forbidden_keys(item) for item in value]
    return value


def _forbidden_matches(line: str) -> list[str]:
    clean = str(line or "")
    return [
        name
        for name, pattern in FORBIDDEN_OUTPUT_PATTERNS.items()
        if pattern.search(clean)
    ]
