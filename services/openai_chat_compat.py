"""Compatibility helpers for OpenAI Chat Completions model families."""
from __future__ import annotations

from typing import Any


def is_gpt5_family(model: str | None) -> bool:
    """Return True for GPT-5 family model ids and snapshots."""
    return str(model or "").strip().lower().startswith("gpt-5")


def build_chat_completion_kwargs(
    *,
    model: str,
    messages: list[dict[str, Any]],
    temperature: float | None = None,
    max_tokens: int | None = None,
    response_format: Any | None = None,
    tools: Any | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Build Chat Completions kwargs with GPT-5 compatible token parameters.

    GPT-5 family models use `max_completion_tokens`. They also work best with
    default sampling in our deterministic JSON paths, so we omit temperature for
    those models instead of sending non-default values that may be rejected by
    some reasoning snapshots.
    """
    kwargs: dict[str, Any] = {
        "model": model,
        "messages": messages,
    }
    gpt5 = is_gpt5_family(model)

    if max_tokens is not None:
        if gpt5:
            kwargs["max_completion_tokens"] = max_tokens
        else:
            kwargs["max_tokens"] = max_tokens

    if temperature is not None and not gpt5:
        kwargs["temperature"] = temperature

    if response_format is not None:
        kwargs["response_format"] = response_format

    if tools is not None:
        kwargs["tools"] = tools

    kwargs.update({key: value for key, value in extra.items() if value is not None})
    return kwargs
