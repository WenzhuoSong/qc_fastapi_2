"""OpenAI Responses API over plain HTTP (the pinned SDK predates it; `event_cause.web_search` does the same).

The gpt-6 models only call function tools with reasoning through /v1/responses. `create` returns the
response JSON or raises `ResponsesError` (named like the SDK errors, so `telegram.friendly_error` maps them).
"""
from __future__ import annotations

import asyncio
import json

import httpx

from scout.config import get_scout_settings

URL = "https://api.openai.com/v1/responses"
RETRIES = 2


class ResponsesError(Exception):
    def __init__(self, kind: str, message: str):
        super().__init__(message)
        self.kind = kind


class AuthenticationError(ResponsesError):
    pass


class RateLimitError(ResponsesError):
    pass


class APITimeoutError(ResponsesError):
    pass


class APIConnectionError(ResponsesError):
    pass


def _raise(status: int, body: dict | str) -> None:
    msg = (body.get("error") or {}).get("message") if isinstance(body, dict) else str(body)
    if status == 401:
        raise AuthenticationError("auth", msg or "unauthorised")
    if status == 429:
        raise RateLimitError("rate", msg or "rate limited")
    raise ResponsesError("http", f"HTTP {status}: {(msg or '')[:300]}")


async def create(body: dict, timeout: float | None = None) -> dict:
    """POST one request; never logs the key or the request body."""
    settings = get_scout_settings()
    for attempt in range(RETRIES + 1):                # dropped connections are retried; timeouts and HTTP errors are not
        try:
            async with httpx.AsyncClient(timeout=timeout or settings.scout_llm_timeout_s) as client:
                resp = await client.post(URL, headers={"Authorization": f"Bearer {settings.openai_api_key}"}, json=body)
            break
        except httpx.TimeoutException as exc:
            raise APITimeoutError("timeout", str(exc)) from exc
        except httpx.TransportError as exc:
            if attempt == RETRIES:
                raise APIConnectionError("connection", f"{type(exc).__name__}: {exc}") from exc
            await asyncio.sleep(2 * (attempt + 1))
    try:
        data = resp.json()
    except json.JSONDecodeError:
        data = resp.text
    if resp.status_code != 200:
        _raise(resp.status_code, data)
    if isinstance(data, dict) and data.get("error"):
        raise ResponsesError("api", str(data["error"])[:300])
    return data


def output_text(data: dict) -> str:
    return "".join(part.get("text", "") for item in data.get("output") or [] if item.get("type") == "message"
                   for part in item.get("content") or [] if part.get("type") in ("output_text", "text"))


def function_calls(data: dict) -> list[dict]:
    return [item for item in data.get("output") or [] if item.get("type") == "function_call"]


def as_tool(fn: dict) -> dict:
    """A Chat Completions style function definition as a Responses tool."""
    return {"type": "function", "name": fn["name"], "description": fn["description"], "parameters": fn["parameters"]}
