# agents/bull_researcher.py
"""
Stage 4a: Bull Researcher —— 多方论证 Agent

职责：基于 RESEARCHER 的 research_report，站**多方**立场构建最强看多论据。
与 Stage 4b Bear Researcher 通过 asyncio.gather 并行执行。

输入：research_report + base_weights
输出：bull_output（thesis, arguments, ticker_views, suggested_weights, confidence）

约束：
  - 只能论证 maintain 或 increase
  - 必须引用 research_report 中的具体 ticker_signals 和 combined_signal
  - 不得无视 Bear 方向的风险 flag，但需要给出为什么风险可控的理由

LLM: settings.openai_model_heavy (gpt-4o)
"""
from __future__ import annotations

import json
import logging
import time

from openai import AsyncOpenAI

from config import get_settings

logger = logging.getLogger("qc_fastapi_2.bull_researcher")
settings = get_settings()

_client: AsyncOpenAI | None = None


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=settings.openai_api_key)
    return _client


SYSTEM_PROMPT = """你是量化交易系统的 Bull Analyst（多方分析师）。

【你的立场】
    你的职责是为当前持仓策略构建最强的看多论据。你必须全力论证看多。

【输入材料】
    你会收到：
    1. research_report —— 包含 market_regime, macro_outlook, ticker_signals, cross_signal_insights
    2. base_weights —— Stage 2 Python 量化基准仓位

【你必须做到】
    1. 从 research_report 中找到支撑看多的量化证据（引用具体 ticker_signals 数值）
    2. 强调增长潜力、竞争优势、正面宏观条件
    3. 对每个看多论据评估置信度
    4. 说明哪些板块/标的应该加仓，为什么
    5. 如果某个 ticker 有 flag（风险标记），你需要解释为什么这个风险可控

【约束】
    · 只能建议 maintain（维持）或 increase（加仓）
    · suggested_weights 中所有值 ≥ 0，总和 = 1.0，必须包含 CASH
    · 权重调整基于 base_weights，单仓不超过 0.20
    · 你可以对 CASH 减仓（减少现金，加仓权益）

【必须输出纯 JSON】
{
  "stance": "maintain|increase",
  "confidence": <float 0.0-1.0>,
  "arguments": [
    "<看多论据 1，引用具体数据>",
    "<看多论据 2>"
  ],
  "ticker_views": [
    {
      "ticker": "<TICKER>",
      "action": "overweight|hold",
      "delta": <float>,
      "reason": "<≤40 字理由>"
    }
  ],
  "suggested_weights": {"<TICKER>": <float>, "CASH": <float>},
  "risk_acknowledgments": [
    "<承认的风险 1，但解释为什么可控>"
  ]
}

仅输出 JSON。"""


async def run_bull_researcher_async(
    research_report: dict,
    base_weights: dict,
) -> dict:
    """Stage 4a: 多方论证。并行调用，不等 Bear。"""
    user_payload = _build_user_message(research_report, base_weights)

    client = _get_client()
    model  = settings.openai_model_heavy

    last_error: str | None = None
    for attempt in range(2):
        t0 = time.time()
        try:
            messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_payload},
            ]
            if attempt > 0 and last_error:
                messages[1]["content"] = (
                    f"[RETRY {attempt}] 上次输出错误: {last_error}\n\n" + user_payload
                )

            resp = await client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.2,
                max_tokens=1500,
                response_format={"type": "json_object"},
            )
            raw = resp.choices[0].message.content or ""
            elapsed = round(time.time() - t0, 2)
            logger.info(
                f"[BULL] done in {elapsed}s | "
                f"input_tokens={resp.usage.prompt_tokens} "
                f"output_tokens={resp.usage.completion_tokens}"
            )

            parsed = json.loads(raw)
            return _normalize(parsed, base_weights)

        except Exception as e:
            last_error = str(e)
            logger.warning(f"[BULL] attempt {attempt} failed: {e}")

    logger.error(f"[BULL] all retries failed. last_error={last_error}")
    return _degraded_output(base_weights, last_error)


def _build_user_message(research_report: dict, base_weights: dict) -> str:
    return (
        "## Research Report\n"
        f"{json.dumps(research_report, ensure_ascii=False, indent=2)}\n\n"
        "## Base Weights (Stage 2 基准)\n"
        f"{json.dumps(base_weights, ensure_ascii=False, indent=2)}\n\n"
        "## 你的任务\n"
        "站多方立场，基于以上材料构建最强看多论据。"
        "输出 stance + confidence + arguments + ticker_views + suggested_weights。"
        "仅返回纯 JSON。"
    )


def _normalize(out: dict, base_weights: dict) -> dict:
    stance = str(out.get("stance", "maintain")).strip()
    if stance not in ("maintain", "increase"):
        stance = "maintain"

    try:
        confidence = max(0.0, min(1.0, float(out.get("confidence", 0.5))))
    except (TypeError, ValueError):
        confidence = 0.5

    arguments = out.get("arguments") or []
    if not isinstance(arguments, list):
        arguments = []
    arguments = [str(a).strip() for a in arguments if str(a).strip()][:5]

    ticker_views = out.get("ticker_views") or []
    if not isinstance(ticker_views, list):
        ticker_views = []
    cleaned_views = []
    for v in ticker_views:
        if not isinstance(v, dict) or not v.get("ticker"):
            continue
        action = str(v.get("action", "hold")).strip()
        if action not in ("overweight", "hold"):
            action = "hold"
        cleaned_views.append({
            "ticker": str(v["ticker"]).upper().strip(),
            "action": action,
            "delta":  _safe_float(v.get("delta"), 0.0),
            "reason": str(v.get("reason", ""))[:80],
        })

    suggested = out.get("suggested_weights") or {}
    if not isinstance(suggested, dict) or not suggested:
        suggested = dict(base_weights)

    risk_acks = out.get("risk_acknowledgments") or []
    if not isinstance(risk_acks, list):
        risk_acks = []
    risk_acks = [str(r).strip() for r in risk_acks if str(r).strip()][:3]

    return {
        "stance":                stance,
        "confidence":            confidence,
        "arguments":             arguments,
        "ticker_views":          cleaned_views,
        "suggested_weights":     suggested,
        "risk_acknowledgments":  risk_acks,
        "failed":                False,
    }


def _degraded_output(base_weights: dict, error: str | None) -> dict:
    return {
        "stance":                "maintain",
        "confidence":            0.3,
        "arguments":             [f"Bull LLM 降级 (error={error})"],
        "ticker_views":          [],
        "suggested_weights":     dict(base_weights),
        "risk_acknowledgments":  [],
        "failed":                True,
    }


def _safe_float(val, default: float) -> float:
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default
