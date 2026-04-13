# agents/synthesizer.py
"""
Stage 5: Synthesizer —— 首席投资官仲裁层（V2.1 新增）

职责：
    权衡 Bull/Bear 双方论点，结合 base_weights，产出最终 adjusted_weights。
    输出接口与 V2 Phase 1 的 researcher_out **完全兼容**，下游 Risk MGR 无需改动。

输入：bull_output, bear_output, research_report, base_weights, risk_params
输出：synthesizer_out（兼容 researcher_out 接口 + 额外 debate_summary 字段）

核心规则：
    - 客观评估双方论据的强度和证据质量
    - 标记双方共识点和分歧点
    - 若双方置信度差距 < 0.15，标记 uncertainty_flag=true，选择保守方案
    - 权重调整幅度 ±5%，有充分理由可 ±10%

LLM: settings.openai_model_heavy (gpt-4o)
"""
from __future__ import annotations

import json
import logging
import time

from openai import AsyncOpenAI

from config import get_settings

logger = logging.getLogger("qc_fastapi_2.synthesizer")
settings = get_settings()

_client: AsyncOpenAI | None = None


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=settings.openai_api_key)
    return _client


SYSTEM_PROMPT = """你是量化交易系统的首席投资官（CIO / Synthesizer），刚刚听完 Bull 和 Bear 两位分析师的辩论。

【你的位置】
    上游 Stage 3 是 RESEARCHER（信息合成报告）。
    Stage 4a Bull 和 Stage 4b Bear 已基于该报告各自论证。
    你需要仲裁，产出最终的 adjusted_weights 供下游 Risk Manager 审查。

【你的任务】
    1. 客观评估双方论据的强度和证据质量
    2. 标记双方共识点和分歧点
    3. 做出最终市场判断（regime + stance）
    4. 基于判断调整 base_weights（调整幅度 ±5%，有充分理由可 ±10%）
    5. 若双方置信度差距 < 0.15，标记 uncertainty_flag=true，选择保守方案

【重要规则】
    · 不要偏向任何一方，只看证据质量
    · 权重调整之和必须 = 1.0
    · 单仓不超过 max_single_position
    · 必须包含 CASH
    · 不要重写 base_weights —— 大多数标的和 base_weights 在 ±5% 以内

【regime 取值（严格 6 选 1）】
    bull_trend / bull_weak / neutral / bear_weak / bear_trend / high_vol

【recommended_stance 取值（严格 5 选 1）】
    buy / overweight / maintain / underweight / sell

【key_events 规则】
    · 从 research_report.macro_outlook.key_events 中继承
    · 必须使用 transmission 匹配器能识别的关键字

【必须输出纯 JSON】
{
  "market_judgment": {
    "regime": "bull_trend|bull_weak|neutral|bear_weak|bear_trend|high_vol",
    "adjusted_confidence": <float 0.0-1.0>,
    "uncertainty_flag": <bool>
  },
  "recommended_stance": "buy|overweight|maintain|underweight|sell",
  "adjusted_weights": {"<TICKER>": <float>, "CASH": <float>},
  "weight_adjustments": [
    {
      "ticker": "<TICKER>",
      "base": <float>,
      "adjusted": <float>,
      "delta": <float>,
      "reason": "<≤40 字中文理由>"
    }
  ],
  "reasoning": "<≤200 字中文总理由>",
  "consensus_points": ["...", "..."],
  "divergence_points": ["...", "..."],
  "key_events": ["<event phrase 1>", "..."],
  "debate_resolution": "<一句话说明仲裁逻辑>"
}

仅输出 JSON。"""

# ═══════════════════════════════════════════════════════════════
# 5 级 stance → 旧 4 级兼容映射（供 defensive_adjust 等下游使用）
# ═══════════════════════════════════════════════════════════════

_VALID_REGIMES = {"bull_trend", "bull_weak", "neutral", "bear_weak", "bear_trend", "high_vol"}
_VALID_STANCES_5 = {"buy", "overweight", "maintain", "underweight", "sell"}


# ═══════════════════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════════════════


async def run_synthesizer_async(
    research_report: dict,
    bull_output: dict,
    bear_output: dict,
    base_weights: dict,
    brief: dict,
    risk_params: dict,
) -> dict:
    """
    Stage 5: 仲裁 Bull/Bear，产出 adjusted_weights。
    输出接口兼容旧 researcher_out，下游 Risk MGR 无需改动。
    """
    max_single_position = float(risk_params.get("max_single_position", 0.20))

    allowed_tickers = _collect_allowed_tickers(brief, base_weights)

    user_payload = _build_user_message(
        research_report, bull_output, bear_output, base_weights, risk_params
    )

    client = _get_client()
    model  = settings.openai_model_heavy

    last_error: str | None = None
    for attempt in range(3):
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
                temperature=0.0,
                max_tokens=2000,
                response_format={"type": "json_object"},
            )
            raw = resp.choices[0].message.content or ""
            elapsed = round(time.time() - t0, 2)
            logger.info(
                f"[SYNTHESIZER] done in {elapsed}s | "
                f"input_tokens={resp.usage.prompt_tokens} "
                f"output_tokens={resp.usage.completion_tokens}"
            )

            parsed = json.loads(raw)
            _validate(parsed)
            return _normalize(
                parsed,
                base_weights=base_weights,
                allowed_tickers=allowed_tickers,
                max_single_position=max_single_position,
                bull_output=bull_output,
                bear_output=bear_output,
                research_report=research_report,
            )

        except Exception as e:
            last_error = str(e)
            logger.warning(f"[SYNTHESIZER] attempt {attempt} failed: {e}")

    # 所有重试失败 → 降级到 base_weights
    logger.error(
        f"[SYNTHESIZER] all retries failed, degrading to base_weights. last_error={last_error}"
    )
    return _degraded_output(
        base_weights, bull_output, bear_output, research_report, last_error
    )


# ═══════════════════════════════════════════════════════════════
# User message builder
# ═══════════════════════════════════════════════════════════════


def _build_user_message(
    research_report: dict,
    bull_output: dict,
    bear_output: dict,
    base_weights: dict,
    risk_params: dict,
) -> str:
    max_pos = float(risk_params.get("max_single_position", 0.20))
    min_cash = float(risk_params.get("min_cash_pct", 0.05))

    # 精简 research_report（不需要完整 ticker_signals）
    regime = research_report.get("market_regime", {})
    macro = research_report.get("macro_outlook", {})
    insights = research_report.get("cross_signal_insights", [])

    return (
        "## Research Report 摘要\n"
        f"market_regime: {json.dumps(regime, ensure_ascii=False)}\n"
        f"macro_outlook: {json.dumps(macro, ensure_ascii=False)}\n"
        f"cross_signal_insights: {json.dumps(insights, ensure_ascii=False)}\n\n"
        "## Bull Analyst 论点\n"
        f"{json.dumps(bull_output, ensure_ascii=False, indent=2)}\n\n"
        "## Bear Analyst 论点\n"
        f"{json.dumps(bear_output, ensure_ascii=False, indent=2)}\n\n"
        "## Base Weights (Stage 2 基准)\n"
        f"{json.dumps(base_weights, ensure_ascii=False, indent=2)}\n\n"
        "## 约束\n"
        f"max_single_position = {max_pos}\n"
        f"min_cash_pct = {min_cash}\n\n"
        "## 你的任务\n"
        "仲裁 Bull/Bear 双方论点，输出 market_judgment + recommended_stance + "
        "adjusted_weights + weight_adjustments + reasoning + key_events + debate_resolution。"
        "仅返回纯 JSON。"
    )


def _collect_allowed_tickers(brief: dict, base_weights: dict) -> set[str]:
    """adjusted_weights 只允许 ticker 来自 brief.holdings 或 base_weights（含 CASH）。"""
    tickers: set[str] = {"CASH"}
    for h in brief.get("holdings") or []:
        t = (h.get("ticker") or "").upper().strip()
        if t:
            tickers.add(t)
    for t in base_weights.keys():
        t = str(t).upper().strip()
        if t:
            tickers.add(t)
    return tickers


# ═══════════════════════════════════════════════════════════════
# Validation + normalization
# ═══════════════════════════════════════════════════════════════


def _validate(out: dict) -> None:
    required = [
        "market_judgment",
        "recommended_stance",
        "adjusted_weights",
        "reasoning",
    ]
    missing = [k for k in required if k not in out]
    if missing:
        raise ValueError(f"missing fields: {missing}")

    mj = out.get("market_judgment") or {}
    if "regime" not in mj:
        raise ValueError("market_judgment.regime missing")

    weights = out.get("adjusted_weights")
    if not isinstance(weights, dict) or not weights:
        raise ValueError("adjusted_weights must be a non-empty dict")


def _normalize(
    out: dict,
    *,
    base_weights: dict,
    allowed_tickers: set[str],
    max_single_position: float,
    bull_output: dict,
    bear_output: dict,
    research_report: dict,
) -> dict:
    mj = out.get("market_judgment") or {}
    regime = str(mj.get("regime", "")).strip()
    if regime not in _VALID_REGIMES:
        regime = "neutral"

    stance = str(out.get("recommended_stance", "")).strip()
    if stance not in _VALID_STANCES_5:
        stance = "maintain"

    try:
        conf = float(mj.get("adjusted_confidence", 0.5) or 0.5)
    except (TypeError, ValueError):
        conf = 0.5
    conf = max(0.0, min(1.0, conf))

    uncertainty = bool(mj.get("uncertainty_flag", False))
    # 自动检测：如果 Bull/Bear 置信度差距 < 0.15，强制 uncertainty
    bull_conf = float(bull_output.get("confidence", 0.5) or 0.5)
    bear_conf = float(bear_output.get("confidence", 0.5) or 0.5)
    if abs(bull_conf - bear_conf) < 0.15:
        uncertainty = True

    # key_events: 优先从 research_report 继承
    key_events = out.get("key_events") or []
    if not isinstance(key_events, list) or not key_events:
        key_events = (research_report.get("macro_outlook") or {}).get("key_events") or []
    key_events = [str(e).strip() for e in key_events if str(e).strip()][:5]
    if not key_events:
        key_events = ["normal market conditions"]

    raw_weights = out.get("adjusted_weights") or {}
    adjusted = _sanitize_weights(
        raw_weights,
        allowed_tickers=allowed_tickers,
        max_single_position=max_single_position,
        fallback=base_weights,
    )

    actual_adjustments = _compute_adjustments(base_weights, adjusted)

    # LLM 提供的 reason 优先
    llm_adjustments = out.get("weight_adjustments") or []
    if isinstance(llm_adjustments, list) and llm_adjustments:
        reason_by_ticker = {}
        for item in llm_adjustments:
            if isinstance(item, dict) and item.get("ticker"):
                reason_by_ticker[str(item["ticker"]).upper()] = str(item.get("reason", ""))[:80]
        for item in actual_adjustments:
            item["reason"] = reason_by_ticker.get(item["ticker"], "")

    # 构建 debate_summary
    debate_summary = _build_debate_summary(bull_output, bear_output, out)

    return {
        # ── 兼容 researcher_out 接口（Risk MGR 消费这些字段）──
        "market_judgment": {
            "regime":              regime,
            "adjusted_confidence": conf,
            "uncertainty_flag":    uncertainty,
        },
        "recommended_stance":  stance,
        "adjusted_weights":    adjusted,
        "weight_adjustments":  actual_adjustments,
        "reasoning":           str(out.get("reasoning", ""))[:500],
        "consensus_points":    list(out.get("consensus_points") or [])[:5],
        "divergence_points":   list(out.get("divergence_points") or [])[:5],
        "key_events":          key_events,
        "used_degraded_fallback": False,
        # ── 新增字段（Communicator 消费，Risk MGR 不消费）──
        "debate_summary":      debate_summary,
    }


def _build_debate_summary(bull_output: dict, bear_output: dict, synth_raw: dict) -> dict:
    """构建 debate_summary 供 Communicator 展示。"""
    return {
        "bull_confidence":      float(bull_output.get("confidence", 0.5) or 0.5),
        "bear_confidence":      float(bear_output.get("confidence", 0.5) or 0.5),
        "bull_stance":          bull_output.get("stance", "maintain"),
        "bear_stance":          bear_output.get("stance", "reduce"),
        "bull_arguments":       (bull_output.get("arguments") or [])[:3],
        "bear_arguments":       (bear_output.get("arguments") or [])[:3],
        "resolution":           str(synth_raw.get("debate_resolution", ""))[:200],
        "bull_failed":          bool(bull_output.get("failed", False)),
        "bear_failed":          bool(bear_output.get("failed", False)),
    }


def _sanitize_weights(
    raw: dict,
    *,
    allowed_tickers: set[str],
    max_single_position: float,
    fallback: dict,
) -> dict:
    """
    清洗 LLM 输出的 adjusted_weights：
      1. 只保留 allowed_tickers 内的 ticker
      2. 负数 / 非数字 → 0
      3. 单仓 clip 到 max_single_position
      4. 缺失 CASH → 自动补
      5. 总和归一化到 1.0
    """
    cleaned: dict[str, float] = {}
    for k, v in raw.items():
        ticker = str(k).upper().strip()
        if ticker not in allowed_tickers:
            logger.warning(f"[SYNTHESIZER] dropped unknown ticker '{ticker}'")
            continue
        try:
            w = float(v)
        except (TypeError, ValueError):
            continue
        if w < 0:
            w = 0.0
        cleaned[ticker] = w

    if not cleaned:
        logger.warning("[SYNTHESIZER] weights empty after cleaning — fallback to base_weights")
        return {k: round(float(v), 4) for k, v in fallback.items()}

    for t in list(cleaned.keys()):
        if t == "CASH":
            continue
        if cleaned[t] > max_single_position:
            cleaned[t] = max_single_position

    cleaned.setdefault("CASH", 0.0)

    total = sum(cleaned.values())
    if total <= 0:
        return {k: round(float(v), 4) for k, v in fallback.items()}

    scaled = {t: w / total for t, w in cleaned.items()}

    out = {t: round(w, 4) for t, w in scaled.items() if t != "CASH"}
    out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 4)
    return out


def _compute_adjustments(
    base: dict[str, float],
    adjusted: dict[str, float],
    threshold: float = 0.01,
) -> list[dict]:
    """对比 base vs adjusted，生成 delta 明细（过滤 < threshold 的噪声）。"""
    out: list[dict] = []
    all_tickers = set(base.keys()) | set(adjusted.keys())
    for ticker in sorted(all_tickers):
        b = float(base.get(ticker, 0.0) or 0.0)
        a = float(adjusted.get(ticker, 0.0) or 0.0)
        delta = a - b
        if abs(delta) < threshold:
            continue
        out.append({
            "ticker":   ticker,
            "base":     round(b, 4),
            "adjusted": round(a, 4),
            "delta":    round(delta, 4),
            "reason":   "",
        })
    return out


def _degraded_output(
    base_weights: dict,
    bull_output: dict,
    bear_output: dict,
    research_report: dict,
    error: str | None,
) -> dict:
    """LLM 全部重试失败时的安全降级输出。"""
    key_events = (research_report.get("macro_outlook") or {}).get("key_events") or [
        "normal market conditions"
    ]

    return {
        "market_judgment": {
            "regime":              "neutral",
            "adjusted_confidence": 0.3,
            "uncertainty_flag":    True,
        },
        "recommended_stance":   "maintain",
        "adjusted_weights":     {k: round(float(v), 4) for k, v in base_weights.items()},
        "weight_adjustments":   [],
        "reasoning":            f"Synthesizer 降级：沿用 Stage 2 基准仓位 (error={error})",
        "consensus_points":     [],
        "divergence_points":    [],
        "key_events":           key_events,
        "used_degraded_fallback": True,
        "debate_summary":       _build_debate_summary(bull_output, bear_output, {}),
    }
