# agents/researcher.py
"""
Stage 3: RESEARCHER —— 宏观策略师（LLM 合成站）

update.txt 的接力棒：
    Stage 2 Python Quant 产出 base_weights（纯数学基准）→
    Stage 3 LLM RESEARCHER 综合 base_weights + 新闻 + 日程 + 定量指标 →
    产出 adjusted_weights（draft_proposal）+ regime + stance + reasoning + key_events →
    Stage 4 Python Risk Manager 审查、应用 overlays、签 token

核心职责：**基于 base_weights 做定性微调**。LLM 不从零分配，而是针对
Python 已经算好的基准做 ±a few percent 的偏移、剔除或加入个别标的，并给
出人类可读的 reasoning。

validation 非常严格：任何越界（non-ticker、和不为 1、单仓超限）都会被
renormalize，若 LLM 输出彻底无法解析 → 回落到 base_weights。

LLM: settings.openai_model_heavy (gpt-4o)，单次调用，3 次重试。
"""
from __future__ import annotations

import json
import logging
import time

from openai import AsyncOpenAI

from config import get_settings

logger = logging.getLogger("qc_fastapi_2.researcher")
settings = get_settings()

_client: AsyncOpenAI | None = None


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=settings.openai_api_key)
    return _client


SYSTEM_PROMPT = """你是量化交易系统的首席宏观策略师（Stage 3 Synthesizer）。

【你的位置】
    上游 Stage 2 是 Python 量化基准层，它已经通过 5 因子（动量/RSI/ATR）
    给你算好了一份"纯数学基准仓位 base_weights"。
    下游 Stage 4 是 Python 风控层，会做硬边界检查和防守覆盖。

【你的任务】
    拿到 base_weights 后，结合：
      1) 市场技术面散文
      2) 定量指标 (breadth, SPY mom, risk_on_score, drawdown...)
      3) 宏观新闻摘要
      4) 本周经济日程
    做**定性微调**，产出 adjusted_weights（draft_proposal），并给出
    regime 判断 + stance + reasoning + key_events。

【调整原则】
    · 基准是 Python 的量化结果，默认尊重它；微调是为了把 Python 看不到
      的东西（新闻、日程、regime 转换）注入权重。
    · 不要重写 base_weights —— 大多数标的的权重应该和 base_weights
      在 ±5% 以内。只有在有明确宏观理由时才做大幅调整（±10%+）。
    · 可以剔除 base_weights 里的个别标的（置 0），也可以加入 base_weights
      里没有的标的（但必须是 current holdings 里出现过的 ticker）。
    · 剩余权重必须进入 CASH。
    · 单仓不得超过 max_single_position（见 risk_params，默认 0.20）。
    · adjusted_weights 必须包含 "CASH"，所有值总和 = 1.0。

【必须输出纯 JSON】
严格字段：
{
  "market_judgment": {
    "regime": "bull_trend | bull_weak | neutral | bear_weak | bear_trend | high_vol",
    "adjusted_confidence": <float 0.0-1.0>,
    "uncertainty_flag": <bool>
  },
  "recommended_stance": "maintain | increase | reduce | defensive",
  "adjusted_weights": {
    "<TICKER>": <float>,
    ...
    "CASH": <float>
  },
  "weight_adjustments": [
    {
      "ticker": "<TICKER>",
      "base": <float>,
      "adjusted": <float>,
      "delta": <float>,
      "reason": "<≤40 字中文理由>"
    }
  ],
  "reasoning": "<≤150 字中文总理由，解释 regime + 整体调整思路>",
  "consensus_points":  ["...", "..."],
  "divergence_points": ["...", "..."],
  "key_events": ["<event phrase 1>", "<event phrase 2>", ...]
}

【regime 取值规则（严格 6 选 1）】
  · bull_trend / bull_weak / neutral / bear_weak / bear_trend / high_vol

【stance 取值规则（严格 4 选 1）】
  · maintain / increase / reduce / defensive

【key_events 规则（至关重要！）】
  · 必须产出 3-5 条短语，每条 ≤ 60 字。
  · 必须使用下游 transmission 匹配器能识别的关键字：
      oil surge / hormuz / middle east / opec / war / russia / ukraine / taiwan /
      rate hike / fed hawkish / cpi / pce / fomc / yields surge /
      rate cut / dovish pivot / liquidity / credit stress / vix spike /
      bank crisis / recession / pmi contraction / jobless claims /
      demand destruction / earnings recession
  · 没有宏观事件时返回 ["normal market conditions"]，不要编造。

【weight_adjustments 规则】
  · 只列出和 base_weights 差 ≥ 1% 的 ticker（含新增和剔除）。
  · 同一个 reason 可以服务多个 ticker。

仅输出 JSON。任何额外文本都会被视为错误。"""


# ═══════════════════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════════════════


async def run_researcher_async(
    pipeline_context: dict,
    brief: dict,
    quant_baseline: dict,
) -> dict:
    """Stage 3 synthesizer。消费 baseline + brief，产出 draft_proposal。"""
    base_weights = quant_baseline.get("base_weights") or {"CASH": 1.0}
    risk_params  = pipeline_context.get("risk_params") or {}
    max_single_position = float(risk_params.get("max_single_position", 0.20))

    user_payload = _build_user_message(brief, quant_baseline, risk_params)

    allowed_tickers = _collect_allowed_tickers(brief, base_weights)

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
                f"[RESEARCHER] done in {elapsed}s | "
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
            )

        except Exception as e:
            last_error = str(e)
            logger.warning(f"[RESEARCHER] attempt {attempt} failed: {e}")

    # 所有重试失败 —— 安全降级：把 base_weights 当 adjusted_weights 返回
    logger.error(
        f"[RESEARCHER] all retries failed, degrading to base_weights. last_error={last_error}"
    )
    return _degraded_output(base_weights, last_error)


# ═══════════════════════════════════════════════════════════════
# User message builder
# ═══════════════════════════════════════════════════════════════


def _build_user_message(brief: dict, quant_baseline: dict, risk_params: dict) -> str:
    prose    = brief.get("prose_summary") or "(无)"
    macro    = brief.get("macro_news_section") or "(无)"
    calendar = brief.get("calendar_section") or "(无)"
    key_facts = brief.get("key_facts") or {}

    base_weights = quant_baseline.get("base_weights") or {}
    scoring      = quant_baseline.get("scoring_breakdown") or []
    ranking      = quant_baseline.get("ranking_summary") or {}

    # 裁剪 scoring breakdown 到前 15 条，避免 token 膨胀
    top_scored = scoring[:15]

    max_pos = float(risk_params.get("max_single_position", 0.20))
    min_cash = float(risk_params.get("min_cash_pct", 0.05))

    return (
        "## 市场技术面\n"
        f"{prose}\n\n"
        "## 定量指标\n"
        f"{json.dumps(key_facts, ensure_ascii=False, indent=2)}\n\n"
        "## 宏观新闻\n"
        f"{macro}\n\n"
        "## 本周日程\n"
        f"{calendar}\n\n"
        "## Python Stage 2 产出的基准仓位 (base_weights)\n"
        f"{json.dumps(base_weights, ensure_ascii=False, indent=2)}\n\n"
        "## 基准打分明细 (top 15, 按 score 降序)\n"
        f"{json.dumps(top_scored, ensure_ascii=False, indent=2)}\n\n"
        "## 基准排名摘要\n"
        f"{json.dumps(ranking, ensure_ascii=False, indent=2)}\n\n"
        "## 约束\n"
        f"max_single_position = {max_pos}\n"
        f"min_cash_pct = {min_cash}\n"
        "## 你的任务\n"
        "基于以上材料，输出 market_judgment + recommended_stance + adjusted_weights +\n"
        "weight_adjustments + reasoning + key_events。仅返回纯 JSON。"
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

_VALID_REGIMES = {"bull_trend", "bull_weak", "neutral", "bear_weak", "bear_trend", "high_vol"}
_VALID_STANCES = {"maintain", "increase", "reduce", "defensive"}


def _validate(out: dict) -> None:
    required = [
        "market_judgment",
        "recommended_stance",
        "adjusted_weights",
        "reasoning",
        "key_events",
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
) -> dict:
    mj = out.get("market_judgment") or {}
    regime = str(mj.get("regime", "")).strip()
    if regime not in _VALID_REGIMES:
        logger.warning(f"[RESEARCHER] invalid regime '{regime}', coerced to neutral")
        regime = "neutral"

    stance = str(out.get("recommended_stance", "")).strip()
    if stance not in _VALID_STANCES:
        logger.warning(f"[RESEARCHER] invalid stance '{stance}', coerced to maintain")
        stance = "maintain"

    try:
        conf = float(mj.get("adjusted_confidence", 0.5) or 0.5)
    except (TypeError, ValueError):
        conf = 0.5
    conf = max(0.0, min(1.0, conf))

    key_events = out.get("key_events") or []
    if not isinstance(key_events, list):
        key_events = []
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

    # 计算实际的调整项 vs base_weights（过滤 < 1% 的噪声）
    actual_adjustments = _compute_adjustments(base_weights, adjusted)

    # LLM 自己提供的 reason 优先，否则用自动计算的 delta
    llm_adjustments = out.get("weight_adjustments") or []
    if isinstance(llm_adjustments, list) and llm_adjustments:
        reason_by_ticker = {}
        for item in llm_adjustments:
            if isinstance(item, dict) and item.get("ticker"):
                reason_by_ticker[str(item["ticker"]).upper()] = str(item.get("reason", ""))[:80]
        for item in actual_adjustments:
            item["reason"] = reason_by_ticker.get(item["ticker"], "")

    return {
        "market_judgment": {
            "regime":              regime,
            "adjusted_confidence": conf,
            "uncertainty_flag":    bool(mj.get("uncertainty_flag", False)),
        },
        "recommended_stance":  stance,
        "adjusted_weights":    adjusted,
        "weight_adjustments":  actual_adjustments,
        "reasoning":           str(out.get("reasoning", ""))[:500],
        "consensus_points":    list(out.get("consensus_points") or [])[:5],
        "divergence_points":   list(out.get("divergence_points") or [])[:5],
        "key_events":          key_events,
        "used_degraded_fallback": False,
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
    清洗后若无有效头寸，回落到 fallback (base_weights)。
    """
    cleaned: dict[str, float] = {}
    for k, v in raw.items():
        ticker = str(k).upper().strip()
        if ticker not in allowed_tickers:
            logger.warning(f"[RESEARCHER] dropped unknown ticker '{ticker}'")
            continue
        try:
            w = float(v)
        except (TypeError, ValueError):
            continue
        if w < 0:
            w = 0.0
        cleaned[ticker] = w

    if not cleaned:
        logger.warning("[RESEARCHER] adjusted_weights empty after cleaning — fallback to base_weights")
        return {k: round(float(v), 4) for k, v in fallback.items()}

    # 非 CASH 单仓 clip
    for t in list(cleaned.keys()):
        if t == "CASH":
            continue
        if cleaned[t] > max_single_position:
            cleaned[t] = max_single_position

    # 若缺 CASH，显式补 0
    cleaned.setdefault("CASH", 0.0)

    # 归一化到 1.0
    total = sum(cleaned.values())
    if total <= 0:
        return {k: round(float(v), 4) for k, v in fallback.items()}

    scaled = {t: w / total for t, w in cleaned.items()}

    # 再做一次四舍五入 + CASH 吸收误差
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


def _degraded_output(base_weights: dict, error: str | None) -> dict:
    """LLM 全部重试失败时的安全降级输出。"""
    return {
        "market_judgment": {
            "regime":              "neutral",
            "adjusted_confidence": 0.3,
            "uncertainty_flag":    True,
        },
        "recommended_stance":   "maintain",
        "adjusted_weights":     {k: round(float(v), 4) for k, v in base_weights.items()},
        "weight_adjustments":   [],
        "reasoning":            f"LLM 降级：沿用 Stage 2 基准仓位 (error={error})",
        "consensus_points":     [],
        "divergence_points":    [],
        "key_events":           ["normal market conditions"],
        "used_degraded_fallback": True,
    }
