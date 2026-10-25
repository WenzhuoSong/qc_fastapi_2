"""Thesis writer: LLM prose over Python-computed evidence.

Guardrails: every number in the LLM text must be traceable to the evidence
(within rounding); on failure we retry once with the offending numbers, then
fall back to a deterministic template. Prices and levels are never LLM output.
"""
from __future__ import annotations

import json
import logging
import math
import re
from typing import Any, Literal

from pydantic import BaseModel, Field

from scout.config import get_scout_settings

logger = logging.getLogger("scout.thesis")


class Thesis(BaseModel):
    headline: str = Field(description="一句话标题，≤30 个汉字")
    thesis: str = Field(description="2–4 句：为什么这是机会、依据是什么")
    catalysts: list[str] = Field(description="0–3 条近期催化剂，只能来自 evidence")
    counter_evidence: list[str] = Field(description="1–3 条反方证据或不利因素")
    key_risk: str = Field(description="最需要盯住的一个风险")
    confidence: Literal["low", "medium", "high"]
    confidence_reason: str = Field(description="置信度理由，引用历史基准或证据一致性")


SYSTEM_PROMPT = """你是一名美股交易机会分析师，为一位有经验的个人投资者撰写"机会卡片"的文字部分。

输入是 Python 已经计算好的 evidence（JSON）：规则触发、价格特征、入场/止损/目标、该规则的历史基准、
市场背景、近期新闻和财报信息。入场区间、止损和目标已经确定，你不需要也不能修改。

写作规则：
1. 只能使用 evidence 中出现的数字；不要自行计算新的数字（例如不要自己算涨幅或比例）。
   百分比字段是小数（0.052 表示 5.2%），写成百分比时保留与原值一致的精度。
2. 不要编造新闻、财报、分析师观点或任何 evidence 中没有的事实。没有新闻就说"近期无可匹配新闻"。
3. 必须给出至少一条反方证据（例如大盘环境、历史胜率一般、距离 52 周高点、财报临近、量能不足等）。
4. 历史基准要如实表述，样本少（n < 30）或平均超额 ≤ 0 时，置信度不能是 high。
5. 历史基准存在幸存者偏差（用的是当前指数成分股），不要把它当成保证。
6. 中文输出，简洁、具体，不写空话；不要出现"建议买入"之类的指令，用"机会/条件/失效"的语气。"""

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_NUM_RE = re.compile(r"(?<![A-Za-z\d.])[-+]?\d+(?:\.\d+)?")
_SMALL_INT_OK = 5


def _numeric_leaves(value: Any) -> list[float]:
    if isinstance(value, bool) or value is None:
        return []
    if isinstance(value, (int, float)):
        return [float(value)] if math.isfinite(value) else []
    if isinstance(value, dict):
        return [x for v in value.values() for x in _numeric_leaves(v)]
    if isinstance(value, (list, tuple)):
        return [x for v in value for x in _numeric_leaves(v)]
    if isinstance(value, str):
        return [float(m) for m in _NUM_RE.findall(_DATE_RE.sub(" ", value))]
    return []


def allowed_numbers(evidence: dict) -> list[float]:
    out: set[float] = set()
    for v in _numeric_leaves(evidence):
        out.update({v, abs(v), v * 100, abs(v) * 100})
    return sorted(out)


def unsupported_numbers(text: str, allowed: list[float]) -> list[str]:
    bad = []
    for token in _NUM_RE.findall(_DATE_RE.sub(" ", text)):
        x = float(token)
        if float(x).is_integer() and abs(x) <= _SMALL_INT_OK:
            continue
        decimals = len(token.split(".")[1]) if "." in token else 0
        tol_abs = 0.5 * 10 ** (-decimals) + 1e-9
        if not any(abs(abs(x) - y) <= max(tol_abs, 0.01 * y) for y in allowed if y >= 0):
            bad.append(token)
    return bad


def thesis_text(t: Thesis) -> str:
    return "\n".join([t.headline, t.thesis, *t.catalysts, *t.counter_evidence, t.key_risk, t.confidence_reason])


def _pct(x: float | None, digits: int = 1) -> str:
    return "—" if x is None else f"{x * 100:.{digits}f}%"


def template_thesis(ev: dict) -> Thesis:
    setup, price, br, market = ev["setup"], ev["price"], ev.get("base_rate") or {}, ev.get("market") or {}
    n = br.get("n", 0)
    excess = br.get("avg_excess")
    lines = [
        f"{ev['ticker']} 触发「{setup['label']}」：{setup['rule']}",
        f"20 日涨幅 {_pct(price.get('ret_20d'))}，相对强度分位 {_pct(price.get('rs_pct'), 0)}，"
        f"量能为均量的 {price.get('vol_ratio', 0):.1f} 倍。",
    ]
    if n:
        lines.append(
            f"该规则历史 {n} 次样本，胜率 {_pct(br.get('win_rate'), 0)}，平均超额 {_pct(excess)}（{br.get('regime')}）。"
        )
    counter = []
    if market.get("regime") in {"downtrend", "mixed"}:
        counter.append(f"大盘处于 {market['regime']} 状态，突破类信号可靠性下降。")
    if excess is not None and excess <= 0:
        counter.append("该规则历史平均超额不为正，属于观察级别。")
    if (price.get("dist_52w_high") or 0) < -0.2:
        counter.append(f"距离 52 周高点仍有 {_pct(price.get('dist_52w_high'))}，上方套牢盘较多。")
    nxt = (ev.get("earnings") or {}).get("next")
    if nxt and nxt.get("days_until", 99) <= 10:
        counter.append(f"{nxt['date']} 有财报，事件风险较高。")
    if not counter:
        counter.append("单一规则信号，缺少基本面或新闻层面的确认。")
    news = ev.get("news") or []
    catalysts = [n_["headline"] for n_ in news[:2]]
    conf = "medium" if n >= 30 and (excess or 0) > 0 else "low"
    return Thesis(
        headline=f"{ev['ticker']} {setup['label']}",
        thesis=" ".join(lines),
        catalysts=catalysts,
        counter_evidence=counter[:3],
        key_risk=f"跌破止损 {ev['levels']['stop']} 即信号失效。",
        confidence=conf,
        confidence_reason=f"历史样本 {n}，平均超额 {_pct(excess)}。" if n else "暂无历史基准。",
    )


async def write_thesis(ev: dict) -> tuple[Thesis, str]:
    """Returns (thesis, source) where source is 'llm' or 'template'."""
    settings = get_scout_settings()
    if not settings.openai_api_key:
        return template_thesis(ev), "template"
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=settings.openai_api_key, timeout=settings.scout_llm_timeout_s)
    allowed = allowed_numbers(ev)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps(ev, ensure_ascii=False, default=str)},
    ]
    for attempt in range(2):
        try:
            resp = await client.beta.chat.completions.parse(
                model=settings.scout_llm_model, messages=messages, response_format=Thesis, temperature=0.3,
            )
            parsed = resp.choices[0].message.parsed
        except Exception as exc:
            logger.warning("thesis LLM call failed for %s: %s", ev.get("ticker"), exc)
            break
        if parsed is None:
            break
        bad = unsupported_numbers(thesis_text(parsed), allowed)
        if not bad:
            if parsed.confidence == "high" and ((ev.get("base_rate") or {}).get("n", 0) < 30):
                parsed.confidence = "medium"
            return parsed, "llm"
        logger.info("thesis for %s had unsupported numbers %s (attempt %d)", ev.get("ticker"), bad, attempt + 1)
        messages += [
            {"role": "assistant", "content": parsed.model_dump_json()},
            {"role": "user", "content": f"以下数字不在 evidence 中：{', '.join(bad)}。请删除或改用 evidence 中的数字后重写。"},
        ]
    return template_thesis(ev), "template"
