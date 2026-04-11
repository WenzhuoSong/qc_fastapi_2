"""
Macro event → sector transmission patterns.

STRATEGY ENGINE.Overlay 1 调用：
    pattern = match_event_to_pattern(researcher_out.key_events)
    if pattern:
        vector = generate_transmission_vector(pattern)
        tilted = apply_transmission(ideal_weights, vector, max_pos)

Pattern 选 6 个 canonical 场景（从 qc_fastapi/transmission_rules.py 抽出）：
    supply_shock_oil / war_geopolitical / rate_shock_hawkish
    risk_off_credit_stress / recession_demand_collapse / fed_dovish_easing

每个 pattern 的 vector 是 {sector: strength in [-1, +1]}。
apply_transmission 把 strength 转成乘子并重新归一化。
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger("qc_fastapi_2.transmission")


# ═══════════════════════════════════════════════════════════════
# Canonical patterns
# ═══════════════════════════════════════════════════════════════

CANONICAL_TRANSMISSIONS: dict[str, dict] = {
    "supply_shock_oil": {
        "keywords": [
            "oil spike", "oil surge", "crude", "wti", "brent",
            "hormuz", "strait", "iran", "embargo", "middle east",
            "opec cut", "supply disruption", "saudi", "energy crisis",
            "natural gas", "lng", "pipeline", "refinery",
        ],
        "vector": {
            "XLE": 0.95, "XLB": 0.60, "XLI": 0.70,
            "XLY": -0.75, "XLK": -0.50, "XLF": -0.30,
            "XLP": 0.10, "XLV": 0.15, "XLU": -0.20,
            "XLC": -0.40, "XLRE": -0.60,
        },
    },

    "war_geopolitical": {
        "keywords": [
            "war", "invasion", "attack", "missile", "bombing", "military strike",
            "russia", "ukraine", "israel", "hamas", "gaza", "hezbollah",
            "taiwan", "south china sea", "north korea",
            "sanctions", "conflict", "escalation", "drone strike", "cyber attack",
        ],
        "vector": {
            "XLI": 0.90, "XLE": 0.80, "XLB": 0.50,
            "XLY": -0.70, "XLF": -0.50, "XLK": -0.45,
            "XLV": 0.30, "XLP": 0.25, "XLU": 0.20,
            "XLRE": -0.40, "XLC": -0.35,
        },
    },

    "rate_shock_hawkish": {
        "keywords": [
            "rate hike", "fed hawkish", "yields surge", "higher for longer",
            "real rates", "10-year yield", "30-year yield",
            "bond selloff", "tightening", "quantitative tightening", "qt",
            "inflation", "cpi", "pce", "core inflation", "sticky inflation",
            "wage growth", "powell hawkish", "fomc", "restrictive policy",
        ],
        "vector": {
            "XLF": 0.70, "XLE": 0.30, "XLB": 0.20,
            "XLK": -0.80, "XLRE": -0.90, "XLU": -0.60,
            "XLY": -0.50, "XLP": -0.10, "XLV": 0.10,
            "XLC": -0.40, "XLI": 0.10,
        },
    },

    "risk_off_credit_stress": {
        "keywords": [
            "credit stress", "bank crisis", "vix spike", "contagion",
            "crash", "liquidity crisis", "margin call", "deleveraging",
            "panic", "svb", "credit suisse", "bank failure",
        ],
        "vector": {
            "XLV": 0.85, "XLP": 0.80, "XLU": 0.70,
            "XLY": -0.85, "XLK": -0.70, "XLF": -0.70,
            "XLI": -0.40, "XLE": -0.30, "XLB": -0.40,
            "XLRE": -0.60, "XLC": -0.50,
        },
    },

    "recession_demand_collapse": {
        "keywords": [
            "recession", "gdp miss", "pmi contraction", "mass layoffs",
            "demand destruction", "hard landing", "unemployment spike",
            "consumer collapse", "retail sales miss", "earnings recession",
            "profit warning", "guidance cut", "weak demand",
            "ism", "manufacturing", "services pmi", "jobless claims",
        ],
        "vector": {
            "XLV": 0.75, "XLP": 0.70, "XLU": 0.60,
            "XLY": -0.80, "XLI": -0.60, "XLE": -0.60,
            "XLF": -0.50, "XLK": -0.40, "XLB": -0.50,
            "XLRE": -0.40, "XLC": -0.30,
        },
    },

    "fed_dovish_easing": {
        "keywords": [
            "rate cut", "qe", "dovish pivot", "liquidity injection",
            "powell dovish", "fed easing", "stimulus", "emergency cut",
            "soft landing", "pause", "disinflation", "inflation cooling",
            "balance sheet expansion", "repo", "bank term funding",
        ],
        "vector": {
            "XLRE": 0.80, "XLK": 0.70, "XLU": 0.70,
            "XLY": 0.60, "XLF": 0.40, "XLI": 0.30,
            "XLC": 0.40, "XLV": 0.20, "XLP": 0.10,
            "XLE": -0.30, "XLB": -0.20,
        },
    },
}


# ═══════════════════════════════════════════════════════════════
# Matching
# ═══════════════════════════════════════════════════════════════

def match_event_to_pattern(
    key_events: list[str],
    min_keyword_matches: int = 1,
) -> Optional[str]:
    """
    根据 RESEARCHER 输出的 key_events（3−5 条短语），匹配最贴合的 canonical pattern。

    返回 pattern_name 或 None。
    取击中 keyword 最多的那个 pattern（至少击中 min_keyword_matches 个关键字）。
    """
    if not key_events:
        return None

    haystack = " ".join(str(e) for e in key_events).lower()

    best: tuple[str, int] | None = None
    for name, meta in CANONICAL_TRANSMISSIONS.items():
        score = sum(1 for kw in meta["keywords"] if kw in haystack)
        if score >= min_keyword_matches:
            if best is None or score > best[1]:
                best = (name, score)

    return best[0] if best else None


def generate_transmission_vector(pattern: str) -> dict[str, float]:
    """返回 pattern 对应的 vector，未找到返回空 dict。"""
    meta = CANONICAL_TRANSMISSIONS.get(pattern)
    return dict(meta["vector"]) if meta else {}


# ═══════════════════════════════════════════════════════════════
# Apply
# ═══════════════════════════════════════════════════════════════

def apply_transmission(
    weights: dict[str, float],
    vector:  dict[str, float],
    max_pos: float = 0.20,
) -> dict[str, float]:
    """
    把 transmission vector 应用到 weights：
      - strength ∈ [-1, +1] 线性映射为乘子 (1 + 0.5 * strength)，即最多 +/- 50%
      - 只影响 weights 里已存在且 > 0 的 ticker；CASH 不动
      - 乘完后 clip 到 max_pos，再把所有非 CASH 权重归一化到原来的总和
    """
    if not weights or not vector:
        return weights

    equity = {t: w for t, w in weights.items() if t != "CASH" and w > 0}
    if not equity:
        return weights

    original_equity_sum = sum(equity.values())

    tilted: dict[str, float] = {}
    for t, w in equity.items():
        strength = float(vector.get(t, 0.0) or 0.0)
        mult = 1.0 + 0.5 * max(-1.0, min(1.0, strength))
        tilted[t] = max(0.0, w * mult)

    # clip single position
    tilted = {t: min(w, max_pos) for t, w in tilted.items()}

    new_sum = sum(tilted.values())
    if new_sum <= 0:
        return weights

    # 归一化回原本的 equity 总和，保持 CASH 不变
    scale = original_equity_sum / new_sum
    tilted = {t: round(w * scale, 6) for t, w in tilted.items()}

    out = dict(weights)
    for t, w in tilted.items():
        out[t] = w
    return out
