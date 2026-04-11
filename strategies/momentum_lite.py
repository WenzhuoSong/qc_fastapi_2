# strategies/momentum_lite.py
"""
MomentumLite v1.0 — Phase 1 默认策略

5 因子复合评分：
    0.30 · z(mom_20d)  + 0.35 · z(mom_60d) + 0.20 · z(mom_252d)
  + 0.10 · z(100 - rsi_14)        # RSI 反向：超买扣分
  + 0.05 · z(1 / atr_pct)         # 低波动加分

权重优化：
    Score 加权（70%）+ 波动率反比加权（30%）
    ↓
    单仓上限截断（max_single_position）
    ↓
    配合现金底线（min_cash_pct）
"""
import logging
import statistics
from typing import Any

from strategies.base import Strategy, ScoredTicker

logger = logging.getLogger("qc_fastapi_2.strategy.momentum_lite")


class MomentumLiteV1(Strategy):
    name = "momentum_lite_v1"
    version = "1.0"
    description = "5因子动量 + 波动率调整的 ETF 策略"

    DEFAULT_PARAMS: dict[str, Any] = {
        # 因子权重（总和 = 1.0）
        "w_mom_20d":       0.30,
        "w_mom_60d":       0.35,
        "w_mom_252d":      0.20,
        "w_rsi":           0.10,
        "w_atr":           0.05,
        # 归一化
        "zscore_clip":     3.0,
        # 选股参数
        "max_holdings":    8,
        # 权重混合
        "vol_blend_alpha": 0.70,
    }

    def __init__(self, params: dict[str, Any] | None = None):
        merged = {**self.DEFAULT_PARAMS, **(params or {})}
        super().__init__(merged)

    # ─────────────────────────────── 评分 ───────────────────────────────

    def score(
        self,
        holdings: list[dict],
        context: dict[str, Any],
    ) -> list[ScoredTicker]:
        p = self.params
        required = ("mom_20d", "mom_60d", "mom_252d", "rsi_14", "atr_pct")

        valid = [
            h for h in holdings
            if all(h.get(f) is not None for f in required)
            and h.get("ticker")
        ]
        if not valid:
            logger.warning("momentum_lite: no tickers with complete factor data")
            return []

        clip = float(p["zscore_clip"])

        z_mom20  = _zscore([float(h["mom_20d"])  for h in valid], clip)
        z_mom60  = _zscore([float(h["mom_60d"])  for h in valid], clip)
        z_mom252 = _zscore([float(h["mom_252d"]) for h in valid], clip)
        # RSI 反向：低 RSI 得分高
        z_rsi    = _zscore([100.0 - float(h["rsi_14"]) for h in valid], clip)
        # ATR 反比：低波动得分高
        z_atr    = _zscore(
            [1.0 / float(h["atr_pct"]) if float(h["atr_pct"]) > 0 else 0.0 for h in valid],
            clip,
        )

        results: list[ScoredTicker] = []
        for i, h in enumerate(valid):
            composite = (
                p["w_mom_20d"]  * z_mom20[i]  +
                p["w_mom_60d"]  * z_mom60[i]  +
                p["w_mom_252d"] * z_mom252[i] +
                p["w_rsi"]      * z_rsi[i]    +
                p["w_atr"]      * z_atr[i]
            )
            results.append(ScoredTicker(
                ticker=h["ticker"],
                score=composite,
                factor_breakdown={
                    "z_mom_20d":  round(z_mom20[i],  4),
                    "z_mom_60d":  round(z_mom60[i],  4),
                    "z_mom_252d": round(z_mom252[i], 4),
                    "z_rsi":      round(z_rsi[i],    4),
                    "z_atr":      round(z_atr[i],    4),
                },
                raw_factors={
                    "mom_20d":  float(h["mom_20d"]),
                    "mom_60d":  float(h["mom_60d"]),
                    "mom_252d": float(h["mom_252d"]),
                    "rsi_14":   float(h["rsi_14"]),
                    "atr_pct":  float(h["atr_pct"]),
                },
            ))

        results.sort(key=lambda x: x.score, reverse=True)
        return results

    # ─────────────────────────────── 权重优化 ───────────────────────────────

    def optimize(
        self,
        scored: list[ScoredTicker],
        context: dict[str, Any],
    ) -> dict[str, float]:
        if not scored:
            return {"CASH": 1.0}

        p = self.params
        risk = context.get("risk_params", {})
        max_pos  = float(risk.get("max_single_position", 0.20))
        min_cash = float(risk.get("min_cash_pct", 0.05))

        # Step 1: 按 direction_bias 动态确定持仓数 N
        n = _decide_n(
            bias=context.get("direction_bias", "neutral"),
            confidence=float(context.get("confidence", 0.5) or 0.5),
            max_n=int(p["max_holdings"]),
        )
        selected = scored[:n]
        if not selected:
            return {"CASH": 1.0}

        # Step 2: Score 加权（平移到正数避免负分爆炸）
        min_s = min(s.score for s in selected)
        shifted = [s.score - min_s + 0.1 for s in selected]
        total_s = sum(shifted)
        score_weights = {
            selected[i].ticker: shifted[i] / total_s
            for i in range(len(selected))
        }

        # Step 3: 波动率反比权重
        inv_vol = {}
        for s in selected:
            atr = s.raw_factors.get("atr_pct") or 0.0
            inv_vol[s.ticker] = 1.0 / atr if atr > 0 else 0.0
        total_iv = sum(inv_vol.values())
        if total_iv > 0:
            vol_weights = {t: v / total_iv for t, v in inv_vol.items()}
        else:
            vol_weights = {t: 1.0 / len(selected) for t in score_weights}

        # Step 4: 混合
        alpha = float(p["vol_blend_alpha"])
        mixed = {
            t: alpha * score_weights[t] + (1 - alpha) * vol_weights[t]
            for t in score_weights
        }

        # Step 5: 单仓上限截断
        clipped = {t: min(w, max_pos) for t, w in mixed.items()}
        total_after_cap = sum(clipped.values())

        # Step 6: 配合现金底线
        leftover = 1.0 - total_after_cap
        if leftover >= min_cash:
            # 已有足够现金，直接保留
            final = {t: round(w, 4) for t, w in clipped.items()}
        else:
            # 按比例缩小到 (1 - min_cash)
            target = 1.0 - min_cash
            if total_after_cap > 0:
                scale = target / total_after_cap
                final = {t: round(w * scale, 4) for t, w in clipped.items()}
            else:
                final = {}

        # Step 7: 精确现金（吸收四舍五入误差）
        non_cash_sum = sum(final.values())
        final["CASH"] = round(max(1.0 - non_cash_sum, 0.0), 4)

        return final


# ─────────────────────────────── 工具函数 ───────────────────────────────


def _zscore(values: list[float], clip: float) -> list[float]:
    """截面 Z-Score + clip，列表长度不变。"""
    n = len(values)
    if n == 0:
        return []
    if n < 2:
        return [0.0] * n
    mean = statistics.fmean(values)
    try:
        std = statistics.stdev(values)
    except statistics.StatisticsError:
        return [0.0] * n
    if std == 0:
        return [0.0] * n
    return [max(-clip, min(clip, (v - mean) / std)) for v in values]


def _decide_n(bias: str, confidence: float, max_n: int) -> int:
    """市场偏向决定持仓数。"""
    if bias == "bullish" and confidence > 0.65:
        n = max_n
    elif bias == "bullish":
        n = int(round(max_n * 0.8))
    elif bias == "neutral":
        n = int(round(max_n * 0.7))
    elif bias == "bearish":
        n = int(round(max_n * 0.5))
    else:
        n = int(round(max_n * 0.6))
    return max(3, min(n, max_n))
