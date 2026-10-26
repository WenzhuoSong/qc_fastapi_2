"""Detector v1 rules. Parameters are fixed common-sense values, not fitted to
the replay — the base-rate table measures them, it does not tune them."""
from __future__ import annotations

import numpy as np
import pandas as pd

from scout.detectors import Detector, Levels, base_mask, r_targets
from scout.features import FeaturePanel

MIN_STOP_ATR = 1.0   # a stop inside one ATR of the entry is inside daily noise


def _levels(lo: float, hi: float, stop: float, atr: float, r1: float = 2.0, r2: float = 3.0) -> Levels:
    stop = min(stop, (lo + hi) / 2 - MIN_STOP_ATR * atr)
    return r_targets(lo, hi, stop, r1, r2)


# D1 ─ Breakout from a 55-day range on expanding volume, in an established uptrend.
def _breakout_mask(p: FeaturePanel) -> pd.DataFrame:
    return (
        base_mask(p)
        & (p["close"] > p["high_55_prior"])
        & (p["vol_ratio"] >= 1.8)
        & (p["close"] > p["sma50"])
        & (p["sma50"] > p["sma200"])
        & (p["rs_pct"] >= 0.80)
        & (p["close_loc"] >= 0.60)
        & (p["gap_pct"] < 0.04)
    )


def _breakout_levels(r: dict) -> Levels:
    atr, close = r["atr14"], r["close"]
    lo, hi = close - 0.25 * atr, close + 0.5 * atr
    return _levels(lo, hi, r["high_55_prior"] - atr, atr)


# D2 ─ Short-term oversold dip inside a strong uptrend, on light volume.
def _pullback_mask(p: FeaturePanel) -> pd.DataFrame:
    return (
        base_mask(p)
        & (p["rs_pct"] >= 0.80)
        & (p["close"] > p["sma200"])
        & (p["sma50"] > p["sma200"])
        & (p["ret_63d"] > 0.10)
        & (p["rsi2"] <= 10)
        & (p["close"] >= 0.97 * p["sma50"])
        & (p["close"] <= p["sma20"])
        & (p["vol_ratio"] <= 1.2)
    )


def _pullback_levels(r: dict) -> Levels:
    atr, close = r["atr14"], r["close"]
    lo, hi = close - 0.5 * atr, close + 0.25 * atr
    stop = min(r["low_5"], r["sma50"]) - 0.5 * atr
    lv = _levels(lo, hi, stop, atr, r1=1.5, r2=2.5)
    # The prior swing high is the natural first target when it is further out.
    t1 = max(lv.target1, round(r["high_20"], 2))
    return Levels(lv.entry_lo, lv.entry_hi, lv.stop, t1, max(lv.target2, t1))


# D3 ─ Large gap up on heavy volume that holds into the close (typically earnings).
def _power_gap_mask(p: FeaturePanel) -> pd.DataFrame:
    gap_floor = np.maximum(0.04, 2.0 * p["atr_pct_prev"])
    return (
        base_mask(p)
        & (p["gap_pct"] >= gap_floor)
        & (p["vol_ratio"] >= 3.0)
        & (p["close_loc"] >= 0.50)
        & (p["close"] >= p["open"])
    )


def _power_gap_levels(r: dict) -> Levels:
    atr, close = r["atr14"], r["close"]
    return _levels(close - 0.5 * atr, close + 0.5 * atr, r["low"] - 0.25 * atr, atr)


# D4 ─ Sector turns from lagging to leading SPY; take the sector's strongest names.
def _sector_rotation_mask(p: FeaturePanel) -> pd.DataFrame:
    return (
        base_mask(p)
        & (p["sector_rel_20"] > 0)
        & (p["sector_rel_20_prev5"] <= 0)
        & (p["sector_rel_63"] > -0.05)
        & (p["sector_rs_pct"] >= 0.85)
        & (p["rs_pct"] >= 0.60)
        & (p["close"] > p["sma50"])
    )


def _sector_rotation_levels(r: dict) -> Levels:
    atr, close = r["atr14"], r["close"]
    return _levels(close - 0.5 * atr, close + 0.25 * atr, close - 2.0 * atr, atr)


# D5 ─ Heavy accumulation day without a gap: price strength built intraday.
def _volume_shock_mask(p: FeaturePanel) -> pd.DataFrame:
    move_floor = np.maximum(0.03, 1.5 * p["atr_pct_prev"])
    return (
        base_mask(p)
        & (p["vol_ratio"] >= 3.0)
        & (p["ret_1d"] >= move_floor)
        & (p["gap_pct"] < 0.02)
        & (p["close_loc"] >= 0.70)
    )


def _volume_shock_levels(r: dict) -> Levels:
    atr, close = r["atr14"], r["close"]
    return _levels(close - 0.5 * atr, close + 0.25 * atr, r["low"] - 0.25 * atr, atr)


# D6 (candidate) ─ Strong stock stretched 2σ below its 20-day mean; exit on reversion to the mean.
def _zscore_oversold_mask(p: FeaturePanel) -> pd.DataFrame:
    return (
        base_mask(p)
        & (p["rs_pct"] >= 0.80)
        & (p["close"] > p["sma200"])
        & (p["sma50"] > p["sma200"])
        & (p["zscore_20"] <= -2.0)
    )


def _zscore_oversold_levels(r: dict) -> Levels:
    atr, close = r["atr14"], r["close"]
    lo, hi = close - 0.5 * atr, close + 0.25 * atr
    lv = _levels(lo, hi, r["low_5"] - 0.5 * atr, atr, r1=1.5, r2=2.5)
    # "Back to 0": the 20-day mean is the natural first target when it is further out.
    t1 = max(lv.target1, round(r["sma20"], 2))
    return Levels(lv.entry_lo, lv.entry_hi, lv.stop, t1, max(lv.target2, t1))


DETECTORS: tuple[Detector, ...] = (
    Detector(
        "trend_breakout", "趋势突破", "setup", 20,
        "收盘突破 55 日高点，量能 ≥1.8 倍均量，50/200 日均线多头排列，相对强度前 20%。",
        _breakout_mask, _breakout_levels,
    ),
    Detector(
        "pullback_in_uptrend", "强势回调", "setup", 10,
        "相对强度前 20% 的上升趋势股，RSI(2) ≤ 10 的缩量回调，仍守在 50 日线附近。",
        _pullback_mask, _pullback_levels,
    ),
    Detector(
        "power_gap", "放量跳空", "setup", 20,
        "跳空 ≥ max(4%, 2×ATR%)，量能 ≥3 倍，收盘守住跳空（多为财报/重大事件驱动）。",
        _power_gap_mask, _power_gap_levels,
    ),
    Detector(
        "sector_rotation", "板块轮动龙头", "setup", 20,
        "行业 ETF 20 日相对 SPY 由负转正，选取行业内相对强度前 15% 的个股。",
        _sector_rotation_mask, _sector_rotation_levels,
    ),
    Detector(
        "volume_shock", "放量异动", "watch", 10,
        "无跳空的情况下当日放量 ≥3 倍并大涨，收在日内高位（盘中资金吸筹）。",
        _volume_shock_mask, _volume_shock_levels,
    ),
    Detector(
        "zscore_oversold", "z 分数超跌", "setup", 10,
        "相对强度前 20% 的上升趋势股，收盘低于 20 日均值 2 个标准差以上，目标回到均值。（候选规则，仅回放）",
        _zscore_oversold_mask, _zscore_oversold_levels, live=False,
    ),
)
