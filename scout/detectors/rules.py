"""Detector v1 rules. Parameters are fixed common-sense values, not fitted to
the replay — the base-rate table measures them, it does not tune them."""
from __future__ import annotations

from scout.detectors import Condition as C
from scout.detectors import Detector, Levels, Ref, r_targets

MIN_STOP_ATR = 1.0   # a stop inside one ATR of the entry is inside daily noise


def _levels(lo: float, hi: float, stop: float, atr: float, r1: float = 2.0, r2: float = 3.0) -> Levels:
    stop = min(stop, (lo + hi) / 2 - MIN_STOP_ATR * atr)
    return r_targets(lo, hi, stop, r1, r2)


# ── entry / stop / target levels ──────────────────────────────────────

def _breakout_levels(r: dict) -> Levels:
    atr, close = r["atr14"], r["close"]
    lo, hi = close - 0.25 * atr, close + 0.5 * atr
    return _levels(lo, hi, r["high_55_prior"] - atr, atr)


def _pullback_levels(r: dict) -> Levels:
    atr, close = r["atr14"], r["close"]
    lo, hi = close - 0.5 * atr, close + 0.25 * atr
    stop = min(r["low_5"], r["sma50"]) - 0.5 * atr
    lv = _levels(lo, hi, stop, atr, r1=1.5, r2=2.5)
    # The prior swing high is the natural first target when it is further out.
    t1 = max(lv.target1, round(r["high_20"], 2))
    return Levels(lv.entry_lo, lv.entry_hi, lv.stop, t1, max(lv.target2, t1))


def _power_gap_levels(r: dict) -> Levels:
    atr, close = r["atr14"], r["close"]
    return _levels(close - 0.5 * atr, close + 0.5 * atr, r["low"] - 0.25 * atr, atr)


def _sector_rotation_levels(r: dict) -> Levels:
    atr, close = r["atr14"], r["close"]
    return _levels(close - 0.5 * atr, close + 0.25 * atr, close - 2.0 * atr, atr)


def _volume_shock_levels(r: dict) -> Levels:
    atr, close = r["atr14"], r["close"]
    return _levels(close - 0.5 * atr, close + 0.25 * atr, r["low"] - 0.25 * atr, atr)


def _zscore_oversold_levels(r: dict) -> Levels:
    atr, close = r["atr14"], r["close"]
    lo, hi = close - 0.5 * atr, close + 0.25 * atr
    lv = _levels(lo, hi, r["low_5"] - 0.5 * atr, atr, r1=1.5, r2=2.5)
    # "Back to 0": the 20-day mean is the natural first target when it is further out.
    t1 = max(lv.target1, round(r["sma20"], 2))
    return Levels(lv.entry_lo, lv.entry_hi, lv.stop, t1, max(lv.target2, t1))


def _breakout_retest_levels(r: dict) -> Levels:
    """Edwards & Magee: after a breakout, prior resistance should act as support.
    Wait for price to come back to the breakout level instead of chasing."""
    atr, pivot = r["atr14"], r["high_55_prior"]
    return _levels(pivot - 0.25 * atr, pivot + 0.25 * atr, pivot - 1.25 * atr, atr)


# ── rules: declarative conditions (scan, replay and "why it was picked" share them) ──

D1_BREAKOUT = (
    C("收盘突破 55 日高点", "close", ">", "high_55_prior"),
    C("量能 ≥ 1.8 倍均量", "vol_ratio", ">=", 1.8),
    C("收盘在 50 日线上方", "close", ">", "sma50"),
    C("50 日线在 200 日线上方", "sma50", ">", "sma200"),
    C("相对强度前 20%", "rs_pct", ">=", 0.80),
    C("收在日内高位", "close_loc", ">=", 0.60),
    C("不是跳空（跳空 < 4%）", "gap_pct", "<", 0.04),
)
D2_PULLBACK = (
    C("相对强度前 20%", "rs_pct", ">=", 0.80),
    C("收盘在 200 日线上方", "close", ">", "sma200"),
    C("50 日线在 200 日线上方", "sma50", ">", "sma200"),
    C("3 个月涨幅 > 10%", "ret_63d", ">", 0.10),
    C("RSI(2) ≤ 10（极度超卖）", "rsi2", "<=", 10),
    C("守住 50 日线（不低于 97%）", "close", ">=", Ref("sma50", 0.97)),
    C("回落到 20 日线下方", "close", "<=", "sma20"),
    C("缩量回调（量比 ≤ 1.2）", "vol_ratio", "<=", 1.2),
)
D3_POWER_GAP = (
    C("跳空 ≥ max(4%, 2×前日 ATR%)", "gap_pct", ">=", Ref("atr_pct_prev", 2.0, floor=0.04)),
    C("量能 ≥ 3 倍均量", "vol_ratio", ">=", 3.0),
    C("收在日内中上部", "close_loc", ">=", 0.50),
    C("收盘不低于开盘（守住跳空）", "close", ">=", "open"),
)
D4_SECTOR_ROTATION = (
    C("所属行业 20 日跑赢 SPY", "sector_rel_20", ">", 0.0),
    C("5 天前行业还在跑输（刚转强）", "sector_rel_20_prev5", "<=", 0.0),
    C("行业 3 个月未明显跑输（> -5%）", "sector_rel_63", ">", -0.05),
    C("行业内相对强度前 15%", "sector_rs_pct", ">=", 0.85),
    C("全市场相对强度前 40%", "rs_pct", ">=", 0.60),
    C("收盘在 50 日线上方", "close", ">", "sma50"),
)
D5_VOLUME_SHOCK = (
    C("量能 ≥ 3 倍均量", "vol_ratio", ">=", 3.0),
    C("涨幅 ≥ max(3%, 1.5×前日 ATR%)", "ret_1d", ">=", Ref("atr_pct_prev", 1.5, floor=0.03)),
    C("不是跳空（跳空 < 2%）", "gap_pct", "<", 0.02),
    C("收在日内高位", "close_loc", ">=", 0.70),
)
D6_ZSCORE = (
    C("相对强度前 20%", "rs_pct", ">=", 0.80),
    C("收盘在 200 日线上方", "close", ">", "sma200"),
    C("50 日线在 200 日线上方", "sma50", ">", "sma200"),
    C("低于 20 日均值 2σ 以上", "zscore_20", "<=", -2.0),
)


DETECTORS: tuple[Detector, ...] = (
    Detector(
        "trend_breakout", "趋势突破", "setup", 20,
        "收盘突破 55 日高点，量能 ≥1.8 倍均量，50/200 日均线多头排列，相对强度前 20%。",
        D1_BREAKOUT, _breakout_levels,
    ),
    Detector(
        "pullback_in_uptrend", "强势回调", "setup", 10,
        "相对强度前 20% 的上升趋势股，RSI(2) ≤ 10 的缩量回调，仍守在 50 日线附近。",
        D2_PULLBACK, _pullback_levels,
    ),
    Detector(
        "power_gap", "放量跳空", "setup", 20,
        "跳空 ≥ max(4%, 2×ATR%)，量能 ≥3 倍，收盘守住跳空（多为财报/重大事件驱动）。",
        D3_POWER_GAP, _power_gap_levels,
    ),
    Detector(
        "sector_rotation", "板块轮动龙头", "setup", 20,
        "行业 ETF 20 日相对 SPY 由负转正，选取行业内相对强度前 15% 的个股。",
        D4_SECTOR_ROTATION, _sector_rotation_levels,
    ),
    Detector(
        "volume_shock", "放量异动", "watch", 10,
        "无跳空的情况下当日放量 ≥3 倍并大涨，收在日内高位（盘中资金吸筹）。",
        D5_VOLUME_SHOCK, _volume_shock_levels,
    ),
    Detector(
        "zscore_oversold", "z 分数超跌", "setup", 10,
        "相对强度前 20% 的上升趋势股，收盘低于 20 日均值 2 个标准差以上，目标回到均值。（候选规则，仅回放）",
        D6_ZSCORE, _zscore_oversold_levels, live=False,
    ),
    Detector(
        "breakout_retest", "突破回踩", "setup", 20,
        "与趋势突破相同的触发条件，但不追价：等待 10 个交易日内回踩突破位（前 55 日高点 ± 0.25 ATR）再入场，"
        "止损放在突破位下方。（候选规则，仅回放）",
        D1_BREAKOUT, _breakout_retest_levels, live=False, trigger_window=10,
    ),
)
