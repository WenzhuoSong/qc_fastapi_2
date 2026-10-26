"""'Scout 分析': a deterministic read of any ticker from its feature snapshot.

Shown on every ticker page, whether or not a rule fired. No LLM here: each
line is a plain reading of numbers already in `features_daily`. The optional
AI interpretation is generated on demand by the Q&A agent (see web.app).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from scout.telegram import market_split_text, pct


@dataclass
class Line:
    label: str
    text: str
    tone: str = ""        # up | down | warn | "" (neutral)


def trend_state(f: dict) -> tuple[str, str]:
    c, s50, s200 = f.get("close"), f.get("sma50"), f.get("sma200")
    if None in (c, s50, s200):
        return "数据不足", ""
    if c > s50 > s200:
        return "上升趋势（价格 > 50 日线 > 200 日线）", "up"
    if c < s50 < s200:
        return "下降趋势（价格 < 50 日线 < 200 日线）", "down"
    if c > s200:
        return "长期向上、短期整理（在 200 日线上方）", ""
    return "长期偏弱（在 200 日线下方）", "warn"


def _rs_text(f: dict) -> Line | None:
    rs = f.get("rs_pct")
    if rs is None:
        return None
    tone = "up" if rs >= 0.8 else "down" if rs <= 0.2 else ""
    text = f"全市场强于 {pct(rs, 0)} 的股票"
    if f.get("sector_rs_pct") is not None:
        text += f"，行业内强于 {pct(f['sector_rs_pct'], 0)}"
    return Line("相对强度", text, tone)


def _position(f: dict) -> Line | None:
    z = f.get("zscore_20")
    parts, tone = [], ""
    if z is not None:
        if z >= 2:
            parts.append(f"高于 20 日均值 {z:.1f} 个标准差，短期过热")
            tone = "warn"
        elif z <= -2:
            parts.append(f"低于 20 日均值 {abs(z):.1f} 个标准差，短期超跌")
            tone = "warn"
        else:
            parts.append(f"距 20 日均值 {z:+.1f} 个标准差")
    if f.get("dist_52w_high") is not None:
        parts.append(f"距 52 周高点 {pct(f['dist_52w_high'], 1, True)}")
    return Line("位置", "，".join(parts), tone) if parts else None


def _risk(f: dict) -> Line | None:
    parts = []
    if f.get("beta_60") is not None:
        parts.append(f"β {f['beta_60']:.2f}")
    if f.get("vol_20") is not None:
        parts.append(f"年化波动 {pct(f['vol_20'], 0)}")
    if f.get("max_dd_60") is not None:
        parts.append(f"60 日最大回撤 {pct(f['max_dd_60'], 1)}")
    if f.get("atr_pct") is not None:
        parts.append(f"日均振幅 {pct(f['atr_pct'], 1)}")
    tone = "warn" if (f.get("vol_20") or 0) > 0.6 or (f.get("max_dd_60") or 0) < -0.25 else ""
    return Line("风险", " · ".join(parts), tone) if parts else None


def _volume(f: dict) -> Line | None:
    vr = f.get("vol_ratio")
    if vr is None:
        return None
    if vr >= 1.8:
        return Line("量能", f"今日成交量是 50 日均量的 {vr:.1f} 倍，明显放量", "warn")
    if vr <= 0.7:
        return Line("量能", f"今日成交量只有 50 日均量的 {vr:.1f} 倍，缩量")
    return Line("量能", f"成交量为 50 日均量的 {vr:.1f} 倍，正常")


def _near_rules(f: dict) -> list[str]:
    """Plain distance-to-trigger readings for rules whose inputs are in the snapshot."""
    out = []
    strong_uptrend = (f.get("rs_pct") or 0) >= 0.8 and (f.get("close") or 0) > (f.get("sma200") or 1e18)
    if strong_uptrend and f.get("rsi2") is not None and f["rsi2"] > 10:
        if f["rsi2"] <= 30:
            out.append(f"接近「强势回调」：RSI(2) = {f['rsi2']:.0f}，降到 10 以下才会触发")
    if strong_uptrend and f.get("zscore_20") is not None and -2 < f["zscore_20"] <= -1.2:
        out.append(f"接近「z 分数超跌」（候选规则）：z = {f['zscore_20']:.1f}，需 ≤ -2")
    return out


def scout_view(
    f: dict,
    *,
    is_stock: bool,
    next_earnings: date | None,
    latest_opp: dict | None,
    today: date,
) -> tuple[str, list[Line]]:
    """Returns (headline, lines)."""
    if not f:
        return "还没有特征数据", []
    trend, tone = trend_state(f)
    lines = [Line("趋势", trend, tone)]
    for line in (_rs_text(f) if is_stock else None, _position(f), _risk(f), _volume(f)):
        if line:
            lines.append(line)
    split = market_split_text(f)
    if split:
        lines.insert(1, Line("大盘 vs 个股", split))
    if next_earnings:
        days = (next_earnings - today).days
        lines.append(Line("事件", f"下次财报 {next_earnings}（{days} 天后）", "warn" if 0 <= days <= 7 else ""))
    if latest_opp and latest_opp.get("status") in ("new", "triggered"):
        label = (latest_opp.get("evidence") or {}).get("setup", {}).get("label", latest_opp["primary_detector"])
        lines.append(Line("规则", f"有活跃机会卡：{label}（{latest_opp['asof']}，评分 {latest_opp['score']:.2f}）", "up"))
    else:
        near = _near_rules(f) if is_stock else []
        lines.append(Line("规则", "；".join(near) if near else "当前没有规则触发，也不接近触发条件"))

    rs = f.get("rs_pct")
    if tone == "up" and is_stock and rs is not None and rs >= 0.8:
        headline = "强势股：上升趋势且相对强度领先"
    elif tone == "up":
        headline = "处于上升趋势"
    elif tone == "down":
        headline = "处于下降趋势，规则不做多"
    else:
        headline = "趋势不明确，观察为主"
    return headline, lines
