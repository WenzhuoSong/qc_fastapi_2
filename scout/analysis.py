"""'Scout 分析': a deterministic read of any ticker from its feature snapshot.

Shown on every ticker page, whether or not a rule fired. No LLM here: each
line is a plain reading of numbers already in `features_daily`. The optional
AI interpretation is generated on demand by the Q&A agent (see web.app).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from scout import knowledge
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


DRIVER_STOCK, DRIVER_MARKET, DRIVER_MIXED = "个股自身", "大盘/板块", "混合"


def attribution(f: dict) -> tuple[str, str] | None:
    """Who drove the last 20 sessions: the stock itself, the market, or both."""
    idio, mkt = f.get("idio_ret_20d"), f.get("market_ret_20d")
    if idio is None:
        return None
    if mkt is None and f.get("ret_20d") is not None:
        mkt = f["ret_20d"] - idio          # snapshots written before market_ret_20d existed
    if mkt is None:
        return None
    text = f"20 日：个股自身 {knowledge.fmt('idio_ret_20d', idio)}，大盘贡献 {knowledge.fmt('market_ret_20d', mkt)}"
    if abs(idio) >= 0.10 or (abs(idio) >= 0.03 and abs(idio) >= 2 * abs(mkt)):
        return DRIVER_STOCK, text + " → 个股自身主导"
    if abs(mkt) >= 0.03 and abs(mkt) >= 2 * abs(idio):
        return DRIVER_MARKET, text + " → 主要跟随大盘"
    return DRIVER_MIXED, text + " → 个股与大盘共同作用"


def _rs_text(f: dict) -> Line | None:
    rs = f.get("rs_pct")
    if rs is None:
        return None
    tone = "up" if rs >= 0.8 else "down" if rs <= 0.2 else ""
    text = f"全市场强于 {pct(rs, 0)} 的股票"
    if f.get("sector_rs_pct") is not None:
        text += f"，行业内强于 {pct(f['sector_rs_pct'], 0)}"
    return Line("相对强度", text, tone)


def _read(key: str, f: dict) -> str | None:
    """'名称 数值（读数）' straight from the feature dictionary."""
    if f.get(key) is None:
        return None
    r = knowledge.reading(key, f[key])
    return f"{knowledge.zh(key)} {knowledge.fmt(key, f[key])}" + (f"（{r}）" if r else "")


def _position(f: dict) -> Line | None:
    parts = [p for p in (_read("zscore_20", f), _read("dist_52w_high", f)) if p]
    z = f.get("zscore_20")
    tone = "warn" if z is not None and abs(z) >= 2 else ""
    return Line("位置", "；".join(parts), tone) if parts else None


def _structure(s: dict) -> Line | None:
    if not s or not s.get("swing_trend"):
        return None
    parts = [s["swing_trend"]]
    if s.get("resistance"):
        parts.append(f"阻力 {s['resistance']:.2f}（{knowledge.fmt('dist_resistance', s['dist_resistance'])}）")
    if s.get("support"):
        parts.append(f"支撑 {s['support']:.2f}（{knowledge.fmt('dist_support', s['dist_support'])}）")
    if s.get("updown_vol_20") is not None:
        parts.append(_read("updown_vol_20", s))
    trend = s["swing_trend"]
    tone = "down" if "转弱" in trend or trend.startswith("下降") else "up" if trend.startswith("上升") else ""
    return Line("价格结构", "；".join(parts), tone)


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
    text = _read("vol_ratio", f)
    if text is None:
        return None
    return Line("量能", text, "warn" if f["vol_ratio"] >= 1.8 else "")


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
    structure: dict | None = None,
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
    struct_line = _structure(structure or {})
    if struct_line:
        lines.insert(1, struct_line)
    attr = attribution(f)
    if attr:
        lines.insert(1, Line("归因", attr[1]))
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
