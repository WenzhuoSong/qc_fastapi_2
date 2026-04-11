"""
全局常量 —— ETF UNIVERSE 与市场分类。

Phase 1 只做 ETF 维度。UNIVERSE 的来源策略（Option C）：
    默认硬编码 17 个 ETF   ∪   最新 QC 快照里出现过的所有 ticker

这样可以保证：
  - 冷启动时有 17 只可跑（硬编码）
  - QC 实际发送不同 ETF 时自动扩展（动态 union）
  - 某个硬编码 ETF 临时从 QC 消失也不会漏掉（仍在硬编码里）
"""
from __future__ import annotations


# ═══════════════════════════════════════════════════════════════
# 默认 ETF UNIVERSE (17 支)
# ═══════════════════════════════════════════════════════════════

# 11 支 SPDR 行业 ETF
SPDR_SECTORS = [
    "XLK",   # Technology
    "XLF",   # Financials
    "XLV",   # Health Care
    "XLE",   # Energy
    "XLI",   # Industrials
    "XLP",   # Consumer Staples
    "XLY",   # Consumer Discretionary
    "XLU",   # Utilities
    "XLB",   # Materials
    "XLRE",  # Real Estate
    "XLC",   # Communication Services
]

# 3 支宽基
BROAD_MARKET = [
    "SPY",   # S&P 500
    "QQQ",   # Nasdaq-100
    "IWM",   # Russell 2000
]

# 3 支宏观对冲
MACRO_HEDGE = [
    "GLD",   # Gold
    "TLT",   # 20+ yr Treasury
    "HYG",   # HY Corporate Credit
]

DEFAULT_ETF_UNIVERSE: list[str] = SPDR_SECTORS + BROAD_MARKET + MACRO_HEDGE

# ═══════════════════════════════════════════════════════════════
# 风格分桶（供 market_brief 的 risk_on_score 计算）
# ═══════════════════════════════════════════════════════════════

RISK_ON_SECTORS  = {"XLK", "XLY", "XLC", "XLI"}
RISK_OFF_SECTORS = {"XLP", "XLU", "XLV", "XLRE"}

# 广基暴露限制（用于 risk_manager.broad_market_ok）
BROAD_MARKET_SET = set(BROAD_MARKET)


# ═══════════════════════════════════════════════════════════════
# UNIVERSE 解析
# ═══════════════════════════════════════════════════════════════

async def resolve_universe() -> list[str]:
    """
    返回当前的 ETF UNIVERSE = 默认硬编码 ∪ 最新 QC 快照里出现过的所有 ticker。

    冷启动（无快照）时回落到 DEFAULT_ETF_UNIVERSE。
    不读 CASH。
    """
    from db.session import AsyncSessionLocal
    from db.queries import get_latest_snapshots

    tickers: set[str] = set(DEFAULT_ETF_UNIVERSE)

    try:
        async with AsyncSessionLocal() as db:
            snaps = await get_latest_snapshots(db, limit=1)
            if snaps:
                payload = snaps[0].raw_payload or {}
                for h in payload.get("holdings", []) or []:
                    t = (h.get("ticker") or "").upper().strip()
                    if t and t != "CASH":
                        tickers.add(t)
    except Exception:
        pass  # 冷启动或 DB 未就绪时直接用默认

    return sorted(tickers)
