"""
新闻覆盖的标的池。

`cron.pre_fetch_news` 为这些 ticker 抓取新闻：
    核心（美东交易时段每 30 分钟）：17 只 ETF ∪ 每个行业成交额最大的 5 只股票 ∪ watchlist、活跃机会与持仓
    全量（工作日美东 05:30 和 16:30）：核心 ∪ Scout 扫描的全部流动性股票（约 900 只）

冷启动或数据库不可用时回落到默认 ETF。
"""
from __future__ import annotations

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

# 2 支宏观对冲
MACRO_HEDGE = [
    "GLD",   # Gold
    "TLT",   # 20+ yr Treasury
]

DEFAULT_ETF_UNIVERSE: list[str] = SPDR_SECTORS + BROAD_MARKET + MACRO_HEDGE

# Plain SQL keeps the news cron free of any scout imports.
_SCOUT_TICKERS_SQL = """
SELECT ticker FROM scout.watchlist WHERE active
UNION SELECT ticker FROM scout.opportunities WHERE status IN ('new', 'triggered')
UNION SELECT ticker FROM scout.trades WHERE closed_on IS NULL
UNION SELECT ticker FROM (
    SELECT f.ticker, row_number() OVER (
        PARTITION BY i.sector ORDER BY (f.payload->>'dollar_vol20')::float DESC NULLS LAST) AS rn
    FROM scout.features_daily f JOIN scout.instruments i USING (ticker)
    WHERE f.date = (SELECT max(date) FROM scout.features_daily) AND i.kind = 'stock' AND i.sector IS NOT NULL
) leaders WHERE rn <= 5
"""


# Full tier: every liquid stock Scout scans (latest snapshot), on top of the core set.
_SCOUT_SCAN_SQL = """
SELECT f.ticker FROM scout.features_daily f JOIN scout.instruments i USING (ticker)
WHERE f.date = (SELECT max(date) FROM scout.features_daily) AND i.kind = 'stock'
  AND (f.payload->>'dollar_vol20')::float >= 20000000
"""


async def resolve_universe(full: bool = False) -> list[str]:
    """Core: ETFs + holdings, open ideas, watchlist and sector leaders (~100).
    Full: core + every liquid stock Scout scans (~900)."""
    tickers: set[str] = set(DEFAULT_ETF_UNIVERSE)
    try:
        from sqlalchemy import text
        from db.session import AsyncSessionLocal
    except Exception:
        return sorted(tickers)
    try:
        async with AsyncSessionLocal() as db:
            for sql in (_SCOUT_TICKERS_SQL, _SCOUT_SCAN_SQL) if full else (_SCOUT_TICKERS_SQL,):
                rows = await db.execute(text(sql))
                tickers.update(str(r[0]).upper().strip() for r in rows if r[0])
    except Exception:
        pass  # scout schema not deployed yet / cold start
    return sorted(tickers)
