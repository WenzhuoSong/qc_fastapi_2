"""
新闻覆盖的标的池。

`cron.pre_fetch_news` 每 2 小时为这些 ticker 抓取新闻：
    默认 17 只 ETF  ∪  Scout watchlist 与活跃机会  ∪  QC 观察器记录的最新实际持仓

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

# Plain SQL keeps the observer/news code free of any scout imports.
_SCOUT_TICKERS_SQL = """
SELECT ticker FROM scout.watchlist WHERE active
UNION SELECT ticker FROM scout.opportunities WHERE status IN ('new', 'triggered')
"""
_QC_HOLDINGS_SQL = """
SELECT h.ticker FROM qc_account_snapshot_holdings h
WHERE h.account_snapshot_id = (SELECT id FROM qc_account_snapshots ORDER BY event_time DESC LIMIT 1)
  AND h.ticker IS NOT NULL AND h.quantity <> 0
"""


async def resolve_universe() -> list[str]:
    tickers: set[str] = set(DEFAULT_ETF_UNIVERSE)
    try:
        from sqlalchemy import text
        from db.session import AsyncSessionLocal
    except Exception:
        return sorted(tickers)
    for sql in (_SCOUT_TICKERS_SQL, _QC_HOLDINGS_SQL):
        try:
            async with AsyncSessionLocal() as db:
                rows = await db.execute(text(sql))
                tickers.update(str(r[0]).upper().strip() for r in rows if r[0])
        except Exception:
            pass  # schema not deployed yet / cold start
    tickers.discard("CASH")
    return sorted(tickers)
