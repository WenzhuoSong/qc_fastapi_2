import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

# This module imports the cron's real SQL statements. Supply inert settings so
# collection never depends on a developer's local .env file.
os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("WEBHOOK_SECRET", "test")
os.environ.setdefault("QC_USER_ID", "test")
os.environ.setdefault("QC_API_TOKEN", "test")
os.environ.setdefault("QC_PROJECT_ID", "test")
os.environ.setdefault("TG_BOT_TOKEN", "test")
os.environ.setdefault("TG_CHAT_ID", "test")

from cron.pre_fetch_news import _cleanup_old_news
from db.models import TickerNewsArchive


def test_ticker_news_archive_is_append_only_research_shape():
    columns = TickerNewsArchive.__table__.columns
    required = {
        "source_library_id",
        "ticker",
        "url",
        "headline",
        "source_api",
        "sentiment",
        "relevance",
        "datetime_utc",
        "source_created_at",
        "archived_at",
    }
    assert required.issubset(columns.keys())


def test_ticker_news_archive_deduplicates_ticker_url():
    constraints = {
        constraint.name
        for constraint in TickerNewsArchive.__table__.constraints
        if constraint.name
    }
    assert "uq_ticker_news_archive_url" in constraints


class _ScalarResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return self

    def all(self):
        return self._rows


class _SessionContext:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_cleanup_archives_expired_rows_before_deleting():
    expired = SimpleNamespace(
        id=42,
        ticker="AAPL",
        url="https://example.test/aapl",
        headline="Example",
        source="Example Source",
        source_api="rss",
        summary="raw",
        llm_summary="summary",
        sentiment="neutral",
        relevance="direct",
        is_hard_event=False,
        hard_risks=None,
        category="company",
        related=["AAPL"],
        datetime_utc=1,
        credibility=75,
        created_at=None,
    )
    delete_result = SimpleNamespace(rowcount=1)
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=[_ScalarResult([expired]), object(), delete_result]),
        commit=AsyncMock(),
    )

    with patch(
        "cron.pre_fetch_news.AsyncSessionLocal",
        return_value=_SessionContext(session),
    ):
        deleted = await _cleanup_old_news()

    assert deleted == 1
    assert session.execute.await_count == 3
    archive_statement = session.execute.await_args_list[1].args[0]
    assert archive_statement.table.name == "ticker_news_archive"
    assert archive_statement._post_values_clause is not None
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_cleanup_skips_archive_insert_when_nothing_expired():
    delete_result = SimpleNamespace(rowcount=0)
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=[_ScalarResult([]), delete_result]),
        commit=AsyncMock(),
    )

    with patch(
        "cron.pre_fetch_news.AsyncSessionLocal",
        return_value=_SessionContext(session),
    ):
        deleted = await _cleanup_old_news()

    assert deleted == 0
    assert session.execute.await_count == 2
    session.commit.assert_awaited_once()
