from contextlib import asynccontextmanager

import pytest

from cron import morning_health


class _Audit:
    def __init__(self) -> None:
        self.summary = {}

    def set_summary(self, **kwargs) -> None:
        self.summary.update(kwargs)


class _SessionContext:
    def __init__(self, session) -> None:
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


@pytest.mark.asyncio
async def test_morning_health_passes_database_session_to_projection(monkeypatch):
    audit = _Audit()
    session = object()
    sent = []

    @asynccontextmanager
    async def fake_audit_cron_run(job_name):
        assert job_name == "morning_health"
        yield audit

    async def fake_projection(received_session):
        assert received_session is session
        return {"pending_parent_count": 0, "checkpoint_status": "healthy"}

    async def fake_observer():
        return {"status": "healthy"}

    async def fake_send(payload):
        sent.append(payload)

    monkeypatch.setattr(morning_health, "audit_cron_run", fake_audit_cron_run)
    monkeypatch.setattr(morning_health, "AsyncSessionLocal", lambda: _SessionContext(session))
    monkeypatch.setattr(morning_health, "load_current_observer_summary", fake_observer)
    monkeypatch.setattr(morning_health, "load_observer_projection_status", fake_projection)
    monkeypatch.setattr(morning_health, "format_current_observer_report", lambda _: "Observer healthy")
    monkeypatch.setattr(morning_health, "tool_send_telegram", fake_send)

    await morning_health.main()

    assert sent and "Projection pending: 0" in sent[0]["text"]
    assert audit.summary == {
        "observer_status": "healthy",
        "projection_status": "healthy",
        "projection_pending": 0,
        "execution_authority": "none",
    }
