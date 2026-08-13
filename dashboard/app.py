"""Minimal authenticated dashboard for the QC observer fact plane."""
from __future__ import annotations

import os
import secrets
from html import escape
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy import desc, select

from db.models import (
    QCAccountSnapshotHoldingProjection,
    QCAccountSnapshotProjection,
    QCDecisionManifestProjection,
)
from db.session import AsyncSessionLocal
from services.execution_authority import execution_authority_status
from services.qc_observer_health import load_current_observer_summary
from services.qc_observer_projections import load_observer_projection_status
from audit.service import load_latest_audit_status


app = FastAPI(title="noble-flow monitor", version="2.0.0-minimal")
security = HTTPBasic()


def require_auth(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    user = os.getenv("DASHBOARD_USER")
    password = os.getenv("DASHBOARD_PASSWORD")
    if not user or not password:
        raise HTTPException(status_code=503, detail="Dashboard authentication is not configured")
    valid = secrets.compare_digest(credentials.username, user) and secrets.compare_digest(
        credentials.password, password
    )
    if not valid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid dashboard credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "role": "qc_observer_dashboard"}


async def build_monitor() -> dict[str, Any]:
    async with AsyncSessionLocal() as db:
        observer = await load_current_observer_summary(db)
        projection = await load_observer_projection_status(db)
        audit = await load_latest_audit_status(db)
        stream = observer.get("event_stream_id")
        snapshot = None
        decision = None
        holdings: list[Any] = []
        if stream:
            snapshot = await db.scalar(
                select(QCAccountSnapshotProjection)
                .where(QCAccountSnapshotProjection.event_stream_id == stream)
                .order_by(desc(QCAccountSnapshotProjection.event_time))
                .limit(1)
            )
            decision = await db.scalar(
                select(QCDecisionManifestProjection)
                .where(QCDecisionManifestProjection.event_stream_id == stream)
                .order_by(desc(QCDecisionManifestProjection.event_time))
                .limit(1)
            )
            if snapshot:
                holdings = list(
                    (
                        await db.scalars(
                            select(QCAccountSnapshotHoldingProjection)
                            .where(QCAccountSnapshotHoldingProjection.account_snapshot_id == snapshot.id)
                            .order_by(desc(QCAccountSnapshotHoldingProjection.holdings_value))
                        )
                    ).all()
                )
        equity = float(snapshot.equity) if snapshot else None
        final_targets = dict(decision.final_target_weights or {}) if decision else {}
        return {
            "available": bool(observer.get("available")),
            "status": observer.get("status"),
            "execution_fence": execution_authority_status(),
            "observer": observer,
            "projection": projection,
            "audit": audit,
            "account": None
            if snapshot is None
            else {
                "as_of": snapshot.event_time.isoformat(),
                "equity": equity,
                "cash": float(snapshot.cash),
                "cash_pct": float(snapshot.cash) / equity if equity else None,
                "pending_orders": snapshot.pending_order_count,
                "deferred_targets": snapshot.deferred_target_count,
                "broker_reconciled": snapshot.broker_reconciled,
                "holdings": [
                    {
                        "ticker": row.ticker,
                        "quantity": float(row.quantity),
                        "value": float(row.holdings_value or 0),
                        "weight": float(row.holdings_value or 0) / equity if equity else None,
                    }
                    for row in holdings
                ],
            },
            "latest_decision": None
            if decision is None
            else {
                "as_of": decision.event_time.isoformat(),
                "decision_id": decision.decision_id,
                "execution_style": decision.execution_style,
                "final_target_weights": final_targets,
                "target_exposure": sum(float(value) for value in final_targets.values()),
                "suppressed_deltas": decision.suppressed_deltas or {},
            },
        }


@app.get("/api/observer-monitor")
async def observer_monitor(_: str = Depends(require_auth)) -> dict[str, Any]:
    return await build_monitor()


@app.get("/", response_class=HTMLResponse)
async def home(_: str = Depends(require_auth)) -> str:
    data = await build_monitor()
    account = data.get("account") or {}
    decision = data.get("latest_decision") or {}
    audit = data.get("audit") or {}
    decision_audit = audit.get("latest_decision") or {}
    holdings = account.get("holdings") or []
    rows = "".join(
        f"<tr><td>{escape(str(row['ticker']))}</td><td>{row['quantity']:.2f}</td>"
        f"<td>{row['weight']:.2%}</td><td>${row['value']:,.2f}</td></tr>"
        for row in holdings
    )
    return f"""<!doctype html><html><head><meta charset='utf-8'><meta http-equiv='refresh' content='60'>
    <title>noble-flow</title><style>
    body{{font:14px system-ui;margin:32px;max-width:1050px;color:#17202a}}h1{{margin-bottom:4px}}
    .ok{{color:#087830}}.grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:20px 0}}
    .box{{border:1px solid #d8dee4;padding:14px;border-radius:6px}}table{{border-collapse:collapse;width:100%}}
    th,td{{text-align:right;padding:8px;border-bottom:1px solid #e7eaee}}th:first-child,td:first-child{{text-align:left}}
    code{{font-size:12px}}</style></head><body>
    <h1>noble-flow</h1><div>QC observer-only · execution authority: none</div>
    <div class='grid'><div class='box'>Status<br><b class='ok'>{escape(str(data.get('status')))}</b></div>
    <div class='box'>Equity<br><b>${account.get('equity', 0):,.2f}</b></div>
    <div class='box'>Cash<br><b>{account.get('cash_pct', 0):.2%}</b></div>
    <div class='box'>Target exposure<br><b>{decision.get('target_exposure', 0):.2%}</b></div></div>
    <div class='grid'><div class='box'>Audit<br><b>{escape(str(audit.get('status', 'pending')))}</b></div>
    <div class='box'>Actual exposure<br><b>{(decision_audit.get('actual_exposure') or 0):.2%}</b></div>
    <div class='box'>Tracking error L1<br><b>{(decision_audit.get('tracking_error_l1') or 0):.2%}</b></div>
    <div class='box'>Orders terminal<br><b>{decision_audit.get('terminal_intent_count', 0)}/{decision_audit.get('intent_count', 0)}</b></div></div>
    <h2>Holdings</h2><table><thead><tr><th>Ticker</th><th>Qty</th><th>Weight</th><th>Value</th></tr></thead>
    <tbody>{rows}</tbody></table><h2>Latest decision</h2><code>{escape(str(decision.get('decision_id', 'none')))}</code>
    <p>Account as of {escape(str(account.get('as_of', 'n/a')))} · refreshes every 60 seconds</p></body></html>"""
