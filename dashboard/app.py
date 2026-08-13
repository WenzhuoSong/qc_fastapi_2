"""Minimal authenticated dashboard for the QC observer fact plane."""
from __future__ import annotations

import os
import secrets
import json
from html import escape
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy import desc, select

from db.models import (
    QCAccountSnapshotHoldingProjection,
    QCAccountSnapshotProjection,
    QCAuditRun,
    QCDecisionExecutionAudit,
    QCDecisionManifestProjection,
    QCDecisionRankingProjection,
)
from db.session import AsyncSessionLocal
from services.execution_authority import execution_authority_status
from services.qc_observer_health import load_current_observer_summary
from services.qc_observer_projections import load_observer_projection_status
from audit.service import load_latest_audit_status
from dashboard.view_models import daily_account_series, momentum_view, weekly_rebalance_logs


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
        account_history: list[dict[str, Any]] = []
        rebalance_logs: list[dict[str, Any]] = []
        momentum: dict[str, Any] = {}
        if stream:
            snapshots = list((await db.scalars(
                select(QCAccountSnapshotProjection)
                .where(QCAccountSnapshotProjection.event_stream_id == stream)
                .order_by(desc(QCAccountSnapshotProjection.event_time))
                .limit(400)
            )).all())
            snapshot = snapshots[0] if snapshots else None
            decisions = list((await db.scalars(
                select(QCDecisionManifestProjection)
                .where(QCDecisionManifestProjection.event_stream_id == stream)
                .order_by(desc(QCDecisionManifestProjection.event_time))
                .limit(12)
            )).all())
            decision = decisions[0] if decisions else None
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
            account_history = daily_account_series(snapshots)
            manifest_ids = [row.id for row in decisions]
            rankings_by_manifest: dict[int, list[Any]] = {}
            if manifest_ids:
                ranking_rows = list((await db.scalars(
                    select(QCDecisionRankingProjection)
                    .where(QCDecisionRankingProjection.decision_manifest_id.in_(manifest_ids))
                    .order_by(QCDecisionRankingProjection.rank)
                )).all())
                for row in ranking_rows:
                    rankings_by_manifest.setdefault(row.decision_manifest_id, []).append(row)
                latest_run = await db.scalar(
                    select(QCAuditRun)
                    .where(QCAuditRun.status == "completed")
                    .order_by(desc(QCAuditRun.id))
                    .limit(1)
                )
                audits_by_manifest: dict[int, Any] = {}
                if latest_run is not None:
                    audit_rows = list((await db.scalars(
                        select(QCDecisionExecutionAudit).where(
                            QCDecisionExecutionAudit.audit_run_id == latest_run.id,
                            QCDecisionExecutionAudit.decision_manifest_id.in_(manifest_ids),
                        )
                    )).all())
                    audits_by_manifest = {row.decision_manifest_id: row for row in audit_rows}
                rebalance_logs = weekly_rebalance_logs(decisions, rankings_by_manifest, audits_by_manifest)
                latest_rankings = rankings_by_manifest.get(decisions[0].id, [])
                prior_rankings = rankings_by_manifest.get(decisions[1].id, []) if len(decisions) > 1 else []
                momentum = momentum_view(
                    latest_rankings,
                    prior_rankings,
                    [str(row.ticker) for row in holdings if row.ticker],
                    boundary_gap=(decisions[0].payload_extra or {}).get("rank_boundary_gap_3_4"),
                )
                momentum["as_of"] = decisions[0].event_time.isoformat()
        equity = float(snapshot.equity) if snapshot else None
        final_targets = dict(decision.final_target_weights or {}) if decision else {}
        return {
            "available": bool(observer.get("available")),
            "status": observer.get("status"),
            "execution_fence": execution_authority_status(),
            "observer": observer,
            "projection": projection,
            "audit": audit,
            "account_history": account_history,
            "weekly_rebalances": rebalance_logs,
            "momentum": momentum,
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
    history = data.get("account_history") or []
    rebalances = data.get("weekly_rebalances") or []
    momentum = data.get("momentum") or {}
    momentum_rows = momentum.get("holdings") or []
    holdings = account.get("holdings") or []
    rows = "".join(
        f"<tr><td>{escape(str(row['ticker']))}</td><td>{row['quantity']:.2f}</td>"
        f"<td>{row['weight']:.2%}</td><td>${row['value']:,.2f}</td></tr>"
        for row in holdings
    )
    momentum_table = ""
    for row in momentum_rows:
        score = f"{row['score']:.3f}" if row["score"] is not None else "n/a"
        rank = f"#{row['rank']}" if row["rank"] is not None else "n/a"
        rank_delta = f"{row['rank_change']:+d}" if row["rank_change"] is not None else "n/a"
        score_delta = f"{row['score_change']:+.3f}" if row["score_change"] is not None else "n/a"
        momentum_table += (
            f"<tr><td>{escape(str(row['ticker']))}</td><td>{rank}</td><td>{score}</td>"
            f"<td>{rank_delta}</td><td>{score_delta}</td></tr>"
        )
    rebalance_html = "".join(
        "<article class='decision'>"
        f"<header><b>{escape(str(item.get('session') or item['as_of'][:10]))}</b>"
        f"<span>{item['target_exposure']:.1%} target · {escape(item['audit_status'])}</span></header>"
        + "".join(f"<p>{escape(reason)}</p>" for reason in item["reasons"])
        + (f"<p><b>新建：</b>{escape(', '.join(item['opened']))}</p>" if item["opened"] else "")
        + (f"<p><b>退出：</b>{escape(', '.join(item['removed']))}</p>" if item["removed"] else "")
        + "</article>"
        for item in rebalances
    )
    chart_data = json.dumps(history, separators=(",", ":")).replace("</", "<\\/")
    return f"""<!doctype html><html><head><meta charset='utf-8'><meta http-equiv='refresh' content='60'>
    <title>noble-flow</title><style>
    *{{box-sizing:border-box}}body{{font:14px system-ui;margin:32px auto;padding:0 20px;max-width:1120px;color:#17202a}}
    h1{{margin-bottom:4px}}h2{{margin:32px 0 12px}}.muted{{color:#66707a}}.ok{{color:#087830}}
    .grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:20px 0}}
    .box{{border:1px solid #d8dee4;padding:14px;border-radius:6px;min-height:68px}}
    .chart{{width:100%;height:300px;border-top:1px solid #e7eaee;border-bottom:1px solid #e7eaee}}
    canvas{{width:100%;height:100%;display:block}}table{{border-collapse:collapse;width:100%}}
    th,td{{text-align:right;padding:8px;border-bottom:1px solid #e7eaee}}th:first-child,td:first-child{{text-align:left}}
    .decision{{padding:14px 0;border-bottom:1px solid #e7eaee}}.decision header{{display:flex;justify-content:space-between;gap:16px}}
    .decision p{{margin:7px 0;color:#3b4650}}code{{font-size:12px}}@media(max-width:760px){{.grid{{grid-template-columns:repeat(2,1fr)}}body{{margin-top:20px}}}}
    </style></head><body>
    <h1>noble-flow</h1><div>QC observer-only · execution authority: none</div>
    <div class='grid'><div class='box'>Status<br><b class='ok'>{escape(str(data.get('status')))}</b></div>
    <div class='box'>Equity<br><b>${account.get('equity', 0):,.2f}</b></div>
    <div class='box'>Cash<br><b>{account.get('cash_pct', 0):.2%}</b></div>
    <div class='box'>Target exposure<br><b>{decision.get('target_exposure', 0):.2%}</b></div></div>
    <div class='grid'><div class='box'>Audit<br><b>{escape(str(audit.get('status', 'pending')))}</b></div>
    <div class='box'>Actual exposure<br><b>{(decision_audit.get('actual_exposure') or 0):.2%}</b></div>
    <div class='box'>Tracking error L1<br><b>{(decision_audit.get('tracking_error_l1') or 0):.2%}</b></div>
    <div class='box'>Orders terminal<br><b>{decision_audit.get('terminal_intent_count', 0)}/{decision_audit.get('intent_count', 0)}</b></div></div>
    <h2>账户历史</h2><p class='muted'>每日最后一份 broker-reconciled snapshot；蓝线为权益，灰柱为现金比例。</p>
    <div class='chart'><canvas id='accountChart'></canvas></div>
    <h2>当前持仓</h2><table><thead><tr><th>Ticker</th><th>Qty</th><th>Weight</th><th>Value</th></tr></thead>
    <tbody>{rows}</tbody></table>
    <h2>持仓动量</h2><p>{escape(str(momentum.get('interpretation', '等待周度 ranking 数据。')))}</p>
    <p class='muted'>这是 stronger252 周度 score/rank 的状态描述，不是日内行情预测。Universe 正分数比例：{(momentum.get('positive_score_pct') or 0):.1%}；rank 3/4 边界差：{(momentum.get('rank_boundary_gap_3_4') or 0):.3f}。</p>
    <table><thead><tr><th>Ticker</th><th>Rank</th><th>Score</th><th>Rank Δ</th><th>Score Δ</th></tr></thead><tbody>{momentum_table}</tbody></table>
    <h2>每周调仓日志</h2>{rebalance_html or '<p class="muted">等待下一份 decision manifest。</p>'}
    <h2>Latest decision</h2><code>{escape(str(decision.get('decision_id', 'none')))}</code>
    <p>Account as of {escape(str(account.get('as_of', 'n/a')))} · refreshes every 60 seconds</p>
    <script>
    const points={chart_data};
    function draw(){{const c=document.getElementById('accountChart'),r=c.getBoundingClientRect(),d=window.devicePixelRatio||1;
      c.width=Math.max(1,Math.floor(r.width*d));c.height=Math.max(1,Math.floor(r.height*d));const x=c.getContext('2d');x.scale(d,d);
      const w=r.width,h=r.height,p={{l:58,r:44,t:22,b:34}};x.clearRect(0,0,w,h);if(!points.length)return;
      const values=points.map(v=>v.equity),lo=Math.min(...values)*0.995,hi=Math.max(...values)*1.005||1;
      const px=i=>p.l+(w-p.l-p.r)*(points.length===1?.5:i/(points.length-1));const py=v=>p.t+(h-p.t-p.b)*(1-(v-lo)/(hi-lo||1));
      x.strokeStyle='#dfe4e8';x.lineWidth=1;for(let i=0;i<4;i++){{const y=p.t+(h-p.t-p.b)*i/3;x.beginPath();x.moveTo(p.l,y);x.lineTo(w-p.r,y);x.stroke();}}
      const bw=Math.max(3,(w-p.l-p.r)/points.length*.42);x.fillStyle='#c8ced5';points.forEach((v,i)=>{{const bh=(h-p.t-p.b)*(v.cash_pct||0);x.fillRect(px(i)-bw/2,h-p.b-bh,bw,bh);}});
      x.strokeStyle='#1769aa';x.lineWidth=2;x.beginPath();points.forEach((v,i)=>{{const a=px(i),b=py(v.equity);i?x.lineTo(a,b):x.moveTo(a,b);}});x.stroke();
      x.fillStyle='#4c5965';x.font='12px system-ui';x.textAlign='left';x.fillText('$'+Math.round(hi).toLocaleString(),4,p.t+4);x.fillText('$'+Math.round(lo).toLocaleString(),4,h-p.b);
      x.textAlign='center';const step=Math.max(1,Math.ceil(points.length/6));points.forEach((v,i)=>{{if(i%step===0||i===points.length-1)x.fillText(v.date.slice(5),px(i),h-10);}});
    }}draw();window.addEventListener('resize',draw);
    </script></body></html>"""
