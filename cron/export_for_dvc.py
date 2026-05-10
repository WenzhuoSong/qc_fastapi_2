# cron/export_for_dvc.py
"""
Standalone script to export Agentix data for DVC versioning.

Usage:
    python -m cron.export_for_dvc --type news              # Export recent news snapshots
    python -m cron.export_for_dvc --type pipeline          # Export pipeline results from DB
    python -m cron.export_for_dvc --type params           # Export current strategy params
    python -m cron.export_for_dvc --type pipeline --id 76  # Export specific analysis_id

Requires (install separately):
    pip install pandas pyarrow boto3 pyyaml

Environment variables (required for S3 upload):
    DVC_S3_BUCKET          # e.g. "my-agentix-data"
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_REGION             # default: us-east-1
    MLFLOW_TRACKING_URI    # optional, for verification
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone

# Optional deps — fail fast if missing
try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import boto3
except ImportError:
    boto3 = None

try:
    import yaml
except ImportError:
    yaml = None


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(name)s | %(levelname)s | %(message)s")
logger = logging.getLogger("export_for_dvc")


# ─────────────────────────────────────────────────────────────────────────────
# S3 helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_s3_client():
    if not boto3:
        raise ImportError("boto3 required for S3 upload. Install: pip install boto3")
    import os
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", ""),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def _upload(local_path: str, s3_key: str, bucket: str):
    if not bucket:
        logger.debug("DVC_S3_BUCKET not set — skipping upload")
        return
    client = _get_s3_client()
    client.upload_file(local_path, bucket, s3_key)
    logger.info(f"[S3] Uploaded → s3://{bucket}/{s3_key}")


# ─────────────────────────────────────────────────────────────────────────────
# News export
# ─────────────────────────────────────────────────────────────────────────────

async def export_news() -> str | None:
    if not pd:
        logger.warning("pandas not available — skipping news export")
        return None

    from db.session import async_session
    from db.models import TickerNewsLibrary, MacroNewsCache
    from sqlalchemy import select
    from datetime import timedelta

    cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    base_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "news")
    out_path = os.path.join(base_dir, f"news_{date_str}.parquet")

    async with async_session() as session:
        stmt = select(TickerNewsLibrary).where(TickerNewsLibrary.fetched_at >= cutoff)
        result = await session.execute(stmt)
        rows = result.scalars().all()

        records = []
        for r in rows:
            records.append({
                "ticker": r.ticker,
                "source_api": r.source_api,
                "headline": r.headline,
                "url": r.url,
                "sentiment": r.sentiment,
                "sentiment_label": r.sentiment_label,
                "relevance": r.relevance,
                "is_hard_event": r.is_hard_event,
                "llm_summary": r.llm_summary,
                "fetched_at": r.fetched_at.isoformat() if r.fetched_at else "",
            })

        macro_stmt = select(MacroNewsCache)
        macro_result = await session.execute(macro_stmt)
        macro_row = macro_result.scalar_one_or_none()
        macro_record = {}
        if macro_row:
            macro_record = {
                "type": "macro_cache",
                "structured_payload": str(macro_row.structured_payload),
                "prose_summary": macro_row.prose_summary,
                "updated_at": macro_row.updated_at.isoformat() if macro_row.updated_at else "",
            }

    df = pd.DataFrame(records)
    df["type"] = "ticker_news"
    if macro_record:
        df = pd.concat([df, pd.DataFrame([macro_record])], ignore_index=True)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_parquet(out_path, index=False)
    logger.info(f"[DVC] Exported {len(df)} rows → {out_path}")

    bucket = os.environ.get("DVC_S3_BUCKET", "")
    _upload(out_path, f"news/{date_str}/news_{date_str}.parquet", bucket)
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline output export
# ─────────────────────────────────────────────────────────────────────────────

async def export_pipeline(analysis_id: int | None = None) -> str | None:
    if not pd:
        logger.warning("pandas not available — skipping pipeline export")
        return None

    from db.queries import get_analysis_by_id
    from db.session import async_session
    from db.models import AgentAnalysis
    from sqlalchemy import select

    base_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data", "outputs")
    os.makedirs(base_dir, exist_ok=True)

    async with async_session() as session:
        if analysis_id is not None:
            stmt = select(AgentAnalysis).where(AgentAnalysis.id == analysis_id)
        else:
            # Latest analysis
            stmt = select(AgentAnalysis).order_by(AgentAnalysis.id.desc()).limit(1)
        result = await session.execute(stmt)
        row = result.scalar_one_or_none()
        if not row:
            logger.warning(f"No analysis found (id={analysis_id or 'latest'})")
            return None

        ai_id = row.id
        researcher_out = row.researcher_output or {}
        allocator_out = row.allocator_output or {}
        risk_out = row.risk_output or {}

        mj = allocator_out.get("market_judgment") or {}
        if not isinstance(mj, dict):
            mj = {}

        weights = allocator_out.get("adjusted_weights") or {}
        top5 = dict(sorted(weights.items(), key=lambda x: x[1], reverse=True)[:5])

        import json
        record = {
            "analysis_id": ai_id,
            "timestamp": row.analyzed_at.isoformat() if row.analyzed_at else "",
            "trigger": row.trigger_type or "",
            "regime": str(mj.get("regime", "")),
            "regime_confidence": float(mj.get("adjusted_confidence", 0.5) or 0.5),
            "uncertainty_flag": bool(mj.get("uncertainty_flag", False)),
            "recommended_stance": allocator_out.get("recommended_stance", ""),
            "decision_rationale": allocator_out.get("decision_rationale", ""),
            "adjusted_weights": json.dumps(weights),
            "risk_approved": bool(risk_out.get("approved", False)),
            "execution_status": risk_out.get("execution_status", row.execution_status or ""),
            "n_adjustments": len(allocator_out.get("weight_adjustments", [])),
            "key_events": json.dumps(allocator_out.get("key_events", [])),
            "overlays": json.dumps(risk_out.get("overlays_applied", [])),
            "used_degraded_fallback": allocator_out.get("used_degraded_fallback", False),
            "top5_weights": str(top5),
        }

        df = pd.DataFrame([record])
        out_path = os.path.join(base_dir, f"analysis_{ai_id}.parquet")
        df.to_parquet(out_path, index=False)
        logger.info(f"[DVC] Exported analysis_{ai_id} → {out_path}")

        bucket = os.environ.get("DVC_S3_BUCKET", "")
        _upload(out_path, f"pipeline_outputs/analysis_{ai_id}.parquet", bucket)
        return out_path


# ─────────────────────────────────────────────────────────────────────────────
# Strategy params export
# ─────────────────────────────────────────────────────────────────────────────

async def export_params() -> str | None:
    if not yaml:
        logger.warning("pyyaml not available — skipping params export")
        return None

    from db.session import async_session
    from db.queries import get_system_config

    base_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data", "params")
    os.makedirs(base_dir, exist_ok=True)

    async with async_session() as db:
        risk_params = await get_system_config(db, "risk_params")
        strategy_key = await get_system_config(db, "active_strategy")
        strategy_params_key = await get_system_config(db, f"strategy_{strategy_key.value}_params")
        regime_key = await get_system_config(db, "regime_result")

        params_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "active_strategy": strategy_key.value if strategy_key else "",
            "risk_params": risk_params.value if risk_params else {},
            "strategy_params": strategy_params_key.value if strategy_params_key else {},
            "regime_result": regime_key.value if regime_key else {},
        }

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(base_dir, f"params_{ts}.yaml")
    with open(out_path, "w") as f:
        yaml.dump(params_data, f, default_flow_style=False)
    logger.info(f"[DVC] Exported strategy params → {out_path}")

    bucket = os.environ.get("DVC_S3_BUCKET", "")
    _upload(out_path, f"strategy_params/params_{ts}.yaml", bucket)
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Export Agentix data for DVC versioning")
    parser.add_argument("--type", choices=["news", "pipeline", "params"], required=True,
                        help="Export type")
    parser.add_argument("--id", type=int, default=None,
                        help="Specific analysis_id to export (pipeline type only)")
    args = parser.parse_args()

    if args.type == "news":
        result = asyncio.run(export_news())
    elif args.type == "pipeline":
        result = asyncio.run(export_pipeline(args.id))
    elif args.type == "params":
        result = asyncio.run(export_params())
    else:
        parser.error(f"Unknown type: {args.type}")

    if result:
        logger.info(f"Export complete: {result}")
    else:
        logger.warning("Export produced no output (check dependencies)")


if __name__ == "__main__":
    main()