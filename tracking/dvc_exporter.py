# tracking/dvc_exporter.py
"""
DVC data export utilities for Agentix.

Exports pipeline results, news snapshots, and strategy params to parquet files
for DVC versioning. Uses boto3 for S3 upload when credentials are configured.

Requires (install separately): pandas, pyarrow, boto3
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("qc_fastapi_2.tracking.dvc_exporter")

# Optional dependencies — fail gracefully if not installed
_pandas_available = False
try:
    import pandas as pd
    _pandas_available = True
except ImportError:
    pd = None  # type: ignore

_boto3_available = False
try:
    import boto3
    _boto3_available = True
except ImportError:
    boto3 = None  # type: ignore


def _get_settings() -> Any:
    """Lazy import to avoid circular dependency at module load time."""
    from config import get_settings
    return get_settings()


def _write_parquet_local(df: "pd.DataFrame", path: str) -> None:
    """Write DataFrame to parquet, creating parent directories if needed."""
    if not _pandas_available:
        raise ImportError("pandas is required for parquet export. Install: pip install pandas pyarrow")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    logger.info(f"[DVC] Wrote {len(df)} rows → {path}")


def _upload_to_s3(local_path: str, s3_key: str) -> None:
    """Upload a local file to S3 bucket configured in settings."""
    if not _boto3_available:
        raise ImportError("boto3 is required for S3 upload. Install: pip install boto3")

    settings = _get_settings()
    if not settings.dvc_s3_bucket:
        logger.debug("[DVC] dvc_s3_bucket not set — skipping S3 upload")
        return

    client = boto3.client(
        "s3",
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_region or "us-east-1",
    )
    client.upload_file(local_path, settings.dvc_s3_bucket, s3_key)
    logger.info(f"[DVC] Uploaded → s3://{settings.dvc_s3_bucket}/{s3_key}")


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline result export
# ─────────────────────────────────────────────────────────────────────────────

async def export_pipeline_results(analysis_id: int) -> str | None:
    """
    Export pipeline output for a single analysis_id to a parquet file.

    Reads from AgentAnalysis DB table, writes to:
      data/outputs/analysis_{analysis_id}.parquet

    Returns local file path on success, None on failure.
    """
    if not _pandas_available:
        logger.warning("[DVC] export_pipeline_results: pandas not available — skipping")
        return None

    from db.queries import get_analysis_by_id

    row = await get_analysis_by_id(analysis_id)
    if not row:
        logger.warning(f"[DVC] export_pipeline_results: analysis_id={analysis_id} not found")
        return None

    researcher_out = row.get("researcher_output") or {}
    allocator_out = row.get("allocator_output") or {}  # synthesizer output
    risk_out = row.get("risk_output") or {}

    mj = allocator_out.get("market_judgment") or {}
    if not isinstance(mj, dict):
        mj = {}

    weights = allocator_out.get("adjusted_weights") or {}
    regime = str(mj.get("regime", ""))
    regime_confidence = float(mj.get("adjusted_confidence", 0.5) or 0.5)
    uncertainty = bool(mj.get("uncertainty_flag", False))

    row_data = {
        "analysis_id": analysis_id,
        "timestamp": row.get("created_at", ""),
        "trigger": row.get("trigger", ""),
        "regime": regime,
        "regime_confidence": regime_confidence,
        "uncertainty_flag": uncertainty,
        "recommended_stance": allocator_out.get("recommended_stance", ""),
        "decision_rationale": allocator_out.get("decision_rationale", ""),
        "base_weights": json.dumps(allocator_out.get("base_weights", {})),
        "adjusted_weights": json.dumps(weights),
        "weight_adjustments": json.dumps(allocator_out.get("weight_adjustments", [])),
        "risk_approved": risk_out.get("approved", False),
        "execution_status": risk_out.get("execution_status", row.get("execution_status", "")),
        "n_adjustments": len(allocator_out.get("weight_adjustments", [])),
        "key_events": json.dumps(allocator_out.get("key_events", [])),
        "overlays": json.dumps(risk_out.get("overlays", [])),
        "used_degraded_fallback": allocator_out.get("used_degraded_fallback", False),
        "degraded_reason": allocator_out.get("reasoning", "")[:300] if allocator_out.get("used_degraded_fallback") else "",
        "top5_weights": str(dict(sorted(weights.items(), key=lambda x: x[1], reverse=True)[:5])),
    }

    df = pd.DataFrame([row_data])

    base_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data", "outputs")
    path = os.path.join(base_dir, f"analysis_{analysis_id}.parquet")
    abs_path = os.path.abspath(path)

    try:
        _write_parquet_local(df, abs_path)

        # Upload to S3 if configured
        s3_key = f"pipeline_outputs/analysis_{analysis_id}.parquet"
        _upload_to_s3(abs_path, s3_key)
        return abs_path
    except Exception as e:
        logger.warning(f"[DVC] export_pipeline_results failed: {e}")
        return None


def export_strategy_params(pipeline_context: dict, output_dir: str | None = None) -> str | None:
    """
    Export current strategy risk params to a YAML file.

    Reads from pipeline_context (set by _guard_and_config in pipeline.py) and
    writes to: data/params/params_{timestamp}.yaml

    Returns local file path on success, None on failure.
    """
    import yaml

    risk_params = pipeline_context.get("risk_params", {})
    regime_result = pipeline_context.get("regime_result") or {}

    params_data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "active_strategy": pipeline_context.get("active_strategy", ""),
        "auth_mode": pipeline_context.get("auth_mode", ""),
        "risk_params": risk_params,
        "regime_result": {
            "regime": regime_result.get("regime", ""),
            "confidence": regime_result.get("confidence", ""),
            "constraints": regime_result.get("constraints", {}),
        },
    }

    if output_dir is None:
        base_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data", "params")
    else:
        base_dir = output_dir

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(base_dir, f"params_{ts}.yaml")
    abs_path = os.path.abspath(path)

    try:
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        with open(abs_path, "w") as f:
            yaml.dump(params_data, f, default_flow_style=False)
        logger.info(f"[DVC] Exported strategy params → {abs_path}")

        # Upload to S3 if configured
        s3_key = f"strategy_params/params_{ts}.yaml"
        _upload_to_s3(abs_path, s3_key)
        return abs_path
    except Exception as e:
        logger.warning(f"[DVC] export_strategy_params failed: {e}")
        return None


async def export_news_snapshot(output_dir: str | None = None) -> str | None:
    """
    Export recent news (48h window) from TickerNewsLibrary + MacroNewsCache
    to a parquet file for DVC versioning.

    Writes to: data/raw/news_{date}.parquet

    Returns local file path on success, None on failure.
    """
    if not _pandas_available:
        logger.warning("[DVC] export_news_snapshot: pandas not available — skipping")
        return None

    from db.session import async_session
    from db.models import TickerNewsLibrary, MacroNewsCache

    if output_dir is None:
        base_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "news")
    else:
        base_dir = os.path.join(output_dir, "news")

    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = os.path.join(base_dir, f"news_{date_str}.parquet")
    abs_path = os.path.abspath(path)

    try:
        async with async_session() as session:
            # Ticker news
            from sqlalchemy import select
            from datetime import timedelta

            cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
            stmt = select(TickerNewsLibrary).where(TickerNewsLibrary.fetched_at >= cutoff)
            result = await session.execute(stmt)
            ticker_rows = result.scalars().all()

            ticker_data = []
            for r in ticker_rows:
                ticker_data.append({
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

            macro_data = {}
            if macro_row:
                macro_data = {
                    "structured_payload": macro_row.structured_payload,
                    "prose_summary": macro_row.prose_summary,
                    "updated_at": macro_row.updated_at.isoformat() if macro_row.updated_at else "",
                }

        df_ticker = pd.DataFrame(ticker_data)
        df_ticker["type"] = "ticker_news"
        df_macro = pd.DataFrame([{"type": "macro_cache", **macro_data}])
        df = pd.concat([df_ticker, df_macro], ignore_index=True)

        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        df.to_parquet(abs_path, index=False)
        logger.info(f"[DVC] Exported news snapshot ({len(df)} rows) → {abs_path}")

        s3_key = f"news/{date_str}/news_{date_str}.parquet"
        _upload_to_s3(abs_path, s3_key)
        return abs_path

    except Exception as e:
        logger.warning(f"[DVC] export_news_snapshot failed: {e}")
        return None