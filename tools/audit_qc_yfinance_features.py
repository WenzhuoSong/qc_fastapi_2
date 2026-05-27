"""Audit QC snapshot research features against yfinance daily features.

Read-only by default. Use --write-db to persist the compact summary to
data_quality_audit for dashboard/cron trend monitoring.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
from datetime import UTC, datetime
from typing import Any


AUDIT_NAME = "qc_yfinance_feature_parity"
EXPECTED_NORMALIZED_RETURN_MAE_MAX = 0.05
DAILY_SNAPSHOT_PACKET = "daily_feature_snapshot"
HEARTBEAT_PACKET = "heartbeat"

RETURN_FIELDS = {
    "daily_return_pct",
    "return_5d",
    "mom_20d",
    "mom_60d",
    "mom_252d",
}

FIELD_PAIRS: tuple[tuple[str, str, str], ...] = (
    ("close_price", "close_price", "price"),
    ("daily_return_pct", "return_1d", "return"),
    ("return_5d", "return_5d", "return"),
    ("mom_20d", "return_20d", "return"),
    ("mom_60d", "return_60d", "return"),
    ("mom_252d", "return_252d", "return"),
    ("sma_20", "sma_20", "price"),
    ("sma_50", "sma_50", "price"),
    ("sma_200", "sma_200", "price"),
    ("rsi_14", "rsi_14", "oscillator"),
    ("atr_pct", "atr_pct", "ratio"),
    ("bb_position", "bb_position", "oscillator"),
    ("hist_vol_20d", "hist_vol_20d", "ratio"),
)

ROLE_CASE_SQL = """
CASE
  WHEN upper(elem->>'ticker') IN ('TQQQ','SQQQ','SOXL','SOXS','SPXL','SPXS','UVXY','VIXY') THEN 'hedge_levered'
  WHEN upper(elem->>'ticker') IN ('SOXX','PSI','FTXL','SMH','XSD','AIQ','BOTZ','CIBR','HACK','IGV','ICLN','TAN','URA','GRID','VUG','VTV','USMV') THEN 'thematic'
  WHEN upper(elem->>'ticker') IN ('DRAM','VEA','VWO','TLT','IEF','BND','SGOV','GLD') THEN 'satellite'
  WHEN upper(elem->>'ticker') IN ('SPY','QQQ','IWM','RSP') THEN 'core'
  ELSE 'sector'
END
""".strip()


def build_audit_sql(lookback_days: int) -> str:
    lookback = max(int(lookback_days), 1)
    qc_selects = []
    yf_selects = []
    metric_selects = []
    for qc_field, yf_field, _kind in FIELD_PAIRS:
        qc_selects.append(f"NULLIF(elem->>'{qc_field}', '')::numeric AS qc_{qc_field}")
        yf_selects.append(f"m.{yf_field}::numeric AS yf_{yf_field}")
        metric_selects.extend(_metric_selects(qc_field, yf_field, normalized=False))
        if qc_field in RETURN_FIELDS:
            metric_selects.extend(_metric_selects(qc_field, yf_field, normalized=True))

    return f"""
WITH latest_qc_by_day AS (
  SELECT DISTINCT ON (q.trading_date, q.packet_type)
    q.id, q.trading_date, q.packet_type, q.received_at, q.raw_payload
  FROM qc_snapshots q
  WHERE q.packet_type IN ('heartbeat', 'daily_feature_snapshot')
    AND q.trading_date >= current_date - interval '{lookback} days'
  ORDER BY q.trading_date, q.packet_type, q.received_at DESC
), qc AS (
  SELECT
    q.trading_date,
    q.packet_type,
    COALESCE(q.raw_payload->>'schema_version', 'legacy') AS schema_version,
    q.received_at,
    upper(elem->>'ticker') AS ticker,
    {ROLE_CASE_SQL} AS ticker_role,
    {", ".join(qc_selects)}
  FROM latest_qc_by_day q
  CROSS JOIN LATERAL jsonb_array_elements(
    CASE
      WHEN q.packet_type = 'daily_feature_snapshot'
      THEN COALESCE(q.raw_payload->'features', '[]'::jsonb)
      ELSE COALESCE(q.raw_payload->'holdings', '[]'::jsonb)
    END
  ) elem
  WHERE elem ? 'ticker'
), joined AS (
  SELECT
    qc.*,
    {", ".join(yf_selects)}
  FROM qc
  JOIN market_daily_features m
    ON m.trading_date = qc.trading_date
   AND m.ticker = qc.ticker
   AND m.source = 'yfinance'
)
SELECT
  packet_type,
  schema_version,
  ticker_role,
  COUNT(*) AS joined_rows,
  COUNT(DISTINCT trading_date) AS days,
  COUNT(DISTINCT ticker) AS tickers,
  {", ".join(metric_selects)}
FROM joined
GROUP BY packet_type, schema_version, ticker_role
ORDER BY packet_type, schema_version, ticker_role
""".strip()


def build_sample_sql(lookback_days: int, limit: int) -> str:
    lookback = max(int(lookback_days), 1)
    sample_limit = max(int(limit), 1)
    return f"""
WITH latest_qc_by_day AS (
  SELECT DISTINCT ON (q.trading_date, q.packet_type)
    q.id, q.trading_date, q.packet_type, q.received_at, q.raw_payload
  FROM qc_snapshots q
  WHERE q.packet_type IN ('heartbeat', 'daily_feature_snapshot')
    AND q.trading_date >= current_date - interval '{lookback} days'
  ORDER BY q.trading_date, q.packet_type, q.received_at DESC
), qc AS (
  SELECT
    q.trading_date,
    q.packet_type,
    COALESCE(q.raw_payload->>'schema_version', 'legacy') AS schema_version,
    upper(elem->>'ticker') AS ticker,
    {ROLE_CASE_SQL} AS ticker_role,
    NULLIF(elem->>'mom_20d', '')::numeric AS qc_mom_20d,
    NULLIF(elem->>'mom_60d', '')::numeric AS qc_mom_60d,
    NULLIF(elem->>'rsi_14', '')::numeric AS qc_rsi_14,
    NULLIF(elem->>'atr_pct', '')::numeric AS qc_atr_pct,
    NULLIF(elem->>'close_price', '')::numeric AS qc_close_price
  FROM latest_qc_by_day q
  CROSS JOIN LATERAL jsonb_array_elements(
    CASE
      WHEN q.packet_type = 'daily_feature_snapshot'
      THEN COALESCE(q.raw_payload->'features', '[]'::jsonb)
      ELSE COALESCE(q.raw_payload->'holdings', '[]'::jsonb)
    END
  ) elem
  WHERE elem ? 'ticker'
), scored AS (
  SELECT
    qc.*,
    m.return_20d::numeric AS yf_return_20d,
    m.return_60d::numeric AS yf_return_60d,
    m.rsi_14::numeric AS yf_rsi_14,
    m.atr_pct::numeric AS yf_atr_pct,
    m.close_price::numeric AS yf_close_price,
    ABS(qc_mom_20d - m.return_20d::numeric) AS e_mom20_raw,
    ABS((qc_mom_20d / 100.0) - m.return_20d::numeric) AS e_mom20_norm,
    ABS(qc_mom_60d - m.return_60d::numeric) AS e_mom60_raw,
    ABS((qc_mom_60d / 100.0) - m.return_60d::numeric) AS e_mom60_norm,
    ABS(qc_rsi_14 - m.rsi_14::numeric) AS e_rsi,
    ABS(qc_atr_pct - m.atr_pct::numeric) AS e_atr,
    ABS(qc_close_price - m.close_price::numeric) AS e_close
  FROM qc
  JOIN market_daily_features m
    ON m.trading_date = qc.trading_date
   AND m.ticker = qc.ticker
   AND m.source = 'yfinance'
)
SELECT *
FROM scored
ORDER BY
  COALESCE(e_mom20_raw, 0)
  + COALESCE(e_mom60_raw, 0)
  + COALESCE(e_rsi / 100.0, 0)
  + COALESCE(e_atr, 0) DESC
LIMIT {sample_limit}
""".strip()


def _metric_selects(qc_field: str, yf_field: str, *, normalized: bool) -> list[str]:
    suffix = f"{qc_field}_norm" if normalized else qc_field
    qc_expr = f"(qc_{qc_field} / 100.0)" if normalized else f"qc_{qc_field}"
    present = f"{qc_expr} IS NOT NULL AND yf_{yf_field} IS NOT NULL"
    return [
        f"COUNT(*) FILTER (WHERE {present}) AS n_{suffix}",
        f"AVG(ABS({qc_expr} - yf_{yf_field})) FILTER (WHERE {present}) AS mae_{suffix}",
        f"MAX(ABS({qc_expr} - yf_{yf_field})) FILTER (WHERE {present}) AS maxe_{suffix}",
    ]


def normalize_rows(rows: list[Any]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        item = dict(row)
        out.append({key: _jsonable(value) for key, value in item.items()})
    return out


def build_summary(rows: list[dict[str, Any]], *, lookback_days: int) -> dict[str, Any]:
    unit_risks = []
    severe_unit_risks = []
    expected_unit_mismatches = []
    heartbeat_lag_classes = []
    high_drift_classes = []
    packet_totals: dict[str, int] = {}
    max_raw_mom_error = 0.0
    max_percent_normalized_mom_error = 0.0
    max_contract_mom_error = 0.0
    daily_snapshot_max_contract_mom_error = 0.0

    for row in rows:
        packet = str(row.get("packet_type") or "unknown")
        role = str(row.get("ticker_role") or "unknown")
        packet_totals[packet] = packet_totals.get(packet, 0) + int(row.get("joined_rows") or 0)
        if role == "hedge_levered":
            high_drift_classes.append({
                "packet_type": packet,
                "ticker_role": role,
                "joined_rows": int(row.get("joined_rows") or 0),
                "reason": "levered_or_inverse_etf_expected_to_have_larger_qc_yfinance_drift",
            })
        heartbeat_lag = detect_heartbeat_daily_feature_lag(row)
        if heartbeat_lag:
            heartbeat_lag_classes.append(heartbeat_lag)
        for field in RETURN_FIELDS:
            risk = detect_unit_risk(row, field)
            if risk:
                unit_risks.append(risk)
                if risk.get("severity") == "normalized_drift":
                    severe_unit_risks.append(risk)
                else:
                    expected_unit_mismatches.append(risk)
        for field in ("mom_20d", "mom_60d", "mom_252d"):
            raw_error = float(row.get(f"maxe_{field}") or 0.0)
            percent_normalized_error = float(row.get(f"maxe_{field}_norm") or 0.0)
            contract_error = min(raw_error, percent_normalized_error)
            max_raw_mom_error = max(max_raw_mom_error, raw_error)
            max_percent_normalized_mom_error = max(max_percent_normalized_mom_error, percent_normalized_error)
            max_contract_mom_error = max(max_contract_mom_error, contract_error)
            if packet == DAILY_SNAPSHOT_PACKET:
                daily_snapshot_max_contract_mom_error = max(daily_snapshot_max_contract_mom_error, contract_error)

    status = "normalized_drift" if severe_unit_risks else ("expected_unit_mismatch" if unit_risks else "ok")
    return {
        "audit_name": AUDIT_NAME,
        "created_at": datetime.now(UTC).isoformat(),
        "lookback_days": int(lookback_days),
        "status": status,
        "packet_totals": packet_totals,
        "row_count": len(rows),
        "unit_risk_count": len(unit_risks),
        "expected_unit_mismatch_count": len(expected_unit_mismatches),
        "severe_unit_risk_count": len(severe_unit_risks),
        "unit_risks": unit_risks[:25],
        "severe_unit_risks": severe_unit_risks[:25],
        "expected_unit_mismatches": expected_unit_mismatches[:25],
        "heartbeat_lag_class_count": len(heartbeat_lag_classes),
        "heartbeat_lag_classes": heartbeat_lag_classes[:25],
        "high_drift_classes": high_drift_classes[:25],
        "max_raw_momentum_error": round(max_raw_mom_error, 6),
        "max_percent_normalized_momentum_error": round(max_percent_normalized_mom_error, 6),
        "max_normalized_momentum_error": round(max_contract_mom_error, 6),
        "daily_snapshot_max_contract_momentum_error": round(daily_snapshot_max_contract_mom_error, 6),
        "rows": rows,
    }


def detect_unit_risk(row: dict[str, Any], field: str) -> dict[str, Any] | None:
    if str(row.get("packet_type") or "") != DAILY_SNAPSHOT_PACKET:
        return None
    raw_mae = _float(row.get(f"mae_{field}"))
    norm_mae = _float(row.get(f"mae_{field}_norm"))
    raw_max = _float(row.get(f"maxe_{field}"))
    n = int(row.get(f"n_{field}") or 0)
    if n <= 0 or raw_mae is None or norm_mae is None:
        return None
    raw_is_large = raw_mae > 1.0 or (raw_max is not None and raw_max > 2.0)
    normalization_helps = norm_mae < raw_mae * 0.25
    if not (raw_is_large and normalization_helps):
        return None
    severity = "expected_unit_mismatch"
    if (
        str(row.get("ticker_role") or "") != "hedge_levered"
        and norm_mae > EXPECTED_NORMALIZED_RETURN_MAE_MAX
    ):
        severity = "normalized_drift"
    return {
        "packet_type": row.get("packet_type"),
        "schema_version": row.get("schema_version"),
        "ticker_role": row.get("ticker_role"),
        "field": field,
        "n": n,
        "raw_mae": round(raw_mae, 6),
        "normalized_mae": round(norm_mae, 6),
        "severity": severity,
        "reason": (
            "qc_return_field_uses_percent_points_but_normalizes_cleanly"
            if severity == "expected_unit_mismatch"
            else "qc_return_field_still_drifts_after_percent_point_normalization"
        ),
    }


def detect_heartbeat_daily_feature_lag(row: dict[str, Any]) -> dict[str, Any] | None:
    """Heartbeat daily indicators can lag until the daily bar consolidates."""
    if str(row.get("packet_type") or "") != HEARTBEAT_PACKET:
        return None
    max_raw = max(
        _float(row.get("mae_mom_20d")) or 0.0,
        _float(row.get("mae_mom_60d")) or 0.0,
        _float(row.get("mae_mom_252d")) or 0.0,
    )
    if max_raw <= 0.01:
        return None
    return {
        "packet_type": row.get("packet_type"),
        "schema_version": row.get("schema_version"),
        "ticker_role": row.get("ticker_role"),
        "joined_rows": int(row.get("joined_rows") or 0),
        "max_raw_momentum_mae": round(max_raw, 6),
        "reason": "heartbeat_daily_indicators_may_lag_until_eod_use_daily_feature_snapshot_for_research_parity",
    }


def build_markdown_report(summary: dict[str, Any], samples: list[dict[str, Any]] | None = None) -> str:
    lines = [
        "# QC vs yfinance Feature Audit",
        "",
        f"- Status: `{summary.get('status')}`",
        f"- Lookback days: {summary.get('lookback_days')}",
        f"- Joined rows: {sum((summary.get('packet_totals') or {}).values())}",
        f"- Unit risks: {summary.get('unit_risk_count')}",
        f"- Expected unit mismatches: {summary.get('expected_unit_mismatch_count')}",
        f"- Severe unit risks: {summary.get('severe_unit_risk_count')}",
        f"- Heartbeat lag classes: {summary.get('heartbeat_lag_class_count')}",
        f"- Max contract momentum error: {summary.get('max_normalized_momentum_error')}",
        f"- Daily snapshot max contract momentum error: {summary.get('daily_snapshot_max_contract_momentum_error')}",
        "",
        "## Packet Totals",
        "",
        "| packet_type | joined_rows |",
        "|---|---:|",
    ]
    for packet, count in sorted((summary.get("packet_totals") or {}).items()):
        lines.append(f"| {packet} | {count} |")

    lines.extend([
        "",
        "## Summary By Packet And Role",
        "",
        "| packet | schema | role | rows | mae_mom20_raw | mae_mom20_norm | mae_mom60_raw | mae_mom60_norm | mae_rsi | mae_atr |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for row in summary.get("rows") or []:
        lines.append(
            "| {packet} | {schema} | {role} | {rows} | {m20} | {m20n} | {m60} | {m60n} | {rsi} | {atr} |".format(
                packet=row.get("packet_type"),
                schema=row.get("schema_version"),
                role=row.get("ticker_role"),
                rows=row.get("joined_rows"),
                m20=_fmt(row.get("mae_mom_20d")),
                m20n=_fmt(row.get("mae_mom_20d_norm")),
                m60=_fmt(row.get("mae_mom_60d")),
                m60n=_fmt(row.get("mae_mom_60d_norm")),
                rsi=_fmt(row.get("mae_rsi_14")),
                atr=_fmt(row.get("mae_atr_pct")),
            )
        )

    if summary.get("unit_risks"):
        lines.extend(["", "## Unit Risk Flags", ""])
        for risk in summary["unit_risks"]:
            lines.append(
                "- {packet}/{schema}/{role}/{field}: raw_mae={raw}, normalized_mae={norm}, severity={severity} ({reason})".format(
                    packet=risk.get("packet_type"),
                    schema=risk.get("schema_version"),
                    role=risk.get("ticker_role"),
                    field=risk.get("field"),
                    raw=risk.get("raw_mae"),
                    norm=risk.get("normalized_mae"),
                    severity=risk.get("severity"),
                    reason=risk.get("reason"),
                )
            )

    if summary.get("severe_unit_risks"):
        lines.extend(["", "## Severe Unit Risks", ""])
        for risk in summary["severe_unit_risks"]:
            lines.append(
                "- {packet}/{schema}/{role}/{field}: normalized_mae={norm} ({reason})".format(
                    packet=risk.get("packet_type"),
                    schema=risk.get("schema_version"),
                    role=risk.get("ticker_role"),
                    field=risk.get("field"),
                    norm=risk.get("normalized_mae"),
                    reason=risk.get("reason"),
                )
            )

    if summary.get("high_drift_classes"):
        lines.extend(["", "## High Drift Classes", ""])
        for item in summary["high_drift_classes"]:
            lines.append(
                "- {packet}/{role}: rows={rows} ({reason})".format(
                    packet=item.get("packet_type"),
                    role=item.get("ticker_role"),
                    rows=item.get("joined_rows"),
                    reason=item.get("reason"),
                )
            )

    if summary.get("heartbeat_lag_classes"):
        lines.extend(["", "## Heartbeat Daily-Feature Lag Classes", ""])
        for item in summary["heartbeat_lag_classes"]:
            lines.append(
                "- {packet}/{schema}/{role}: rows={rows}, max_raw_momentum_mae={mae} ({reason})".format(
                    packet=item.get("packet_type"),
                    schema=item.get("schema_version"),
                    role=item.get("ticker_role"),
                    rows=item.get("joined_rows"),
                    mae=item.get("max_raw_momentum_mae"),
                    reason=item.get("reason"),
                )
            )

    if samples:
        lines.extend([
            "",
            "## Largest Raw Divergence Samples",
            "",
            "| date | packet | schema | role | ticker | qc_mom20 | yf_return20 | raw_e20 | norm_e20 | qc_mom60 | yf_return60 | raw_e60 | norm_e60 |",
            "|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ])
        for row in samples:
            lines.append(
                "| {date} | {packet} | {schema} | {role} | {ticker} | {qm20} | {yf20} | {e20} | {e20n} | {qm60} | {yf60} | {e60} | {e60n} |".format(
                    date=row.get("trading_date"),
                    packet=row.get("packet_type"),
                    schema=row.get("schema_version"),
                    role=row.get("ticker_role"),
                    ticker=row.get("ticker"),
                    qm20=_fmt(row.get("qc_mom_20d")),
                    yf20=_fmt(row.get("yf_return_20d")),
                    e20=_fmt(row.get("e_mom20_raw")),
                    e20n=_fmt(row.get("e_mom20_norm")),
                    qm60=_fmt(row.get("qc_mom_60d")),
                    yf60=_fmt(row.get("yf_return_60d")),
                    e60=_fmt(row.get("e_mom60_raw")),
                    e60n=_fmt(row.get("e_mom60_norm")),
                )
            )

    lines.append("")
    return "\n".join(lines)


async def run_audit(*, lookback_days: int, sample_limit: int, write_db: bool = False) -> dict[str, Any]:
    rows = await _query_rows(build_audit_sql(lookback_days))
    samples = await _query_rows(build_sample_sql(lookback_days, sample_limit))

    summary = build_summary(rows, lookback_days=lookback_days)
    summary["samples"] = samples
    if write_db:
        await write_audit_summary(summary)
    return summary


async def _query_rows(sql: str) -> list[dict[str, Any]]:
    try:
        from sqlalchemy import text

        from db.session import AsyncSessionLocal
    except ModuleNotFoundError:
        return _query_rows_with_psql(sql)

    async with AsyncSessionLocal() as db:
        return normalize_rows((await db.execute(text(sql))).mappings().all())


def _query_rows_with_psql(sql: str) -> list[dict[str, Any]]:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required when SQLAlchemy is unavailable")
    wrapped = f"SELECT COALESCE(json_agg(row_to_json(q)), '[]'::json) FROM ({sql}) q"
    result = subprocess.run(
        ["psql", database_url, "-t", "-A", "-q", "-c", wrapped],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = result.stdout.strip() or "[]"
    return normalize_rows(json.loads(payload))


async def write_audit_summary(summary: dict[str, Any]) -> None:
    try:
        from sqlalchemy import text

        from db.session import AsyncSessionLocal
    except ModuleNotFoundError:
        _write_audit_summary_with_psql(summary)
        return

    status = str(summary.get("status") or "unknown")
    async with AsyncSessionLocal() as db:
        await db.execute(text("""
            CREATE TABLE IF NOT EXISTS data_quality_audit (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMP NOT NULL DEFAULT now(),
                audit_name VARCHAR(80) NOT NULL,
                lookback_days INTEGER NOT NULL,
                summary JSONB NOT NULL,
                status VARCHAR(30) NOT NULL
            )
        """))
        await db.execute(
            text("""
                INSERT INTO data_quality_audit (audit_name, lookback_days, summary, status)
                VALUES (:audit_name, :lookback_days, CAST(:summary AS JSONB), :status)
            """),
            {
                "audit_name": AUDIT_NAME,
                "lookback_days": int(summary.get("lookback_days") or 0),
                "summary": json.dumps(summary, default=str),
                "status": status,
            },
        )
        await db.commit()


def _write_audit_summary_with_psql(summary: dict[str, Any]) -> None:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required when SQLAlchemy is unavailable")
    audit_name = AUDIT_NAME.replace("$audit$", "")
    status = str(summary.get("status") or "unknown").replace("$status$", "")
    summary_json = json.dumps(summary, default=str).replace("$summary$", "")
    lookback_days = int(summary.get("lookback_days") or 0)
    sql = f"""
        CREATE TABLE IF NOT EXISTS data_quality_audit (
            id BIGSERIAL PRIMARY KEY,
            created_at TIMESTAMP NOT NULL DEFAULT now(),
            audit_name VARCHAR(80) NOT NULL,
            lookback_days INTEGER NOT NULL,
            summary JSONB NOT NULL,
            status VARCHAR(30) NOT NULL
        );
        INSERT INTO data_quality_audit (audit_name, lookback_days, summary, status)
        VALUES ($audit${audit_name}$audit$, {lookback_days}, $summary${summary_json}$summary$::jsonb, $status${status}$status$);
    """
    subprocess.run(["psql", database_url, "-q", "-v", "ON_ERROR_STOP=1", "-c", sql], check=True)


def _jsonable(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    try:
        import decimal

        if isinstance(value, decimal.Decimal):
            return float(value)
    except Exception:
        pass
    return value


def _float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _fmt(value: Any) -> str:
    num = _float(value)
    if num is None:
        return ""
    return f"{num:.6f}"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit QC snapshot features against yfinance daily features.")
    parser.add_argument("--lookback-days", type=int, default=45)
    parser.add_argument("--sample-limit", type=int, default=12)
    parser.add_argument("--write-db", action="store_true", help="Persist compact audit summary to data_quality_audit.")
    parser.add_argument(
        "--fail-on-unit-risk",
        action="store_true",
        help="Exit non-zero when percent-vs-decimal unit risk is detected.",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of markdown.")
    return parser.parse_args(argv)


async def _amain(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = await run_audit(
        lookback_days=args.lookback_days,
        sample_limit=args.sample_limit,
        write_db=bool(args.write_db),
    )
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    else:
        print(build_markdown_report(summary, summary.get("samples") or []))
    if args.fail_on_unit_risk and summary.get("unit_risk_count"):
        return 2
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_amain()))


if __name__ == "__main__":
    main()
