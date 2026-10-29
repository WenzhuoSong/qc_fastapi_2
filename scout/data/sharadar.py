"""Sharadar (personal-use subscription): bulk tables to local files, point in time, incl. delisted.

The subscription's data lives only in the git-ignored `exports/sharadar/` on the user's machine
(option A, 2026-10-29): bulk zips in `raw/`, each converted once to parquet in `parquet/`. The
licence requires deleting every copy within 30 days of ending the subscription (`purge`), and
forbids sharing it; derived research results without the data may be kept.

The key goes in the `x-api-key` header, never in a URL or a log. A bulk request answers with a
redirect to a pre-signed file, fetched without the key.
"""
from __future__ import annotations

import hashlib
import json
import logging
import shutil
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import httpx

logger = logging.getLogger("scout.data.sharadar")

BASE = "https://api.sharadar.com/v1.0"
ROOT = Path(__file__).resolve().parents[2] / "exports" / "sharadar"
# Bulk tables used by Scout's research. `holdings` (every 13F line) is left out: the per-ticker
# summary answers the ownership questions at a fraction of the size.
TABLES = ("tickers", "descriptions", "actions", "sp500", "events", "stocks", "funds", "metrics", "daily",
          "fundamentals", "insiders", "holdings_ticker")
CHUNK = 1 << 20


def _headers(key: str) -> dict[str, str]:
    return {"x-api-key": key}


def bulk_location(key: str, table: str, client: httpx.Client) -> str:
    """The pre-signed URL of a table's full-history bulk file."""
    r = client.get(f"{BASE}/data/{table}", headers=_headers(key), params={"years": "full"}, follow_redirects=False)
    if r.status_code not in (301, 302, 303, 307, 308) or "location" not in r.headers:
        raise RuntimeError(f"bulk {table}: HTTP {r.status_code}")
    return r.headers["location"]


def download(key: str, table: str, root: Path = ROOT, client: httpx.Client | None = None) -> dict:
    """Fetch one table's bulk zip into root/raw; returns its manifest entry."""
    raw = root / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    own = client is None
    client = client or httpx.Client(timeout=httpx.Timeout(60.0, read=600.0))
    try:
        url = bulk_location(key, table, client)
        tmp, dest = raw / f"{table}.zip.part", raw / f"{table}.zip"
        digest = hashlib.sha256()
        t0 = time.time()
        with client.stream("GET", url, follow_redirects=True) as r:      # pre-signed: no key sent
            r.raise_for_status()
            with tmp.open("wb") as f:
                for chunk in r.iter_bytes(CHUNK):
                    f.write(chunk)
                    digest.update(chunk)
        with zipfile.ZipFile(tmp) as z:
            if z.testzip() is not None:
                raise RuntimeError(f"bulk {table}: corrupt zip")
            members = z.namelist()
        tmp.replace(dest)
        return {"table": table, "file": dest.name, "bytes": dest.stat().st_size, "sha256": digest.hexdigest(),
                "members": members, "seconds": round(time.time() - t0, 1),
                "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    finally:
        if own:
            client.close()


def to_parquet(table: str, root: Path = ROOT) -> dict:
    """Convert a downloaded zip (one CSV) to root/parquet/<table>.parquet, streaming in batches."""
    import pyarrow as pa
    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq

    out = root / "parquet"
    out.mkdir(parents=True, exist_ok=True)
    dest, tmp = out / f"{table}.parquet", out / f"{table}.parquet.part"
    with zipfile.ZipFile(root / "raw" / f"{table}.zip") as z:
        csvs = [m for m in z.namelist() if m.lower().endswith(".csv")]
        if len(csvs) != 1:
            raise RuntimeError(f"{table}: expected one CSV, found {csvs}")
        with z.open(csvs[0]) as fh:
            header = fh.readline().decode("utf-8-sig").strip().split(",")
        with z.open(csvs[0]) as fh:
            # every column as text: a streamed reader infers types from its first block, and later
            # blocks of Sharadar files can disagree; typed per table where it is read
            reader = pcsv.open_csv(fh, read_options=pcsv.ReadOptions(block_size=64 << 20),
                                   convert_options=pcsv.ConvertOptions(column_types={c: pa.string() for c in header},
                                                                       strings_can_be_null=True))
            writer, rows = None, 0
            for batch in reader:
                if writer is None:
                    schema = pa.schema([pa.field(f.name, pa.string()) for f in batch.schema])
                    writer = pq.ParquetWriter(tmp, schema, compression="zstd")
                writer.write_table(pa.Table.from_batches([batch]).cast(schema))
                rows += batch.num_rows
            if writer is not None:
                writer.close()
    tmp.replace(dest)
    return {"table": table, "rows": rows, "parquet_bytes": dest.stat().st_size}


def write_manifest(entries: list[dict], root: Path = ROOT) -> Path:
    path = root / "manifest.json"
    old = json.loads(path.read_text()) if path.exists() else {}
    for e in entries:
        old[e["table"]] = {**old.get(e["table"], {}), **e}
    path.write_text(json.dumps(old, indent=2, ensure_ascii=False))
    return path


def purge(root: Path = ROOT) -> None:
    """Delete every local copy (licence: within 30 days of the subscription ending)."""
    if root.exists():
        shutil.rmtree(root)
