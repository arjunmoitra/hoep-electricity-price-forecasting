"""
Databricks job entrypoint: fetch new IESO files for one report and append them
to that report's bronze Delta table, as raw content plus provenance metadata.

Idempotency: a file is only ever fetched once. We anti-join the remote file
listing against filenames already present in the bronze table, so a rerun (or
an hourly schedule landing twice due to a retry) never re-fetches or
duplicates a row. The *content* of a given filename on IESO's site is
immutable once published -- revisions get their own `_vN` filename -- so
"already have this filename" is a safe definition of "already ingested".

Usage (as a Databricks job task):
    python bronze_job.py <report_key> <catalog> [schema]

This file intentionally does almost nothing itself -- list/fetch logic lives
in ieso_source.py so it's unit-testable without Spark; this is just the glue
that calls it and writes the result with Spark/Delta.
"""
from __future__ import annotations

import datetime as dt
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.ingestion.ieso_source import REPORT_CONFIGS, fetch_file, list_remote_files

BRONZE_SCHEMA = StructType(
    [
        StructField("report", StringType(), False),
        StructField("source_filename", StringType(), False),
        StructField("key", StringType(), True),
        StructField("version", IntegerType(), True),
        StructField("fetched_at", TimestampType(), False),
        StructField("raw_content", StringType(), False),
        StructField("ingestion_run_id", StringType(), False),
    ]
)


def run(report: str, catalog: str, schema: str = "bronze") -> int:
    if report not in REPORT_CONFIGS:
        raise ValueError(f"unknown report {report!r}, expected one of {list(REPORT_CONFIGS)}")

    spark = SparkSession.builder.getOrCreate()
    table = f"{catalog}.{schema}.{report}"

    remote_files = list_remote_files(report)

    try:
        existing = {
            r["source_filename"]
            for r in spark.table(table).select("source_filename").distinct().collect()
        }
    except Exception:
        # table doesn't exist yet -- first run for this report.
        existing = set()

    new_files = [f for f in remote_files if f.filename not in existing]
    if not new_files:
        print(f"[{report}] no new files (checked {len(remote_files)}, all already ingested)")
        return 0

    run_id = dt.datetime.utcnow().isoformat()
    now = dt.datetime.utcnow()
    rows = []
    errors = []
    for rf in new_files:
        try:
            content = fetch_file(rf.url)
        except Exception as e:  # noqa: BLE001 -- one bad file shouldn't kill the run
            errors.append((rf.filename, repr(e)))
            continue
        rows.append(
            {
                "report": report,
                "source_filename": rf.filename,
                "key": rf.key,
                "version": rf.version,
                "fetched_at": now,
                "raw_content": content,
                "ingestion_run_id": run_id,
            }
        )

    if rows:
        df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(table)

    print(
        f"[{report}] fetched {len(rows)}/{len(new_files)} new files "
        f"({len(errors)} errors)"
    )
    for filename, err in errors:
        print(f"[{report}]   FAILED {filename}: {err}")

    if errors and not rows:
        # every single fetch failed -- treat as a hard failure so the job
        # surfaces in Databricks job-failure notifications (Phase 5).
        raise RuntimeError(f"[{report}] all {len(errors)} fetches failed")

    return len(rows)


if __name__ == "__main__":
    report_arg = sys.argv[1]
    catalog_arg = sys.argv[2]
    schema_arg = sys.argv[3] if len(sys.argv) > 3 else "bronze"
    run(report_arg, catalog_arg, schema_arg)
