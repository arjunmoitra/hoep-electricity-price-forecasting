"""
Databricks job entrypoint: bronze (raw files) -> silver (clean hourly rows).

For each report:
  1. Read bronze rows not yet processed into silver (anti-join on
     source_filename against a `_silver_processed_files` marker table, so
     reruns are idempotent at the file level).
  2. Parse each row's raw_content with the matching parser from
     src/ingestion/parsers.py.
  3. For realtime and day-ahead: collapse to one row per
     (delivery_date, delivery_hour) using the latest version
     (src/silver/dedupe.py), then MERGE INTO the silver table (upsert, so a
     later-arriving higher version replaces an earlier one without
     duplicating).
  4. For predispatch: keep every (delivery_date, delivery_hour, published_at)
     row -- that full publish-time history is the point of
     `silver.predispatch_asof` (Phase 1 spec: compare our forecast to the
     predispatch value that was actually available h hours before delivery).
  5. Run data-quality checks (src/silver/quality.py) per delivery date touched
     this run and append the result to `data_quality_log`.

Usage (as a Databricks job task):
    python merge_job.py <report_key> <catalog> [bronze_schema] [silver_schema]
"""
from __future__ import annotations

import datetime as dt
import sys

from pyspark.sql import Row, SparkSession

from src.ingestion.parsers import PARSERS, ParseError
from src.silver.dedupe import pick_latest_per_key
from src.silver.quality import check_all_days

# Reports collapsed to one row per (delivery_date, delivery_hour).
LATEST_WINS_REPORTS = {
    "realtime_ontario_zonal_price": {
        "silver_table": "ontario_zonal_price_hourly",
        "price_key": "ontario_zonal_price",
        "merge_keys": ("delivery_date", "delivery_hour"),
    },
    "dayahead_hourly_ontario_zonal_price": {
        "silver_table": "dayahead_hourly_ontario_zonal_price",
        "price_key": "dayahead_ontario_zonal_price",
        "merge_keys": ("delivery_date", "delivery_hour"),
    },
    "demand": {
        "silver_table": "demand_hourly",
        "price_key": None,
        "merge_keys": ("delivery_date", "delivery_hour"),
    },
}

# Reports kept as full publish-time history (append, not collapse).
ASOF_REPORTS = {
    "predispatch_hourly_ontario_zonal_price": {
        "silver_table": "predispatch_asof",
        "price_key": "predispatch_ontario_zonal_price",
        "merge_keys": ("delivery_date", "delivery_hour", "published_at"),
    },
}


def _processed_marker_table(catalog: str, silver_schema: str) -> str:
    return f"{catalog}.{silver_schema}._silver_processed_files"


def _unprocessed_bronze_rows(spark, report: str, catalog: str, bronze_schema: str, silver_schema: str):
    bronze_table = f"{catalog}.{bronze_schema}.{report}"
    marker_table = _processed_marker_table(catalog, silver_schema)
    bronze_df = spark.table(bronze_table)
    try:
        # `report` always comes from our own fixed REPORT_CONFIGS keys (see
        # ieso_source.py), never from external input, so plain string
        # formatting here is safe -- not building SQL from untrusted data.
        processed = spark.table(marker_table).filter(f"report = '{report}'")
        filenames = {r["source_filename"] for r in processed.select("source_filename").collect()}
    except Exception:
        filenames = set()
    if filenames:
        bronze_df = bronze_df.filter(~bronze_df.source_filename.isin(filenames))
    return bronze_df


def run(report: str, catalog: str, bronze_schema: str = "bronze", silver_schema: str = "silver") -> dict:
    spark = SparkSession.builder.getOrCreate()
    parser = PARSERS[report]

    bronze_df = _unprocessed_bronze_rows(spark, report, catalog, bronze_schema, silver_schema)
    bronze_rows = bronze_df.collect()
    if not bronze_rows:
        print(f"[{report}] no new bronze rows to process")
        return {"parsed_rows": 0, "parse_errors": 0}

    parsed_rows: list[dict] = []
    parse_errors: list[tuple[str, str]] = []
    processed_filenames: list[str] = []
    for brow in bronze_rows:
        try:
            out = parser(brow["raw_content"], brow["source_filename"])
        except ParseError as e:
            parse_errors.append((brow["source_filename"], str(e)))
            continue
        for r in out:
            r["version"] = brow["version"]
        parsed_rows.extend(out)
        processed_filenames.append(brow["source_filename"])

    if parse_errors:
        print(f"[{report}] {len(parse_errors)} files failed to parse:")
        for fn, err in parse_errors:
            print(f"  {fn}: {err}")

    if not parsed_rows:
        raise RuntimeError(
            f"[{report}] every file in this batch failed to parse "
            f"({len(parse_errors)} errors). Not writing anything to silver. "
            "This almost certainly means the parser's assumed schema doesn't "
            "match the real file -- see src/ingestion/parsers.py docstring."
        )

    cfg = LATEST_WINS_REPORTS.get(report) or ASOF_REPORTS.get(report)
    merge_keys = cfg["merge_keys"]
    if report in LATEST_WINS_REPORTS:
        parsed_rows = pick_latest_per_key(parsed_rows, merge_keys)

    silver_table = f"{catalog}.{silver_schema}.{cfg['silver_table']}"
    _merge_into(spark, silver_table, parsed_rows, merge_keys)

    # Data quality: check every delivery date touched this run.
    if cfg["price_key"] is not None:
        reports = check_all_days(parsed_rows, price_key=cfg["price_key"])
        _log_quality(spark, catalog, silver_schema, report, reports)
        for qr in reports:
            if not qr.is_clean:
                print(
                    f"[{report}] DQ issue on {qr.delivery_date}: "
                    f"missing={qr.missing_hours} dup={qr.duplicate_hours} "
                    f"out_of_range={len(qr.out_of_range_rows)}"
                )

    _mark_processed(spark, catalog, silver_schema, report, processed_filenames)

    print(f"[{report}] merged {len(parsed_rows)} rows into {silver_table}")
    return {"parsed_rows": len(parsed_rows), "parse_errors": len(parse_errors)}


def _merge_into(spark, table: str, rows: list[dict], merge_keys: tuple[str, ...]) -> None:
    if not rows:
        return
    df = spark.createDataFrame(rows)
    df.createOrReplaceTempView("_silver_updates")
    try:
        spark.table(table)
        table_exists = True
    except Exception:
        table_exists = False

    if not table_exists:
        df.write.format("delta").mode("overwrite").saveAsTable(table)
        return

    on_clause = " AND ".join(f"t.{k} <=> s.{k}" for k in merge_keys)
    set_clause = ", ".join(f"t.{c} = s.{c}" for c in df.columns if c not in merge_keys)
    insert_cols = ", ".join(df.columns)
    insert_vals = ", ".join(f"s.{c}" for c in df.columns)
    spark.sql(
        f"""
        MERGE INTO {table} t
        USING _silver_updates s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )


def _mark_processed(spark, catalog: str, silver_schema: str, report: str, filenames: list[str]) -> None:
    if not filenames:
        return
    marker_table = _processed_marker_table(catalog, silver_schema)
    now = dt.datetime.utcnow()
    rows = [Row(report=report, source_filename=fn, processed_at=now) for fn in filenames]
    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("append").saveAsTable(marker_table)


def _log_quality(spark, catalog: str, silver_schema: str, report: str, reports) -> None:
    now = dt.datetime.utcnow()
    rows = [
        Row(
            report=report,
            delivery_date=qr.delivery_date,
            checked_at=now,
            rows_checked=qr.rows_checked,
            missing_hours=qr.missing_hours,
            duplicate_hours=qr.duplicate_hours,
            out_of_range_count=len(qr.out_of_range_rows),
            is_clean=qr.is_clean,
        )
        for qr in reports
    ]
    if not rows:
        return
    df = spark.createDataFrame(rows)
    table = f"{catalog}.{silver_schema}.data_quality_log"
    df.write.format("delta").mode("append").saveAsTable(table)


if __name__ == "__main__":
    report_arg = sys.argv[1]
    catalog_arg = sys.argv[2]
    bronze_schema_arg = sys.argv[3] if len(sys.argv) > 3 else "bronze"
    silver_schema_arg = sys.argv[4] if len(sys.argv) > 4 else "silver"
    run(report_arg, catalog_arg, bronze_schema_arg, silver_schema_arg)
