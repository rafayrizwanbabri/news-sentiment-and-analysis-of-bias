"""Raw CSV to Bronze Delta ingestion with explicit schema-on-read."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.audit import new_run_id, utc_now, write_execution_log
from pipeline.lakehouse import merge_with_metrics
from pipeline.schemas import RAW_COLUMNS, RAW_NEWS_SCHEMA


def _extra_source_columns(spark: SparkSession, input_path: str) -> list[str]:
    """Inspect CSV header separately, preserving awareness of added columns."""
    # ``spark.read.text`` is supported on Databricks serverless compute.  Do
    # not use SparkContext.wholeTextFiles here: that legacy API is unavailable
    # on serverless Free Edition workspaces.
    first_line = spark.read.text(input_path).limit(1).collect()
    if not first_line:
        return []
    header = first_line[0]["value"].lstrip("\ufeff").split(",")
    return [name for name in header if name not in RAW_COLUMNS]


def read_raw_news(spark: SparkSession, input_path: str) -> DataFrame:
    """Read source files using a declared String schema; schema inference is never used."""
    drift_columns = _extra_source_columns(spark, input_path)
    return (spark.read.schema(RAW_NEWS_SCHEMA).option("header", True)
            .option("multiLine", True).option("mode", "PERMISSIVE").csv(input_path)
            .withColumn("source_file", F.input_file_name())
            .withColumn("source_extra_columns", F.lit(",".join(drift_columns)))
            .withColumn("load_timestamp", F.current_timestamp())
            .withColumn("bronze_record_id", F.sha2(F.concat_ws("||", F.col("source_file"), *[
                F.coalesce(F.col(column), F.lit("")) for column in RAW_COLUMNS
            ]), 256))
            .withColumn("payload_hash", F.sha2(F.concat_ws("||", *[
                F.coalesce(F.col(column), F.lit("")) for column in RAW_COLUMNS
            ]), 256))
            .dropDuplicates(["bronze_record_id"]))


def load_bronze(spark: SparkSession, input_path: str, bronze_path: str,
                audit_path: str, load_type: str = "incremental") -> tuple[int, int]:
    run_id, started_at = new_run_id(), utc_now()
    layer = "Raw-to-Bronze"
    try:
        bronze = read_raw_news(spark, input_path).withColumn("load_type", F.lit(load_type))
        inserted, updated = merge_with_metrics(spark, bronze, bronze_path, "bronze_record_id")
        write_execution_log(spark, audit_path, run_id=run_id, layer=layer,
                            parameter_processed=input_path, started_at=started_at,
                            status="Success", rows_inserted=inserted, rows_updated=updated)
        return inserted, updated
    except Exception as exc:
        write_execution_log(spark, audit_path, run_id=run_id, layer=layer,
                            parameter_processed=input_path, started_at=started_at,
                            status="Failure", error_message=str(exc)[:4000])
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Load raw news CSV files into Bronze Delta.")
    parser.add_argument("--input-path", required=True, help="CSV file, folder, or glob for a backfill.")
    parser.add_argument("--bronze-path", default="data/lakehouse/bronze/news_articles")
    parser.add_argument("--audit-path", default="data/lakehouse/operations/pipeline_execution_logs")
    parser.add_argument("--load-type", choices=["full", "incremental", "backfill"], default="incremental")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("news-raw-to-bronze").getOrCreate()
    load_bronze(spark, **vars(args))


if __name__ == "__main__":
    main()
