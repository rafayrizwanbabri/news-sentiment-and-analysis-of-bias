"""Operational audit logging for all Lakehouse pipeline executions."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (LongType, StringType, StructField, StructType,
                               TimestampType)


EXECUTION_LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("parameter_processed", StringType(), False),
    StructField("execution_start_time", TimestampType(), False),
    StructField("execution_end_time", TimestampType(), False),
    StructField("status", StringType(), False),
    StructField("rows_inserted", LongType(), False),
    StructField("rows_updated", LongType(), False),
    StructField("error_message", StringType(), True),
])


def new_run_id() -> str:
    return str(uuid4())


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def write_execution_log(
    spark: SparkSession, log_path: str, *, run_id: str, layer: str,
    parameter_processed: str, started_at: datetime, status: str,
    rows_inserted: int = 0, rows_updated: int = 0, error_message: str | None = None,
) -> None:
    """Append one immutable audit event. This is deliberately never merged."""
    row = [(run_id, layer, parameter_processed, started_at, utc_now(), status,
            rows_inserted, rows_updated, error_message)]
    spark.createDataFrame(row, EXECUTION_LOG_SCHEMA).withColumn(
        "load_timestamp", F.current_timestamp()
    ).write.format("delta").mode("append").save(log_path)
