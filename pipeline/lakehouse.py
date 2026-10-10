"""Reusable Delta Lake helpers used by Bronze and Silver jobs."""

from __future__ import annotations

from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def is_delta_table(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def merge_with_metrics(
    spark: SparkSession, source: DataFrame, target_path: str, key: str,
) -> tuple[int, int]:
    """Idempotently upsert a DataFrame and return (inserted, updated) counts.

    ``payload_hash`` represents all business columns. Matching identical rows
    are no-ops, so rerunning the same raw input cannot duplicate Silver data.
    """
    if not is_delta_table(spark, target_path):
        inserted = source.count()
        source.write.format("delta").mode("overwrite").save(target_path)
        return inserted, 0

    target = spark.read.format("delta").load(target_path).select(
        F.col(key).alias("target_key"), F.col("payload_hash").alias("target_payload_hash")
    )
    metrics = source.select(key, "payload_hash").join(
        target, F.col(key) == F.col("target_key"), "left"
    ).agg(
        F.sum(F.when(F.col("target_key").isNull(), 1).otherwise(0)).alias("inserted"),
        F.sum(F.when(F.col("target_key").isNotNull() &
                     (F.col("payload_hash") != F.col("target_payload_hash")), 1)
              .otherwise(0)).alias("updated"),
    ).first()

    source_view = f"merge_source_{uuid4().hex}"
    source.createOrReplaceTempView(source_view)
    # Explicit SQL MERGE is compatible with Databricks Delta and supports
    # backfills safely, because the primary key determines the match.
    spark.sql(f"""
        MERGE INTO delta.`{target_path}` AS target
        USING {source_view} AS source
        ON target.`{key}` = source.`{key}`
        WHEN MATCHED AND target.payload_hash <> source.payload_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return int(metrics.inserted or 0), int(metrics.updated or 0)


def append_quarantine(df: DataFrame, quarantine_path: str) -> int:
    """Idempotently persist rejected records without failing the valid batch."""
    quarantined = df.withColumn(
        "quarantine_record_id",
        F.sha2(F.concat_ws("||", F.col("bronze_record_id"), F.col("quarantine_reason")), 256),
    )
    if not quarantined.take(1):
        return 0
    inserted, updated = merge_with_metrics(
        df.sparkSession, quarantined, quarantine_path, "quarantine_record_id"
    )
    return inserted + updated
