"""Bronze to Silver typed transformation, quarantine, and Delta MERGE."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipeline.audit import new_run_id, utc_now, write_execution_log
from pipeline.lakehouse import append_quarantine, merge_with_metrics


def _clean_text(column: str) -> F.Column:
    return F.trim(F.coalesce(F.col(column), F.lit("")))


def transform_bronze_to_silver(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return typed valid records and invalid records for quarantine."""
    typed = (bronze
        .withColumn("source", _clean_text("Source"))
        .withColumn("article_url", _clean_text("Link"))
        .withColumn("headline", _clean_text("Headline"))
        .withColumn("description", _clean_text("Description"))
        .withColumn("topic", _clean_text("Topic"))
        .withColumn("author", _clean_text("Author"))
        .withColumn("region", _clean_text("Region"))
        .withColumn("article_content", _clean_text("Article_Content"))
        .withColumn("sentiment_bias", _clean_text("Sentiment_Bias"))
        .withColumn("keywords", _clean_text("Keywords"))
        .withColumn("published_timestamp", F.coalesce(
            F.to_timestamp("Timestamp", "d MMM yyyy"), F.to_timestamp("Timestamp"),
            F.to_timestamp("Date", "yyyy-MM-dd")))
        .withColumn("published_date", F.to_date("Date", "yyyy-MM-dd"))
        .withColumn("article_id", F.sha2(F.concat_ws("||", F.when(F.length("Link") > 0, F.col("Link")).otherwise(
            F.concat_ws("|", F.col("Source"), F.col("Headline"), F.col("Timestamp")))), 256))
        .withColumn("load_timestamp", F.current_timestamp()))
    invalid_reason = F.when(F.length("source_extra_columns") > 0,
                            F.lit("unexpected source columns: schema drift")) \
        .when(F.length("article_url") == 0, F.lit("missing article URL")) \
        .when(F.length("headline") == 0, F.lit("missing headline")) \
        .when((F.length(F.trim(F.coalesce(F.col("Timestamp"), F.lit("")))) > 0) &
              F.col("published_timestamp").isNull(), F.lit("invalid Timestamp type/format")) \
        .when((F.length(F.trim(F.coalesce(F.col("Date"), F.lit("")))) > 0) &
              F.col("published_date").isNull(), F.lit("invalid Date type/format"))
    invalid = typed.withColumn("quarantine_reason", invalid_reason).filter(F.col("quarantine_reason").isNotNull())
    valid = typed.filter(invalid_reason.isNull()).select(
        "article_id", "source", "article_url", "headline", "description", "published_timestamp",
        "published_date", "topic", "author", "region", "article_content", "sentiment_bias",
        "keywords", "source_file", "bronze_record_id", "load_type", "source_extra_columns", "load_timestamp"
    ).withColumn("payload_hash", F.sha2(F.concat_ws("||", *[
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in [
            "source", "article_url", "headline", "description", "published_timestamp", "published_date",
            "topic", "author", "region", "article_content", "sentiment_bias", "keywords"
        ]
    ]), 256))
    # A source batch can contain repeated URLs. Pick one deterministic winner
    # before MERGE, which also avoids Delta's multiple-source-match error.
    valid = valid.withColumn(
        "_row_number", F.row_number().over(
            Window.partitionBy("article_id").orderBy(F.col("bronze_record_id").desc())
        )
    ).filter(F.col("_row_number") == 1).drop("_row_number")
    return valid, invalid


def load_silver(spark: SparkSession, bronze_path: str, silver_path: str, audit_path: str,
                quarantine_path: str) -> tuple[int, int]:
    run_id, started_at = new_run_id(), utc_now()
    layer = "Bronze-to-Silver"
    try:
        bronze = spark.read.format("delta").load(bronze_path)
        valid, invalid = transform_bronze_to_silver(bronze)
        append_quarantine(invalid, quarantine_path)
        inserted, updated = merge_with_metrics(spark, valid, silver_path, "article_id")
        write_execution_log(spark, audit_path, run_id=run_id, layer=layer,
                            parameter_processed=bronze_path, started_at=started_at,
                            status="Success", rows_inserted=inserted, rows_updated=updated)
        return inserted, updated
    except Exception as exc:
        write_execution_log(spark, audit_path, run_id=run_id, layer=layer,
                            parameter_processed=bronze_path, started_at=started_at,
                            status="Failure", error_message=str(exc)[:4000])
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Transform Bronze news records into Silver Delta.")
    parser.add_argument("--bronze-path", default="data/lakehouse/bronze/news_articles")
    parser.add_argument("--silver-path", default="data/lakehouse/silver/news_articles")
    parser.add_argument("--audit-path", default="data/lakehouse/operations/pipeline_execution_logs")
    parser.add_argument("--quarantine-path", default="data/lakehouse/quarantine/news_articles")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("news-bronze-to-silver").getOrCreate()
    load_silver(spark, **vars(args))


if __name__ == "__main__":
    main()
