"""Orchestrate a parameterized Bronze and Silver Lakehouse run."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from pipeline.bronze_load import load_bronze
from pipeline.silver_transform import load_silver


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the PySpark Bronze/Silver news pipeline.")
    parser.add_argument("--input-path", required=True, help="Raw CSV path, folder, or historical backfill glob.")
    parser.add_argument("--load-type", choices=["full", "incremental", "backfill"], default="incremental")
    parser.add_argument("--lakehouse-root", default="data/lakehouse")
    args = parser.parse_args()
    root = args.lakehouse_root.rstrip("/")
    spark = SparkSession.builder.appName("news-lakehouse-phase-2").getOrCreate()
    audit = f"{root}/operations/pipeline_execution_logs"
    bronze = f"{root}/bronze/news_articles"
    silver = f"{root}/silver/news_articles"
    quarantine = f"{root}/quarantine/news_articles"
    load_bronze(spark, args.input_path, bronze, audit, args.load_type)
    load_silver(spark, bronze, silver, audit, quarantine)


if __name__ == "__main__":
    main()
