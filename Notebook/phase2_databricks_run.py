# Databricks notebook source
# COMMAND ----------
# Phase 2 Lakehouse runner for Databricks Free Edition (serverless).
#
# Before running, replace the values below with paths in a Unity Catalog volume.
# Example raw path:
# /Volumes/<catalog>/<schema>/<volume>/raw/incremental/*.csv
# Example Lakehouse root:
# /Volumes/<catalog>/<schema>/<volume>/lakehouse

raw_input_path = "/Volumes/<catalog>/<schema>/<volume>/raw/incremental/*.csv"
load_type = "incremental"  # full | incremental | backfill
lakehouse_root = "/Volumes/<catalog>/<schema>/<volume>/lakehouse"

# COMMAND ----------
# The repository is a Databricks Git folder. Add it to the import path if the
# notebook is not being run from the repository root.
import sys

# Copy your Git folder path from the Databricks Workspace browser and replace
# this placeholder. Serverless does not guarantee a notebook's working folder.
repo_root = "/Workspace/Users/<your-email>/<repository-name>"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from pipeline.bronze_load import load_bronze
from pipeline.silver_transform import load_silver

# COMMAND ----------
spark.conf.set("spark.sql.session.timeZone", "UTC")

audit_path = f"{lakehouse_root}/operations/pipeline_execution_logs"
bronze_path = f"{lakehouse_root}/bronze/news_articles"
silver_path = f"{lakehouse_root}/silver/news_articles"
quarantine_path = f"{lakehouse_root}/quarantine/news_articles"

bronze_metrics = load_bronze(
    spark, raw_input_path, bronze_path, audit_path, load_type
)
silver_metrics = load_silver(
    spark, bronze_path, silver_path, audit_path, quarantine_path
)

print(f"Bronze inserted/updated: {bronze_metrics}")
print(f"Silver inserted/updated: {silver_metrics}")

# COMMAND ----------
# Inspect results and retain these outputs/screenshots as submission evidence.
display(spark.read.format("delta").load(silver_path))
display(spark.read.format("delta").load(audit_path).orderBy("execution_end_time", ascending=False))
