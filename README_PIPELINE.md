# Phase 2 PySpark Lakehouse Execution Guide

The Phase 2 jobs use Delta Lake and are designed for Databricks Free Edition
serverless compute. The pipeline is limited to the required foundational
layers: Raw-to-Bronze and Bronze-to-Silver. Gold aggregation remains outside
this phase.

## Prerequisites

Use the Databricks Free Edition workspace and a Unity Catalog volume. Free
Edition is serverless-only, so use a volume path (`/Volumes/...`) rather than
the legacy DBFS/FileStore path. The notebook runner is
`Notebooks/phase2_databricks_run.py`.

## Standard incremental run

Set these values in the notebook and run all cells:

```python
raw_input_path = "/Volumes/<catalog>/<schema>/<volume>/raw/incremental/*.csv"
load_type = "incremental"
lakehouse_root = "/Volumes/<catalog>/<schema>/<volume>/lakehouse"
```

## Full historical initialization

```python
raw_input_path = "/Volumes/<catalog>/<schema>/<volume>/raw/full_load/*.csv"
load_type = "full"
```

## Historical backfill

Choose any historical folder, file, or glob—there is no date-specific path in
the code.

```python
raw_input_path = "/Volumes/<catalog>/<schema>/<volume>/raw/2024-10/*.csv"
load_type = "backfill"
```

The run creates these Delta paths below `--lakehouse-root`:

- `bronze/news_articles`
- `silver/news_articles`
- `quarantine/news_articles`
- `operations/pipeline_execution_logs`

Both layer writes use an explicit Delta `MERGE INTO` statement. Reprocessing the
same input produces no duplicate Bronze or Silver records; modified article
payloads update the existing Silver primary key instead. Invalid required
fields or non-empty unparseable date values are isolated in quarantine and do
not fail the entire batch. Unexpected source columns are also quarantined as
schema drift for review.
