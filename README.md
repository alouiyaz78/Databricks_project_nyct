# NYC Taxi Data ETL Pipeline on Databricks

A medallion-architecture ETL pipeline on NYC yellow taxi trip data, built with **Databricks, Delta Lake, PySpark and Spark SQL**. It was developed on Databricks Free Edition.

## Goal

Turn raw taxi trip files into clean, enriched and aggregated tables that are ready for analysis, with both a one-off historical load and incremental monthly processing.

## What it demonstrates

- Raw data ingestion, cleansing, enrichment, aggregation and export
- Historical backfill and incremental processing
- Modular PySpark notebooks with reusable helper functions
- Delta Lake tables (ACID transactions) at every layer
- Notebook orchestration with job gates

## Architecture

```
00_landing  ->  01_bronze  ->  02_silver  ->  03_gold  ->  04_export
 raw files      raw tables     cleansed +     daily          analytics-ready
 + lookups                     enriched       summaries      dataset
```

| Layer | Description | Notebooks / tables |
|---|---|---|
| 00_landing | Initial raw data and lookup tables | `backfill_historical_yellow_trips.py`, `load_taxi_zone_lookup.py` |
| 01_bronze | Raw trip data ingestion | `yellow_trips_raw` |
| 02_silver | Cleansing and enrichment (trips joined with the taxi zone lookup) | `taxi_zone_lookup`, `yellow_trips_cleansed`, `yellow_trips_enriched` |
| 03_gold | Daily aggregated summaries | `daily_trip_summary` |
| 04_export | Datasets exported for analytics | `yellow_trips_export` |

## Getting started

1. Open the project in Databricks.
2. Set up catalogs, schemas and volumes.
3. Run the historical load notebooks (landing -> bronze -> silver -> gold -> export).
4. Use the incremental notebooks for ongoing processing. Job gates (`set_continue_downstream`) stop or continue the downstream notebooks.

## Helper functions

| Function | Purpose |
|---|---|
| `file_exists(path)` | Check that a file exists |
| `download_file(url, dir_path, path)` | Fetch external data |
| `add_processed_timestamp(df)` | Add an ETL processing timestamp |
| `get_target_yyyymm(months_ago)` | Compute the target month for incremental loads |
| `set_continue_downstream("yes"\|"no")` | Control notebook execution flow |

## Development workflow

Code is written and tested in VS Code, pushed to GitHub, and the Databricks workspace is synchronised with GitHub for execution.

## Skills and technologies

Databricks workspace and ETL orchestration, Delta Lake, PySpark, Spark SQL, incremental and historical processing, data aggregation and reporting, Git and VS Code.
