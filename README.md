### NYC Taxi Data ETL Pipeline

Overview

This project implements a scalable ETL pipeline on NYC taxi trip data using Databricks, Delta Lake, and Spark SQL.

It demonstrates:

Raw data ingestion → cleansing → enrichment → aggregation → export

Handling historical and incremental data

Using modular PySpark notebooks and reusable helper functions

Designing production-ready data pipelines in the cloud


Illustrative pipeline flow: Bronze → Silver → Gold → Export

# Project Structure

Layer	Description	Notebooks / Tables
00_landing	Initial raw data & lookup tables	backfill_historical_yellow_trips.py, load_taxi_zone_lookup.py
01_bronze	Raw trip data ingestion	yellow_trips_raw
02_silver	Cleansing & enrichment	taxi_zone_lookup, yellow_trips_cleansed, yellow_trips_enriched
03_gold	Daily aggregated summaries	daily_trip_summary
04_export	Export datasets for analytics	yellow_trips_export
Getting Started

Open the project in Databricks

Set up catalogs, schemas, and volumes

Run historical load notebooks (landing → bronze → silver → gold → export)

Use incremental notebooks for ongoing processing with job gates (continue_downstream)

Development Workflow & Tools

VS Code: main IDE for writing and organizing PySpark notebooks and scripts

GitHub: version control and repository synchronization

Databricks: execution environment for ETL notebooks and Delta Lake tables

# Workflow:

Code is written and tested locally in VS Code

Changes are committed and pushed to GitHub

Databricks workspace is synchronized with GitHub for execution

Helper Functions

file_exists(path) – verify file existence

download_file(url, dir_path, path) – fetch external data

add_processed_timestamp(df) – add ETL processing timestamp

get_target_yyyymm(months_ago) – compute target month for incremental loads

set_continue_downstream("yes"|"no") – control notebook execution flow

# Skills & Technologies

Databricks workspace management and ETL orchestration

Delta Lake CRUD operations and ACID-compliant pipelines

PySpark & Spark SQL for large-scale transformations

Incremental & historical data processing

Data analysis, aggregation, and reporting

Git & VS Code integration for professional workflow
