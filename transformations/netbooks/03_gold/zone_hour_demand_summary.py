# Databricks notebook source
import sys
import os

# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))
if project_root not in sys.path:
    sys.path.append(project_root)

from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum,
    round,
    hour
)

from modules.utils.date_utils import get_month_start_n_months_ago

# COMMAND ----------
# Define time window (last completed month)

two_months_ago_start = get_month_start_n_months_ago(2)

# COMMAND ----------
# Load enriched Silver data

df = (
    spark.read
    .table("nyctaxi.01_silver.yellow_trips_enriched")
    .filter(col("tpep_pickup_datetime") >= two_months_ago_start)
)

# COMMAND ----------
# Create optimization-ready demand features

df_gold = (
    df
    .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
    .groupBy(
        "pickup_zone",
        "pickup_hour"
    )
    .agg(
        count("*").alias("trip_count"),
        round(avg("trip_duration"), 1).alias("avg_trip_duration"),
        round(avg("trip_distance"), 1).alias("avg_trip_distance"),
        round(sum("total_amount"), 2).alias("estimated_revenue")
    )
)

# COMMAND ----------
# Write optimization-ready Gold table

df_gold.write.mode("append").saveAsTable(
    "nyctaxi.03_gold.zone_hour_demand_summary"
)
