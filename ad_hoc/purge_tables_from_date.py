# Databricks notebook source
# Update this so that the date is the start of the month that was 2 months prior to the current date
date_from = '2025-11-01'

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "nyctaxi.`02_bronze`.yellow_trips_raw")

dt.delete(f"tpep_pickup_datetime >= '{date_from}'")

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "nyctaxi.`01_silver`.yellow_trips_cleansed")

dt.delete(f"tpep_pickup_datetime >= '{date_from}'")

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "nyctaxi.`01_silver`.yellow_trips_enriched")

dt.delete(f"tpep_pickup_datetime >= '{date_from}'")

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "nyctaxi.`03_gold`.daily_trips_summary")

dt.delete(f"pickup_date >= '{date_from}'")

# COMMAND ----------

spark.read.table("nyctaxi.`04_export`.yellow_trips_export").\
    groupBy("year_month").\
    agg(count("*").alias("total_records")).\
    orderBy("year_month").display()

# COMMAND ----------

spark.read.table("nyctaxi.`01_silver`.taxi_zone_lookup").display()