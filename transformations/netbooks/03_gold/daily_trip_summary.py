# Databricks notebook source
import sys
import os
from pyspark.sql.functions import count,sum,avg,min,max,round
from datetime import date
from dateutil.relativedelta import relativedelta
from modules.utils.date_utils import get_mont_start_n_moth_ago

# COMMAND ----------

# get the first day of the month two month ago 
two_month_ago_start = get_mont_start_n_moth_ago(2)

# COMMAND ----------

df = spark.read.table("nyctaxi.01_silver.yellow_trips_enriched").filter(f"tpep_pickup_datetime >= '{two_month_ago_start}'" )

# COMMAND ----------

df = df.\
    groupby(df.tpep_pickup_datetime.cast("date").alias("pickup_date")).\
    agg(count("*").alias("total_trips"),
         round(avg("passenger_count"),1).alias("avg_passenger"),
         round(avg("trip_distance"),1).alias("avg_distance"),
         round(avg("fare_amount"),1).alias("avg_fare_per_trip"),
         max("fare_amount").alias("max_fare"),
         min("fare_amount").alias("min_fare"),
         round(sum("total_amount"),2).alias("total_amount")
    )
    

# COMMAND ----------

df.write.mode("append").saveAsTable("nyctaxi.03_gold.daily_trips_summary")