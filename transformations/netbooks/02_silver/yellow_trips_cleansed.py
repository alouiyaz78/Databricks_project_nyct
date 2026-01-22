# Databricks notebook source
import sys
import os

# Go two levels up to reach the project_root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../../"))

if project_root not in sys.path:
    sys.path.append(project_root)

from pyspark.sql.functions import col, when, timestamp_diff
from datetime import date
from dateutil.relativedelta import relativedelta
from modules.utils.date_utils import get_mont_start_n_moth_ago
# COMMAND ----------

# get the first day of the month two month ago 
two_month_ago_start = get_mont_start_n_moth_ago(2)

# get the first day of the moth one month ago
one_month_ago_start = get_mont_start_n_moth_ago(1)      

# COMMAND ----------

df = spark.read.table("nyctaxi.02_bronze.yellow_trips_raw").filter(
    f"tpep_pickup_datetime >= '{two_month_ago_start}' and tpep_pickup_datetime <= '{one_month_ago_start   }'")

# COMMAND ----------

df = df.select(
when(col("VendorID") ==1 ,"Creative Mobile technologie , LLC")
.when(col("VendorID") ==2 ,"Curb Mobility , LLC")
.when(col("VendorID") ==6 ,"Myle technologie Inc")
.when(col("VendorID") ==7 ,"Helix")
.otherwise("unknown").alias("vendor"),
"tpep_pickup_datetime",
"tpep_dropoff_datetime",
timestamp_diff('Minute',df.tpep_dropoff_datetime, df.tpep_pickup_datetime).alias("trip_duration"),
"passenger_count",
"trip_distance",
when(col("RatecodeID") ==1 ,"Standard rate")
.when(col("RatecodeID") ==2 ,"JFK")
.when(col("RatecodeID") ==3 ,"Newark")
.when(col("RatecodeID") ==4 ,"Nassau or Westchester")
.when(col("RatecodeID") ==5 ,"Negotiated fare")
.when(col("RatecodeID") ==6 ,"Group ride")
.otherwise("unknown").alias("rate_type"),
"store_and_fwd_flag",
col("PULocationID").alias("pu_location_id"),
col("DOLocationID").alias("do_location_id"),
when(col("payment_type") ==0 ,"Flex Fare trip")
.when(col("payment_type") ==1 ,"Credit card")
.when(col("payment_type") ==2 ,"Cash")
.when(col("payment_type") ==3 ,"No charge")
.when(col("payment_type") ==4 ,"Dispute")
.when(col("payment_type") ==6 ,"Voided trip")
.otherwise("unknown").alias("payment_type"),
"fare_amount",
"extra",
"mta_tax",
"tolls_amount",
"improvement_surcharge",
"total_amount",
"congestion_surcharge",
col("Airport_fee").alias("airport_fee"),
"cbd_congestion_fee",
"processed_timestamp"



)









# COMMAND ----------

df.write.mode("append").saveAsTable("nyctaxi.01_silver.yellow_trips_cleansed")