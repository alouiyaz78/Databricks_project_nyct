# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from datetime import date
from dateutil.relativedelta import relativedelta
from modules.transformations.metadata import add_processed_timestamp
from modeules.utils.date_utils import get_target_yyyy_mm

# COMMAND ----------


# Obtains the year-month for 2 months prior to the current month in yyyy-MM format
formatted_date = get_target_yyyy_mm(2)

# Read all Parquet files for the specified month from the landing directory into a DataFrame
df = spark.read.format("parquet").load(f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}")

# COMMAND ----------

# Add a column to capture when the data was processed
df = add_processed_timestamp(df)


# COMMAND ----------

# Write the DataFrame to a Unity Catalog managed Delta table in the bronze schema, appending the new data
df.write.mode("append").saveAsTable("nyctaxi.02_bronze.yellow_trips_raw")