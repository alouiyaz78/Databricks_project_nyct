# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

df = spark.read.format("parquet").load("/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/*")

# COMMAND ----------

df = df.withColumn("processed_timestamp", current_timestamp())


# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.01_silver.yellow_trips_cleansed")



