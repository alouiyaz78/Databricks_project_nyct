# Databricks notebook source
# DBTITLE 1,Cell 1
spark.sql("create catalog if not exists nyctaxi managed location '" 
          "abfss://unity-catalog-storage@dbstorage3x7g2gejydmkk.dfs.core.windows.net/7405616476401946'")

# COMMAND ----------

spark.sql("create schema if not exists nyctaxi.00_landing ")
spark.sql("create schema if not exists nyctaxi.01_silver ")
spark.sql("create schema if not exists nyctaxi.02_bronze ")
spark.sql("create schema if not exists nyctaxi.03_gold ")

# COMMAND ----------

spark.sql("create volume if not exists nyctaxi.00_landing.data_sources")