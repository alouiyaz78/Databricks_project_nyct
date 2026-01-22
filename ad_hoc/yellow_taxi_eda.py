# Databricks notebook source
# MAGIC %md
# MAGIC ### Which vendor makes the most revenue?

# COMMAND ----------

from pyspark.sql.functions import *


# COMMAND ----------

df_trips = spark.read.table("nyctaxi.01_silver.yellow_trips_cleansed")

# COMMAND ----------

df_trips.display()

# COMMAND ----------

from pyspark.sql.functions import sum, round; df_trips.groupBy("vendor").agg(round(sum("total_amount"), 2).alias("total_revenue")).orderBy("total_revenue", ascending=False).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Which is the most popular pickup borough ?

# COMMAND ----------

df= spark.read.table("nyctaxi.01_silver.yellow_trips_enriched")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is the common journey ?

# COMMAND ----------

df.groupby("pu_borough","do_borough").agg(count("*").alias("number_of_trips")).orderBy("number_of_trips", ascending=False).display()

# COMMAND ----------

df.groupby(concat("pu_borough", lit("-->"), "do_borough").alias("journey")).agg(count("*").alias("number_of_trips")).orderBy("number_of_trips", ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a time series chart showing the number of trips and total revenus per day

# COMMAND ----------

df_2 = spark.read.table("nyctaxi.03_gold.daily_trips_summary")
df_2.display()

# COMMAND ----------

