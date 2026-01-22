# Databricks notebook source
df_trips = spark.read.table("nyctaxi.01_silver.yellow_trips_cleansed")

# COMMAND ----------

df_trips.display()

# COMMAND ----------

df_zone = spark.read.table("nyctaxi.01_silver.taxi_zone_lookup")


# COMMAND ----------

df_join1 = df_trips.join(
    df_zone,
    df_trips.pu_location_id == df_zone.location_id,
    how="left"
).select(
df_trips.vendor,
df_trips.tpep_pickup_datetime,
df_trips.tpep_dropoff_datetime,
df_trips.trip_duration,
df_trips.passenger_count,
df_trips.trip_distance,
df_trips.rate_type,
df_zone.borough.alias("pu_borough"),
df_zone.zone.alias("pu_zone"),
df_trips.do_location_id,
df_trips.payment_type,
df_trips.fare_amount,
df_trips.extra,
df_trips.mta_tax,
df_trips.tolls_amount,
df_trips.improvement_surcharge,
df_trips.total_amount,
df_trips.congestion_surcharge,
df_trips.airport_fee,
df_trips.cbd_congestion_fee,
df_trips.processed_timestamp




)
                    

# COMMAND ----------

df_join_final = df_join1.join(
    df_zone,
    df_join1.do_location_id == df_zone.location_id,
    how="left"
).select(
df_join1.vendor,
df_join1.tpep_pickup_datetime,
df_join1.tpep_dropoff_datetime,
df_join1.trip_duration,
df_join1.passenger_count,
df_join1.trip_distance,
df_join1.rate_type,
df_join1.pu_borough,
df_zone.borough.alias("do_borough"),
df_join1.pu_zone,
df_zone.zone.alias("do_zone"),
df_join1.payment_type,
df_join1.fare_amount,
df_join1.extra,
df_join1.mta_tax,
df_join1.tolls_amount,
df_join1.improvement_surcharge,
df_join1.total_amount,
df_join1.congestion_surcharge,
df_join1.airport_fee,
df_join1.cbd_congestion_fee,
df_join1.processed_timestamp




)

# COMMAND ----------

df_join_final.write.mode("overwrite").saveAsTable("nyctaxi.01_silver.yellow_trips_enriched")

# COMMAND ----------

