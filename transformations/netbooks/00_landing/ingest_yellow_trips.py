# Databricks notebook source
import urllib.request
import os
import shutil
from datetime import date
from dateutil.relativedelta import relativedelta

# COMMAND ----------
# Compute target month (2 months ago) in YYYY-MM format

two_months_ago = date.today() - relativedelta(months=2)
formatted_date = two_months_ago.strftime("%Y-%m")

print(f"Target month: {formatted_date}")

# COMMAND ----------
# Define paths and URL

dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"
local_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"

url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"

print(f"Download URL: {url}")
print(f"Local path: {local_path}")

# COMMAND ----------
# Check if file already ex
