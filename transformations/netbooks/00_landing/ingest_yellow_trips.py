# Databricks notebook source
import sys
import os

# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

# COMMAND ----------
from datetime import date
from dateutil.relativedelta import relativedelta

from modules.utils.date_utils import get_target_yyyy_mm
from modules.data_loader.file_downloader import download_file

# COMMAND ----------
# Get target month (2 months ago) in YYYY-MM format

formatted_date = get_target_yyyy_mm(2)

# COMMAND ----------
# Define paths (Unity Catalog Volume)

# Folder path
dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"

# File path (Volume path)
local_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"

# DBFS path (used for dbutils.fs.ls)
dbfs_path = f"dbfs:{local_path}"

# Local filesystem path (used for Python file writing)
local_fs_path = f"/dbfs{local_path}"

# COMMAND ----------
# Define the URL

url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"

print("=======================================")
print(f"Target month: {formatted_date}")
print(f"Download URL: {url}")
print(f"Volume folder path: {dir_path}")
print(f"Volume file path: {local_path}")
print(f"DBFS path (for ls): {dbfs_path}")
print(f"Local FS path (for writing): {local_fs_path}")
print("=======================================")

# COMMAND ----------
# Check if the file already exists

try:
    dbutils.fs.ls(dbfs_path)

    # If the file already exists then set continue_downstream to no
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print("File already downloaded, aborting downstream tasks.")
    print("continue_downstream = no")

except Exception:
    try:
        # Download the file into the Volume folder
        download_file(url, f"/dbfs{dir_path}", local_fs_path)

        # Set continue_downstream to yes if the file was loaded
        dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
        print("File successfully uploaded in current run.")
        print("continue_downstream = yes")

    except Exception as e:
        # Set continue downstream to no if the file was not loaded
        dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
        print(f"File download failed: {str(e)}")
        print("continue_downstream = no")
