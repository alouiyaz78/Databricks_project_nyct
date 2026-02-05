# Databricks notebook source
import sys
import os

# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))
if project_root not in sys.path:
    sys.path.append(project_root)

# COMMAND ----------
from modules.utils.date_utils import get_target_yyyy_mm
from modules.data_loader.file_downloader import download_file

# COMMAND ----------
# Get target month (3 months ago) in YYYY-MM format
formatted_date = get_target_yyyy_mm(3)

# COMMAND ----------
# Define paths (Unity Catalog Volume)
dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"
local_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"

# DBFS path for existence check
dbfs_path = f"dbfs:{local_path}"

# URL
url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"

print(f"formatted_date=[{formatted_date}]")
print(f"dir_path=[{dir_path}]")
print(f"local_path=[{local_path}]")
print(f"dbfs_path=[{dbfs_path}]")
print(f"url=[{url}]")

# COMMAND ----------
def set_gate(value: str) -> None:
    v = (value or "").strip().lower()
    dbutils.jobs.taskValues.set(key="continue_downstream", value=v)
    print(f"continue_downstream=[{v}]")

# COMMAND ----------
# Check if the file already exists
try:
    dbutils.fs.ls(dbfs_path)

    set_gate("no")
    print("File already downloaded, aborting downstream tasks")

except Exception:
    try:
        # Download the file into the target folder/file
        download_file(url, dir_path, local_path)

        set_gate("yes")
        print("File successfully uploaded in current run")

    except Exception as e:
        set_gate("no")
        print(f"File download failed: {str(e)}")

# COMMAND ----------
# Debug (works only inside Job runs)
try:
    v = dbutils.jobs.taskValues.get(taskKey=None, key="continue_downstream", debugValue="missing")
    print(f"DEBUG saved continue_downstream=[{v}]")
except Exception as e:
    print(f"DEBUG skipped (not running as a Job task): {type(e).__name__}")
