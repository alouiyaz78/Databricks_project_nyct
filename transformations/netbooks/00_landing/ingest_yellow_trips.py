# Databricks notebook source
import sys
import os
import urllib.request
import shutil

project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))
if project_root not in sys.path:
    sys.path.append(project_root)

# COMMAND ----------
from modules.utils.date_utils import get_target_yyyy_mm

# COMMAND ----------
# Get target month (3 months ago)
formatted_date = get_target_yyyy_mm(3)

# Target paths in Unity Catalog Volume
dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"
local_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"

# DBFS-style path for dbutils.fs operations
dbfs_dir = f"dbfs:{dir_path}"
dbfs_file = f"dbfs:{local_path}"

# Remote URL
url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"

# Temp local path (driver local disk)
tmp_dir = f"/tmp/nyctaxi_yellow/{formatted_date}"
tmp_file = f"{tmp_dir}/yellow_tripdata_{formatted_date}.parquet"

print(f"formatted_date=[{formatted_date}]")
print(f"url=[{url}]")
print(f"dbfs_file=[{dbfs_file}]")
print(f"tmp_file=[{tmp_file}]")

# COMMAND ----------
def set_gate(value: str) -> None:
    v = (value or "").strip().lower()
    dbutils.jobs.taskValues.set(key="continue_downstream", value=v)
    print(f"continue_downstream=[{v}]")

# COMMAND ----------
# 1) Check if file exists already
try:
    dbutils.fs.ls(dbfs_file)
    set_gate("no")
    print("File already exists -> aborting downstream tasks")
except Exception:
    # 2) Download to /tmp
    try:
        os.makedirs(tmp_dir, exist_ok=True)
        with urllib.request.urlopen(url) as response, open(tmp_file, "wb") as f:
            shutil.copyfileobj(response, f)

        print("Downloaded to /tmp successfully.")

        # 3) Ensure target directory exists in the Volume (dbutils.fs)
        try:
            dbutils.fs.ls(dbfs_dir)
        except Exception:
            dbutils.fs.mkdirs(dbfs_dir)

        # 4) Copy from local file to DBFS/Volume
        dbutils.fs.cp(f"file:{tmp_file}", dbfs_file)

        print("Copied file into Unity Catalog Volume successfully.")
        set_gate("yes")

    except Exception as e:
        set_gate("no")
        print(f"File download/copy failed: {type(e).__name__}: {str(e)}")

# COMMAND ----------
# Debug (only works inside Job runs)
try:
    v = dbutils.jobs.taskValues.get(taskKey=None, key="continue_downstream", debugValue="missing")
    print(f"DEBUG saved continue_downstream=[{v}]")
except Exception as e:
    print(f"DEBUG skipped (not running as a Job task): {type(e).__name__}")
