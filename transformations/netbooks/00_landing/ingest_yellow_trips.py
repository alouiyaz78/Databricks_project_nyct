# Databricks notebook source
import sys
import os

project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))
if project_root not in sys.path:
    sys.path.append(project_root)

# COMMAND ----------
from modules.utils.date_utils import get_target_yyyy_mm

# COMMAND ----------
formatted_date = get_target_yyyy_mm(3)

dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"
local_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"

dbfs_dir = f"dbfs:{dir_path}"
dbfs_file = f"dbfs:{local_path}"

url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"

print(f"formatted_date=[{formatted_date}]")
print(f"url=[{url}]")
print(f"dbfs_file=[{dbfs_file}]")

# COMMAND ----------
def set_gate(value: str) -> None:
    v = (value or "").strip().lower()
    dbutils.jobs.taskValues.set(key="continue_downstream", value=v)
    print(f"continue_downstream=[{v}]")

# COMMAND ----------
# 1) If file exists, stop downstream
try:
    dbutils.fs.ls(dbfs_file)
    set_gate("no")
    print("File already exists -> aborting downstream tasks")
except Exception:
    try:
        # 2) Ensure target directory exists
        try:
            dbutils.fs.ls(dbfs_dir)
        except Exception:
            dbutils.fs.mkdirs(dbfs_dir)

        # 3) Download directly into DBFS/Volume
        dbutils.fs.cp(url, dbfs_file)

        set_gate("yes")
        print("File successfully downloaded to Volume via dbutils.fs.cp(url, dst)")
    except Exception as e:
        set_gate("no")
        print(f"Download failed: {type(e).__name__}: {str(e)}")

# COMMAND ----------
# Debug (only works inside Job runs)
try:
    v = dbutils.jobs.taskValues.get(taskKey=None, key="continue_downstream", debugValue="missing")
    print(f"DEBUG saved continue_downstream=[{v}]")
except Exception as e:
    print(f"DEBUG skipped (not running as a Job task): {type(e).__name__}")
