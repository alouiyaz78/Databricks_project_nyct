# Databricks notebook source
import sys
import os

# Go to levels up to reach thee project_root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)
import urllib.request
import shutil   
from modules.data_loader.file_downloader import dowload_file    


try : 
    # construct the url for the parquet file correspondinf to yhis month
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    
    # define and create the local directory to yhis date's data
    dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/loockup/"
    
    # define the local file path
    local_path = f"{dir_path}/taxi_zone_lookup.csv"
    
    # download the file using the reusable function
    dowload_file(url, dir_path, local_path)
    
    dbutils.jobs.taskValues.set(key = "continue_downstream", value = "yes")
    print("file is successfully uploaded")    
except Exception as e:
    dbutils.jobs.taskValues.set(key = "continue_downstream", value = "no")
    print(f"Error: {e}") 


# COMMAND ----------

