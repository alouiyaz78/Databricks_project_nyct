# Databricks notebook source
import urllib.request
import os
import shutil 

try : 
    # construct the url for the parquet file correspondinf to yhis month
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
# open a connection and stream the remote file
    reponse  = urllib.request.urlopen(url)
    # define and create the local directory to yhis date's data
    dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/loockup/"
    os.makedirs(dir_path, exist_ok=True)
    # define the local file path
    file_path = f"{dir_path}/taxi_zone_lookup.csv"
    # save the streamed contend to the local file in binary mode
    with open(file_path, "wb") as f :
        shutil.copyfileobj(reponse, f)
    dbutils.jobs.taskValues.set(key = "continue_downstream", value = "yes")
    print("file is successfully uploaded")    
except Exception as e:
    dbutils.jobs.taskValues.set(key = "continue_downstream", value = "no")
    print(f"Error: {e}") 


# COMMAND ----------

