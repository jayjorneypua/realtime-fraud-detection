# Databricks notebook source
# MAGIC %pip install "kaggle>=1.8"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS fraud")
spark.sql("CREATE SCHEMA IF NOT EXISTS fraud.landing")
spark.sql("CREATE VOLUME IF NOT EXISTS fraud.landing.raw")

# COMMAND ----------

import os
import shutil

os.environ["KAGGLE_API_TOKEN"] = dbutils.secrets.get("fraud", "kaggle_api_token")

from kaggle.api.kaggle_api_extended import KaggleApi

tmp_dir = "/tmp/kaggle_fraud"
landing = "/Volumes/fraud/landing/raw"

os.makedirs(tmp_dir, exist_ok=True)

api = KaggleApi()
api.authenticate()
api.dataset_download_files("kartik2112/fraud-detection", path=tmp_dir, unzip=True)

for name in os.listdir(tmp_dir):
    if name.endswith(".csv"):
        shutil.copy(os.path.join(tmp_dir, name), os.path.join(landing, name))

# COMMAND ----------

display(dbutils.fs.ls(landing))