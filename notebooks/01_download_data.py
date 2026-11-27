# Databricks notebook source
# Download: Kaggle credit card transactions (kartik2112/fraud-detection) into the landing volume.
# fraudTrain.csv is the training history, fraudTest.csv is replayed later as the live feed.
# kaggle -> /Volumes/fraud/landing/kaggle

# COMMAND ----------

# MAGIC %pip install "kaggle>=1.8"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import shutil

# the token has to be in the environment before the kaggle package is imported
os.environ["KAGGLE_API_TOKEN"] = dbutils.secrets.get("fraud", "kaggle_api_token")

from kaggle.api.kaggle_api_extended import KaggleApi

dataset_name: str = "kartik2112/fraud-detection"
download_folder: str = "/tmp/kaggle_fraud"
landing_folder: str = "/Volumes/fraud/landing/kaggle"

os.makedirs(download_folder, exist_ok=True)

kaggle_api: KaggleApi = KaggleApi()
kaggle_api.authenticate()
kaggle_api.dataset_download_files(dataset_name, path=download_folder, unzip=True)

# download to local disk first, then copy only the csv files to the volume
for file_name in os.listdir(download_folder):
    if file_name.endswith(".csv"):
        shutil.copy(os.path.join(download_folder, file_name), os.path.join(landing_folder, file_name))

display(dbutils.fs.ls(landing_folder))