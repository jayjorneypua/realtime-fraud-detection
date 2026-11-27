# Databricks notebook source
# Bronze: fraudTrain.csv loaded as received, every column a string, plus load metadata.
# Typing and cleaning happen in silver.
# /Volumes/fraud/landing/kaggle/fraudTrain.csv -> fraud.bronze.transactions_history
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

source_file: str = "/Volumes/fraud/landing/kaggle/fraudTrain.csv"

transactions_history: DataFrame = (
    spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(source_file)
        .drop("_c0")  # unnamed row index column in the source file
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
)

transactions_history.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("fraud.bronze.transactions_history")