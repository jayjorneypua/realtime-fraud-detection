# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS fraud.bronze")

# COMMAND ----------

from pyspark.sql import functions as F

source = "/Volumes/fraud/landing/raw/fraudTrain.csv"

history_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(source)
    .drop("_c0")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

history_df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("fraud.bronze.transactions_history")

# COMMAND ----------

display(spark.sql("""
    SELECT COUNT(*) AS total_rows
         , SUM(CASE WHEN is_fraud = '1' THEN 1 ELSE 0 END) AS fraud_rows
         , ROUND(100 * SUM(CASE WHEN is_fraud = '1' THEN 1 ELSE 0 END) / COUNT(*), 3) AS fraud_pct
         , MIN(trans_date_trans_time) AS first_txn
         , MAX(trans_date_trans_time) AS last_txn
    FROM fraud.bronze.transactions_history
"""))