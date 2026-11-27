# Databricks notebook source
# Bronze live: picks up only the files the lakehouse sink landed since the last run (Auto Loader, available now),
# every field kept as a string like bronze history, plus load metadata.
# /Volumes/fraud/landing/live/<topic>/ -> fraud.bronze.transactions_live, transaction_labels_live, scores_live
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

landing_volume: str = "/Volumes/fraud/landing/live"
checkpoint_root: str = f"{landing_volume}/_checkpoints"
table_by_topic: dict[str, str] = {
    "transactions": "fraud.bronze.transactions_live",
    "transaction-labels": "fraud.bronze.transaction_labels_live",
    "scores": "fraud.bronze.scores_live",
}

for topic, table_name in table_by_topic.items():
    checkpoint_folder: str = f"{checkpoint_root}/{topic}"
    topic_stream: DataFrame = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", False)
            .option("cloudFiles.schemaLocation", checkpoint_folder)
            .load(f"{landing_volume}/{topic}")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
    )
    (
        topic_stream.writeStream
            .option("checkpointLocation", checkpoint_folder)
            .trigger(availableNow=True)
            .toTable(table_name)
            .awaitTermination()
    )