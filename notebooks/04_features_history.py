# Databricks notebook source
# Gold: per-card features for training, using only the card's past and the current transaction (no leakage).
# The features live in sql/transaction_features.sql. The scoring service computes the same features
# online in Redis, and a parity test checks both give the same values.
# fraud.silver.transactions_history -> fraud.gold.transaction_features_history
from pyspark.sql import DataFrame

with open("../sql/transaction_features.sql") as features_sql_file:
    transaction_features_query: str = features_sql_file.read()

spark.table("fraud.silver.transactions_history").createOrReplaceTempView("silver_transactions")

feature_history: DataFrame = spark.sql(transaction_features_query)

feature_history.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("fraud.gold.transaction_features_history")