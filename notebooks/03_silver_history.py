# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath("../src"))

from fraud_pipeline.silver import clean_transactions

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS fraud.silver")

card_hash_salt = dbutils.secrets.get("fraud", "card_hash_salt")

bronze_history = spark.table("fraud.bronze.transactions_history")
silver_history = clean_transactions(bronze_history, card_hash_salt)

silver_history.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("fraud.silver.transactions_history")

# COMMAND ----------

display(spark.sql("""
    SELECT COUNT(*) AS total_rows
         , COUNT(DISTINCT transaction_id) AS distinct_transactions
         , COUNT(DISTINCT card_key) AS distinct_cards
         , SUM(is_fraud) AS fraud_rows
         , SUM(CASE WHEN transaction_time IS NULL THEN 1 ELSE 0 END) AS null_transaction_times
         , SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amounts
         , MIN(transaction_time) AS first_transaction
         , MAX(transaction_time) AS last_transaction
    FROM fraud.silver.transactions_history
"""))

# COMMAND ----------

display(spark.table("fraud.silver.transactions_history").limit(10))