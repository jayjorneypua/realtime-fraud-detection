# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS fraud.silver")

card_hash_salt = dbutils.secrets.get("fraud", "card_hash_salt")

with open("../sql/silver_transactions.sql") as silver_sql_file:
    silver_transactions_query = silver_sql_file.read()

spark.table("fraud.bronze.transactions_history").createOrReplaceTempView("bronze_transactions")

silver_history = spark.sql(silver_transactions_query, args={"card_hash_salt": card_hash_salt})

silver_history.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("fraud.silver.transactions_history")

# COMMAND ----------

display(spark.sql("""
    SELECT
        COUNT(*) AS total_rows
        , COUNT(DISTINCT transaction_id) AS distinct_transactions
        , COUNT(DISTINCT card_key) AS distinct_cards
        , SUM(is_fraud) AS fraud_rows
        , SUM(CASE WHEN transaction_time IS NULL THEN 1 ELSE 0 END) AS null_transaction_times
        , SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amounts
        , MIN(transaction_time) AS first_transaction
        , MAX(transaction_time) AS last_transaction
    FROM
        fraud.silver.transactions_history
"""))