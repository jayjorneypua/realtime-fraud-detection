# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS fraud.gold")

with open("../sql/transaction_features.sql") as features_sql_file:
    transaction_features_query = features_sql_file.read()

spark.table("fraud.silver.transactions_history").createOrReplaceTempView("silver_transactions")

feature_history = spark.sql(transaction_features_query)

feature_history.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("fraud.gold.transaction_features_history")

# COMMAND ----------

display(spark.sql("""
    SELECT
        is_fraud
        , COUNT(*) AS transactions
        , ROUND(AVG(amount), 2) AS average_amount
        , ROUND(AVG(amount_to_card_average_ratio), 2) AS average_amount_to_card_average_ratio
        , ROUND(AVG(card_transaction_count_24h), 2) AS average_card_transactions_24h
        , ROUND(AVG(card_amount_sum_24h), 2) AS average_card_spend_24h
        , ROUND(AVG(is_night), 3) AS night_share
        , ROUND(AVG(is_new_merchant_for_card), 3) AS new_merchant_share
        , ROUND(AVG(customer_to_merchant_km), 1) AS average_customer_to_merchant_km
    FROM
        fraud.gold.transaction_features_history
    GROUP BY
        is_fraud
    ORDER BY
        is_fraud
"""))