# Databricks notebook source
# Silver: typed, renamed, deduplicated and de-identified transactions.
# The transform lives in sql/silver_transactions.sql, so live data landed by the sink goes through the same SQL.
# Card numbers become a salted SHA-256 card_key (salt in the fraud secret scope),
# names and street address are dropped, zip is cut to 3 digits.
# fraud.bronze.transactions_history -> fraud.silver.transactions_history
from pyspark.sql import DataFrame

card_hash_salt: str = dbutils.secrets.get("fraud", "card_hash_salt")

with open("../sql/silver_transactions.sql") as silver_sql_file:
    silver_transactions_query: str = silver_sql_file.read()

spark.table("fraud.bronze.transactions_history").createOrReplaceTempView("bronze_transactions")

silver_history: DataFrame = spark.sql(silver_transactions_query, args={"card_hash_salt": card_hash_salt})

silver_history.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("fraud.silver.transactions_history")