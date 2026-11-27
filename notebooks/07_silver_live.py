# Databricks notebook source
# Silver live: live transactions joined with their fraud labels (sql/live_bronze_transactions.sql), then the
# exact same sql/silver_transactions.sql as history (same salt, same dedup). Live scores typed and deduplicated
# by sql/live_scores.sql.
# fraud.bronze.transactions_live + transaction_labels_live -> fraud.silver.transactions_live
# fraud.bronze.scores_live -> fraud.gold.scores_live
from pyspark.sql import DataFrame


def read_sql_file(sql_path: str) -> str:
    with open(sql_path) as sql_file:
        return sql_file.read()


card_hash_salt: str = dbutils.secrets.get("fraud", "card_hash_salt")

live_bronze_transactions: DataFrame = spark.sql(read_sql_file("../sql/live_bronze_transactions.sql"))
live_bronze_transactions.createOrReplaceTempView("bronze_transactions")

silver_live: DataFrame = spark.sql(read_sql_file("../sql/silver_transactions.sql"), args={"card_hash_salt": card_hash_salt})
silver_live.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("fraud.silver.transactions_live")

scores_live: DataFrame = spark.sql(read_sql_file("../sql/live_scores.sql"))
scores_live.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("fraud.gold.scores_live")