from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_transactions(bronze_transactions: DataFrame, card_hash_salt: str) -> DataFrame:
    return (
        bronze_transactions
        .select(
            F.col("trans_num").alias("transaction_id"),
            F.to_timestamp("trans_date_trans_time", "yyyy-MM-dd HH:mm:ss").alias("transaction_time"),
            F.sha2(F.concat(F.lit(card_hash_salt), F.col("cc_num")), 256).alias("card_key"),
            F.regexp_replace("merchant", "^fraud_", "").alias("merchant_name"),
            F.col("category").alias("merchant_category"),
            F.col("amt").cast("decimal(12,2)").alias("amount"),
            F.col("gender").alias("customer_gender"),
            F.col("city").alias("customer_city"),
            F.col("state").alias("customer_state"),
            F.substring(F.lpad("zip", 5, "0"), 1, 3).alias("customer_zip3"),
            F.col("lat").cast("double").alias("customer_latitude"),
            F.col("long").cast("double").alias("customer_longitude"),
            F.col("city_pop").cast("int").alias("customer_city_population"),
            F.col("job").alias("customer_job"),
            F.to_date("dob").alias("customer_birth_date"),
            F.col("merch_lat").cast("double").alias("merchant_latitude"),
            F.col("merch_long").cast("double").alias("merchant_longitude"),
            F.col("is_fraud").cast("int").alias("is_fraud"),
            F.col("_source_file").alias("source_file"),
            F.col("_ingested_at").alias("ingested_at"),
        )
        .dropDuplicates(["transaction_id"])
    )