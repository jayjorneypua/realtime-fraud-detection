# Databricks notebook source
# Data quality: runs every check in sql/data_quality_checks.sql, appends the results to the log with the job
# run id, then fails the run if any error-severity check failed. Warnings and info are logged only.
# fraud.bronze.*_live, fraud.silver.transactions_live, fraud.gold.scores_live -> fraud.gold.data_quality_log
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F

dbutils.widgets.text("run_id", "manual")
run_id: str = dbutils.widgets.get("run_id")

with open("../sql/data_quality_checks.sql") as checks_sql_file:
    data_quality_checks_query: str = checks_sql_file.read()

check_results: DataFrame = (
    spark.sql(data_quality_checks_query)
        .withColumn("passed", F.col("failed_rows") == 0)
        .withColumn("run_id", F.lit(run_id))
        .withColumn("checked_at", F.current_timestamp())
)

# collect once, so the log and the pass/fail decision come from the same results
check_rows: list[Row] = check_results.collect()
spark.createDataFrame(check_rows, check_results.schema).write.mode("append").saveAsTable("fraud.gold.data_quality_log")

for check_row in check_rows:
    print(f"{check_row['severity']:<8} {'passed' if check_row['passed'] else 'FAILED':<7} {check_row['check_name']}: {check_row['failed_rows']} of {check_row['checked_rows']}")

failed_error_checks: list[str] = [check_row["check_name"] for check_row in check_rows if check_row["severity"] == "error" and not check_row["passed"]]
if failed_error_checks:
    raise RuntimeError(f"data quality errors: {', '.join(failed_error_checks)}")