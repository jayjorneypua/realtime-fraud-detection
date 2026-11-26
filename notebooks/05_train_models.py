# Databricks notebook source
# MAGIC %pip install xgboost

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, precision_score, recall_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from xgboost import XGBClassifier

VALIDATION_START = "2020-03-01"
EXPERIMENT_PATH = "/Users/jayjorneypua@gmail.com/fraud-detection-training"

CATEGORICAL_FEATURES = ["merchant_category"]
NUMERIC_FEATURES = [
    "amount",
    "hour_of_day",
    "day_of_week",
    "is_night",
    "customer_age_years",
    "customer_to_merchant_km",
    "card_transaction_count_1h",
    "card_amount_sum_1h",
    "card_transaction_count_24h",
    "card_amount_sum_24h",
    "amount_to_card_average_ratio",
    "seconds_since_previous_card_transaction",
    "km_from_previous_merchant",
    "is_new_category_for_card",
    "is_new_merchant_for_card",
]
MODEL_FEATURES = CATEGORICAL_FEATURES + NUMERIC_FEATURES

# COMMAND ----------

feature_columns_sql = "\n        , ".join(MODEL_FEATURES)

training_data = spark.sql(f"""
    SELECT
        {feature_columns_sql}
        , is_fraud
        , CASE WHEN transaction_time >= '{VALIDATION_START}' THEN 'validation' ELSE 'train' END AS dataset_split
    FROM
        fraud.gold.transaction_features_history
""").toPandas()

train_rows = training_data[training_data["dataset_split"] == "train"]
validation_rows = training_data[training_data["dataset_split"] == "validation"]

train_features = train_rows[MODEL_FEATURES]
train_labels = train_rows["is_fraud"]
validation_features = validation_rows[MODEL_FEATURES]
validation_labels = validation_rows["is_fraud"]

print(f"train rows: {len(train_rows)}, fraud: {train_labels.sum()}")
print(f"validation rows: {len(validation_rows)}, fraud: {validation_labels.sum()}")

# COMMAND ----------

def build_preprocessor() -> ColumnTransformer:
    return ColumnTransformer([
        ("merchant_category_encoding", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
        ("numeric_scaling", StandardScaler(), NUMERIC_FEATURES),
    ])


def evaluate_model(model: Pipeline, features: pd.DataFrame, labels: pd.Series) -> dict:
    fraud_probabilities = model.predict_proba(features)[:, 1]
    fraud_predictions = (fraud_probabilities >= 0.5).astype(int)
    return {
        "pr_auc": average_precision_score(labels, fraud_probabilities),
        "roc_auc": roc_auc_score(labels, fraud_probabilities),
        "precision_at_0_5": precision_score(labels, fraud_predictions),
        "recall_at_0_5": recall_score(labels, fraud_predictions),
    }


def train_and_log(run_name: str, classifier, classifier_parameters: dict) -> dict:
    with mlflow.start_run(run_name=run_name):
        model = Pipeline([
            ("preprocessing", build_preprocessor()),
            ("classifier", classifier),
        ])
        model.fit(train_features, train_labels)
        validation_metrics = evaluate_model(model, validation_features, validation_labels)

        mlflow.log_params(classifier_parameters)
        mlflow.log_param("validation_start", VALIDATION_START)
        mlflow.log_metrics(validation_metrics)
        mlflow.sklearn.log_model(model, name="model", input_example=validation_features.head(5))

        return {"model": run_name, **validation_metrics}

# COMMAND ----------

mlflow.set_experiment(EXPERIMENT_PATH)

fraud_to_normal_ratio = (train_labels == 0).sum() / (train_labels == 1).sum()

logistic_regression_parameters = {"class_weight": "balanced", "max_iter": 1000}
xgboost_parameters = {
    "n_estimators": 300,
    "max_depth": 6,
    "learning_rate": 0.1,
    "scale_pos_weight": float(fraud_to_normal_ratio),
    "tree_method": "hist",
    "eval_metric": "aucpr",
}

model_results = [
    train_and_log("logistic_regression_baseline", LogisticRegression(**logistic_regression_parameters), logistic_regression_parameters),
    train_and_log("xgboost", XGBClassifier(**xgboost_parameters), xgboost_parameters),
]

display(pd.DataFrame(model_results))