# Databricks notebook source
# Train: logistic regression baseline and XGBoost on per-card features, split by time
# (train before 2020-03-01, validate from then on), both tracked in MLflow.
# The best model by PR-AUC is registered in Unity Catalog with its alert threshold (highest recall at
# 90% precision) as a version tag. It becomes @champion only if it is at least as good as the current champion.
# fraud.gold.transaction_features_history -> MLflow experiment + fraud.ml.fraud_detector@champion

# COMMAND ----------

# MAGIC %pip install xgboost

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json

import mlflow
import numpy as np
import pandas as pd
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from xgboost import XGBClassifier

VALIDATION_START: str = "2020-03-01"
EXPERIMENT_PATH: str = "/Users/jayjorneypua@gmail.com/fraud-detection-training"
REGISTERED_MODEL_NAME: str = "fraud.ml.fraud_detector"
CHAMPION_ALIAS: str = "champion"
TARGET_PRECISION: float = 0.90

CATEGORICAL_FEATURES: list[str] = ["merchant_category"]
NUMERIC_FEATURES: list[str] = [
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
MODEL_FEATURES: list[str] = CATEGORICAL_FEATURES + NUMERIC_FEATURES

# COMMAND ----------

feature_columns_sql: str = "\n        , ".join(MODEL_FEATURES)

training_data: pd.DataFrame = spark.sql(f"""
    SELECT
        {feature_columns_sql}
        , is_fraud
        , CASE WHEN transaction_time >= '{VALIDATION_START}' THEN 'validation' ELSE 'train' END AS dataset_split
    FROM
        fraud.gold.transaction_features_history
""").toPandas()

# live scoring sends every numeric feature as a float, so the model is trained and signed on floats
training_data[NUMERIC_FEATURES] = training_data[NUMERIC_FEATURES].astype("float64")

train_rows: pd.DataFrame = training_data[training_data["dataset_split"] == "train"]
validation_rows: pd.DataFrame = training_data[training_data["dataset_split"] == "validation"]

train_features: pd.DataFrame = train_rows[MODEL_FEATURES]
train_labels: pd.Series = train_rows["is_fraud"]
validation_features: pd.DataFrame = validation_rows[MODEL_FEATURES]
validation_labels: pd.Series = validation_rows["is_fraud"]

# COMMAND ----------

def build_preprocessor() -> ColumnTransformer:
    return ColumnTransformer([
        ("merchant_category_encoding", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
        ("numeric_scaling", StandardScaler(), NUMERIC_FEATURES),
    ])


def choose_alert_threshold(labels: pd.Series, fraud_probabilities: np.ndarray) -> tuple[float, float, float]:
    # highest recall among thresholds that keep precision at or above the target
    precisions, recalls, thresholds = precision_recall_curve(labels, fraud_probabilities)
    precisions, recalls = precisions[:-1], recalls[:-1]  # the curve has one more point than thresholds
    meets_target: np.ndarray = np.where(precisions >= TARGET_PRECISION)[0]
    if len(meets_target) == 0:
        chosen_index: int = int(np.argmax(precisions))
    else:
        chosen_index = int(meets_target[np.argmax(recalls[meets_target])])
    return float(thresholds[chosen_index]), float(precisions[chosen_index]), float(recalls[chosen_index])


def train_and_log(run_name: str, classifier, classifier_parameters: dict) -> dict:
    with mlflow.start_run(run_name=run_name):
        model: Pipeline = Pipeline([
            ("preprocessing", build_preprocessor()),
            ("classifier", classifier),
        ])
        model.fit(train_features, train_labels)

        fraud_probabilities: np.ndarray = model.predict_proba(validation_features)[:, 1]
        alert_threshold, precision_at_threshold, recall_at_threshold = choose_alert_threshold(validation_labels, fraud_probabilities)
        validation_metrics: dict = {
            "pr_auc": average_precision_score(validation_labels, fraud_probabilities),
            "roc_auc": roc_auc_score(validation_labels, fraud_probabilities),
            "alert_threshold": alert_threshold,
            "precision_at_threshold": precision_at_threshold,
            "recall_at_threshold": recall_at_threshold,
        }

        mlflow.log_params(classifier_parameters)
        mlflow.log_params({"validation_start": VALIDATION_START, "target_precision": TARGET_PRECISION})
        mlflow.log_metrics(validation_metrics)

        model_info = mlflow.sklearn.log_model(
            model,
            name="model",
            signature=infer_signature(validation_features, model.predict_proba(validation_features.head(100))),
            input_example=validation_features.head(5),
            pyfunc_predict_fn="predict_proba",
        )

        return {"model": run_name, "model_uri": model_info.model_uri, **validation_metrics}

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT_PATH)

normal_to_fraud_ratio: float = float((train_labels == 0).sum() / (train_labels == 1).sum())

logistic_regression_parameters: dict = {"class_weight": "balanced", "max_iter": 1000}
xgboost_parameters: dict = {
    "n_estimators": 300,
    "max_depth": 6,
    "learning_rate": 0.1,
    "scale_pos_weight": normal_to_fraud_ratio,
    "tree_method": "hist",
    "eval_metric": "aucpr",
}

model_results: list[dict] = [
    train_and_log("logistic_regression_baseline", LogisticRegression(**logistic_regression_parameters), logistic_regression_parameters),
    train_and_log("xgboost", XGBClassifier(**xgboost_parameters), xgboost_parameters),
]

display(pd.DataFrame(model_results).drop(columns=["model_uri"]))

# COMMAND ----------

mlflow_client: MlflowClient = MlflowClient()


def current_champion_pr_auc() -> float | None:
    try:
        champion_version = mlflow_client.get_model_version_by_alias(REGISTERED_MODEL_NAME, CHAMPION_ALIAS)
    except MlflowException:
        return None  # first registration: no model or no champion yet
    return float(champion_version.tags["validation_pr_auc"])


best_model: dict = max(model_results, key=lambda result: result["pr_auc"])
registered_version = mlflow.register_model(best_model["model_uri"], REGISTERED_MODEL_NAME)

version_tags: dict = {
    "model_type": best_model["model"],
    "validation_pr_auc": best_model["pr_auc"],
    "alert_threshold": best_model["alert_threshold"],
    "precision_at_threshold": best_model["precision_at_threshold"],
    "recall_at_threshold": best_model["recall_at_threshold"],
    "validation_start": VALIDATION_START,
}
for tag_name, tag_value in version_tags.items():
    mlflow_client.set_model_version_tag(REGISTERED_MODEL_NAME, registered_version.version, tag_name, str(tag_value))

champion_pr_auc: float | None = current_champion_pr_auc()
promoted: bool = champion_pr_auc is None or best_model["pr_auc"] >= champion_pr_auc
if promoted:
    mlflow_client.set_registered_model_alias(REGISTERED_MODEL_NAME, CHAMPION_ALIAS, registered_version.version)

training_summary: dict = {
    "registered_version": registered_version.version,
    "model_type": best_model["model"],
    "pr_auc": round(best_model["pr_auc"], 4),
    "alert_threshold": round(best_model["alert_threshold"], 4),
    "precision_at_threshold": round(best_model["precision_at_threshold"], 4),
    "recall_at_threshold": round(best_model["recall_at_threshold"], 4),
    "previous_champion_pr_auc": champion_pr_auc,
    "promoted_to_champion": promoted,
}
dbutils.notebook.exit(json.dumps(training_summary))