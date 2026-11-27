# Databricks notebook source
# Train: logistic regression baseline and XGBoost on per-card features, split by time
# (train before 2020-03-01, validate from then on), both tracked in MLflow.
# XGBoost handles merchant_category natively and is logged in XGBoost's JSON format, which loads on any OS
# and later XGBoost versions (a pickle saved on Linux does not load on Windows). The baseline is logged for
# comparison only. XGBoost is registered in Unity Catalog with its alert threshold (highest recall at 90%
# precision) and category list as version tags, and becomes @champion only if it is at least as good as the
# current champion.
# fraud.gold.transaction_features_history -> MLflow experiment + fraud.ml.fraud_detector@champion

# COMMAND ----------

# MAGIC %pip install xgboost==3.4.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json

import mlflow
import mlflow.xgboost
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

# fixed category list: scoring encodes merchant_category with exactly this list, stored on the model version
merchant_categories: list[str] = sorted(train_features["merchant_category"].unique().tolist())

# COMMAND ----------

def to_xgboost_frame(features: pd.DataFrame) -> pd.DataFrame:
    xgboost_frame: pd.DataFrame = features.copy()
    xgboost_frame["merchant_category"] = pd.Categorical(xgboost_frame["merchant_category"], categories=merchant_categories)
    return xgboost_frame


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


def evaluate(fraud_probabilities: np.ndarray) -> dict:
    alert_threshold, precision_at_threshold, recall_at_threshold = choose_alert_threshold(validation_labels, fraud_probabilities)
    return {
        "pr_auc": average_precision_score(validation_labels, fraud_probabilities),
        "roc_auc": roc_auc_score(validation_labels, fraud_probabilities),
        "alert_threshold": alert_threshold,
        "precision_at_threshold": precision_at_threshold,
        "recall_at_threshold": recall_at_threshold,
    }


def train_logistic_regression(parameters: dict) -> dict:
    # baseline for comparison only: a scikit-learn pickle, never registered
    with mlflow.start_run(run_name="logistic_regression_baseline"):
        model: Pipeline = Pipeline([
            ("preprocessing", ColumnTransformer([
                ("merchant_category_encoding", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
                ("numeric_scaling", StandardScaler(), NUMERIC_FEATURES),
            ])),
            ("classifier", LogisticRegression(**parameters)),
        ])
        model.fit(train_features, train_labels)
        validation_metrics: dict = evaluate(model.predict_proba(validation_features)[:, 1])

        mlflow.log_params(parameters)
        mlflow.log_params({"validation_start": VALIDATION_START, "target_precision": TARGET_PRECISION})
        mlflow.log_metrics(validation_metrics)
        return {"model": "logistic_regression_baseline", "model_uri": None, **validation_metrics}


def train_xgboost(parameters: dict) -> dict:
    with mlflow.start_run(run_name="xgboost"):
        model: XGBClassifier = XGBClassifier(**parameters)
        model.fit(to_xgboost_frame(train_features), train_labels)
        fraud_probabilities: np.ndarray = model.predict_proba(to_xgboost_frame(validation_features))[:, 1]
        validation_metrics: dict = evaluate(fraud_probabilities)

        mlflow.log_params(parameters)
        mlflow.log_params({"validation_start": VALIDATION_START, "target_precision": TARGET_PRECISION})
        mlflow.log_metrics(validation_metrics)

        # the signature lists the input columns in order; merchant_category arrives as a string
        model_info = mlflow.xgboost.log_model(
            model,
            name="model",
            model_format="json",
            signature=infer_signature(validation_features.head(100), fraud_probabilities[:100]),
        )
        return {"model": "xgboost", "model_uri": model_info.model_uri, **validation_metrics}

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
    "enable_categorical": True,
    "eval_metric": "aucpr",
}

model_results: list[dict] = [
    train_logistic_regression(logistic_regression_parameters),
    train_xgboost(xgboost_parameters),
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


xgboost_result: dict = next(result for result in model_results if result["model"] == "xgboost")
registered_version = mlflow.register_model(xgboost_result["model_uri"], REGISTERED_MODEL_NAME)

version_tags: dict = {
    "model_type": "xgboost",
    "model_format": "xgboost_json",
    "validation_pr_auc": xgboost_result["pr_auc"],
    "alert_threshold": xgboost_result["alert_threshold"],
    "precision_at_threshold": xgboost_result["precision_at_threshold"],
    "recall_at_threshold": xgboost_result["recall_at_threshold"],
    "merchant_categories": json.dumps(merchant_categories),
    "validation_start": VALIDATION_START,
}
for tag_name, tag_value in version_tags.items():
    mlflow_client.set_model_version_tag(REGISTERED_MODEL_NAME, registered_version.version, tag_name, str(tag_value))

champion_pr_auc: float | None = current_champion_pr_auc()
promoted: bool = champion_pr_auc is None or xgboost_result["pr_auc"] >= champion_pr_auc
if promoted:
    mlflow_client.set_registered_model_alias(REGISTERED_MODEL_NAME, CHAMPION_ALIAS, registered_version.version)

baseline_pr_auc: float = next(result["pr_auc"] for result in model_results if result["model"] == "logistic_regression_baseline")
training_summary: dict = {
    "registered_version": registered_version.version,
    "pr_auc": round(xgboost_result["pr_auc"], 4),
    "baseline_pr_auc": round(baseline_pr_auc, 4),
    "alert_threshold": round(xgboost_result["alert_threshold"], 4),
    "precision_at_threshold": round(xgboost_result["precision_at_threshold"], 4),
    "recall_at_threshold": round(xgboost_result["recall_at_threshold"], 4),
    "previous_champion_pr_auc": champion_pr_auc,
    "promoted_to_champion": promoted,
}
dbutils.notebook.exit(json.dumps(training_summary))