# Final evaluation: live scoring on fraudTest

Full replay of fraudTest (555,719 transactions, Jun-Dec 2020) through the live path: Kafka -> scoring service
(@champion v3, XGBoost, threshold 0.7937) -> lakehouse sink -> fraud.gold.scores_live, joined to the true labels
from the separate labels topic. Query: sql/final_evaluation.sql. Run: .\run_live.ps1 -Limit 555719 -NoPacing.

| Metric | Validation (Mar-Jun 2020) | Live, full fraudTest (Jun-Dec 2020) |
|---|---|---|
| Transactions scored | - | 555,719 (all labeled, 1 model version) |
| Fraud transactions | - | 2,145 |
| Alerts | - | 2,316 |
| True positives | - | 2,016 |
| False positives | - | 300 |
| False negatives | - | 129 |
| Precision | 90.03% | 87.05% |
| Recall | 95.52% | 93.99% |
| PR-AUC | 0.9796 | 0.9673 |

Latency is measured on paced runs (-Speed 1000, service already running): p50 about 350 ms, p95 about 600 ms.
The full-set run publishes without pacing, so its latency includes queue time and is not a latency result.

Full run time: about 38 minutes (scoring about 20 min at about 400 transactions per second, sink batches of 25,000
uploaded in parallel, Databricks live job about 2 min).