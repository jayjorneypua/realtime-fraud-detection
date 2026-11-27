# starts the local live path: kafka and redis (waits until both are healthy), creates the topics,
# and downloads the replay file from Databricks if it is not there (stop.ps1 deletes it).
# topics have 3 partitions and are keyed by card, so each card's events stay in order.
[string[]]$Topics = @("transactions", "transaction-labels", "scores", "fraud-alerts")
[string]$Profile = "dbc-609647e1-c073"
[string]$ReplayFile = "data\fraudTest.csv"

docker compose up -d --wait
if ($LASTEXITCODE -ne 0) { throw "docker compose did not start, check docker desktop" }

foreach ($Topic in $Topics) {
  docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $Topic --partitions 3 --replication-factor 1
}

if (-not (Test-Path $ReplayFile)) {
  New-Item -ItemType Directory -Path data -Force | Out-Null
  databricks fs cp dbfs:/Volumes/fraud/landing/kaggle/fraudTest.csv $ReplayFile --profile $Profile
}

docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
Write-Host "live path ready: kafka localhost:9092, redis localhost:6380" -ForegroundColor Green