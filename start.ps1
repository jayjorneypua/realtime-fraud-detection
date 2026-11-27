# starts the local live path: kafka and redis (waits until both are healthy), then creates the topics.
# topics have 3 partitions and are keyed by card, so each card's events stay in order.
[string[]]$Topics = @("transactions", "transaction-labels", "scores", "fraud-alerts")

docker compose up -d --wait
if ($LASTEXITCODE -ne 0) { throw "docker compose did not start, check docker desktop" }

foreach ($Topic in $Topics) {
  docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $Topic --partitions 3 --replication-factor 1
}

docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
Write-Host "live path ready: kafka localhost:9092, redis localhost:6380" -ForegroundColor Green