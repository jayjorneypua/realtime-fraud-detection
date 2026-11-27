# stops the local live path and removes everything it left on this pc: the kafka and redis containers
# with their volumes (messages and card history), and the data folder (replay file and downloaded model).
docker compose down --volumes --remove-orphans

if (Test-Path data) { Remove-Item -Recurse -Force data }

Write-Host "live path stopped, local data removed" -ForegroundColor Green