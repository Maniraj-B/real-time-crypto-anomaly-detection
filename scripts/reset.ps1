$ErrorActionPreference = "Stop"
Write-Host "Stopping project and deleting Kafka/HDFS state..." -ForegroundColor Yellow
docker compose down -v --remove-orphans
Write-Host "Reset complete." -ForegroundColor Green
