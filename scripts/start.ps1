$ErrorActionPreference = "Stop"

Write-Host "Starting Real-Time Crypto Anomaly project..." -ForegroundColor Cyan
docker compose up -d --build kafka namenode datanode
docker compose up kafka-init

Write-Host "Starting producer, Spark processing and dashboard..." -ForegroundColor Cyan
docker compose up -d --build producer spark dashboard

Write-Host ""
Write-Host "Project started." -ForegroundColor Green
Write-Host "Dashboard: http://localhost:8501"
Write-Host "HDFS UI:   http://localhost:9870"
Write-Host ""
Write-Host "Run: docker compose logs -f producer spark"
