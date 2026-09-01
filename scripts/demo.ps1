Write-Host "=== CONTAINER STATUS ===" -ForegroundColor Cyan
docker compose ps

Write-Host "`n=== KAFKA TOPICS ===" -ForegroundColor Cyan
docker exec crypto-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list

Write-Host "`n=== SAMPLE LIVE RAW EVENTS (Ctrl+C after a few) ===" -ForegroundColor Cyan
docker exec -it crypto-kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:29092 `
  --topic crypto.raw.trades `
  --from-beginning `
  --max-messages 5
