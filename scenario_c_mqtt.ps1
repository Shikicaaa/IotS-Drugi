Write-Host ""
Write-Host "=== MQTT Scenario C ==="
Write-Host ""

Write-Host "Phase 1: 50 msg/s for 30s"

docker run --rm `
  --network iots-2_default `
  emqx/emqtt-bench pub `
  -h mosquitto `
  -t sensor/data `
  -c 50 `
  -I 1000 `
  -L 1500

Write-Host ""
Write-Host "Phase 2: BURST 5000 msg/s"

docker run --rm `
  --network iots-2_default `
  emqx/emqtt-bench pub `
  -h mosquitto `
  -t sensor/data `
  -c 5000 `
  -I 10 `
  -L 50000

Write-Host ""
Write-Host "Phase 3: Recovery 50 msg/s"

docker run --rm `
  --network iots-2_default `
  emqx/emqtt-bench pub `
  -h mosquitto `
  -t sensor/data `
  -c 50 `
  -I 1000 `
  -L 1500