#!/bin/bash
# First run:  ./docker_run.sh
# Restart:    docker start kv-bridge
# Stop:       docker stop kv-bridge
# Logs:       docker logs -f kv-bridge

docker rm -f kv-bridge 2>/dev/null

docker run --name kv-bridge -d \
  --restart unless-stopped \
  --network host \
  -e MQTT_HOST=localhost \
  -e MQTT_PORT=1883 \
  -e MQTT_TOPIC=kv_bridge/write \
  -e NATS_URL=nats://127.0.0.1:4222 \
  -e LOG_LEVEL=info \
  nanodatacenter/kv-bridge:latest
