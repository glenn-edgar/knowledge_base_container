#!/bin/bash
# docker_run.sh -- Run kv-bridge on planner-net.
#
# Bridges MQTT topics to NATS KV (async writes).
# Uses Docker DNS to reach MQTT and NATS by container name.
#
# First run:  ./docker_run.sh
# Restart:    docker start kv-bridge
# Stop:       docker stop kv-bridge
# Logs:       docker logs -f kv-bridge

NETWORK="${DOCKER_NETWORK:-planner-net}"

docker network create "$NETWORK" 2>/dev/null || true
docker rm -f kv-bridge 2>/dev/null

docker run --name kv-bridge -d \
  --restart unless-stopped \
  --network "$NETWORK" \
  -e MQTT_HOST=mosquitto-ram-ws_main \
  -e MQTT_PORT=1883 \
  -e MQTT_TOPIC=kv_bridge/write \
  -e NATS_URL=nats://nats-js-ram:4222 \
  -e LOG_LEVEL=info \
  nanodatacenter/kv-bridge:latest

echo "kv-bridge started on $NETWORK"
echo "  Logs: docker logs -f kv-bridge"
