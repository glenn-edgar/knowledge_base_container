#!/bin/bash
# docker_run.sh -- Run the ros-planner container.
#
# Uses planner-net Docker network for container-to-container communication.
# MQTT and NATS are reached by container name (Docker DNS).
#
# Prerequisites:
#   - docker network create planner-net
#   - NATS, MQTT containers running and connected to planner-net
#   - SQLite DBs built: cd kb_dsl/scripts && luajit master_build.lua
#
# Usage:
#   ./docker_run.sh                              # surface_ops (default)
#   ./docker_run.sh warehouse_ops                # different domain
#   MQTT_HOST=10.0.1.5 ./docker_run.sh           # env var override

set -e

DOMAIN="${1:-surface_ops}"
SQLITE_DATA="${SQLITE_DATA:-/home/gedgar/Sqlite_Data}"
NETWORK="${DOCKER_NETWORK:-planner-net}"

if [ ! -f "$SQLITE_DATA/${DOMAIN}.db" ]; then
    echo "ERROR: SQLite DB not found: $SQLITE_DATA/${DOMAIN}.db"
    echo "Run: cd kb_dsl/scripts && luajit master_build.lua"
    exit 1
fi

# Ensure network exists
docker network create "$NETWORK" 2>/dev/null || true

CONTAINER_NAME="${DOMAIN}-planner"

# Stop existing container if running
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

echo "=== Starting $CONTAINER_NAME ==="
echo "  DB:      $SQLITE_DATA/${DOMAIN}.db"
echo "  Network: $NETWORK"
echo ""

# Container names for MQTT/NATS on the shared network (Docker DNS)
MQTT_HOST="${MQTT_HOST:-mosquitto-ram-ws_main}"
NATS_SERVER="${NATS_SERVER:-nats://nats-js-ram:4222}"

ENV_ARGS="-e SQLITE_DB=/data/${DOMAIN}.db"
ENV_ARGS="$ENV_ARGS -e MQTT_HOST=$MQTT_HOST"
ENV_ARGS="$ENV_ARGS -e NATS_SERVER=$NATS_SERVER"
[ -n "$MQTT_PORT" ]    && ENV_ARGS="$ENV_ARGS -e MQTT_PORT=$MQTT_PORT"
[ -n "$VMRT_KB_SITE" ] && ENV_ARGS="$ENV_ARGS -e VMRT_KB_SITE=$VMRT_KB_SITE"

docker run -d \
    --name "$CONTAINER_NAME" \
    --restart unless-stopped \
    --network "$NETWORK" \
    -v "$SQLITE_DATA:/data" \
    $ENV_ARGS \
    nanodatacenter/ros-planner:latest

echo "Container $CONTAINER_NAME started (detached)."
echo "  Logs: docker logs -f $CONTAINER_NAME"
