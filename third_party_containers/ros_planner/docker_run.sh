#!/bin/bash
# docker_run.sh -- Run the ros-planner container.
#
# Reads infrastructure (NATS, MQTT host:port) from the KB SQLite.
# Only SQLITE_DB env var is required. Everything else comes from KB.
#
# Prerequisites:
#   - NATS, MQTT, kv-bridge containers running
#   - SQLite DBs built: cd kb_dsl/scripts && luajit master_build.lua
#
# Usage:
#   ./docker_run.sh                              # surface_ops (default)
#   ./docker_run.sh warehouse_ops                # different domain
#   MQTT_HOST=10.0.1.5 ./docker_run.sh           # env var override

set -e

DOMAIN="${1:-surface_ops}"
SQLITE_DATA="${SQLITE_DATA:-/home/gedgar/Sqlite_Data}"

if [ ! -f "$SQLITE_DATA/${DOMAIN}.db" ]; then
    echo "ERROR: SQLite DB not found: $SQLITE_DATA/${DOMAIN}.db"
    echo "Run: cd kb_dsl/scripts && luajit master_build.lua"
    exit 1
fi

CONTAINER_NAME="${DOMAIN}-planner"

# Stop existing container if running
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

echo "=== Starting $CONTAINER_NAME ==="
echo "  DB:   $SQLITE_DATA/${DOMAIN}.db"
echo ""

# Build env var overrides (only pass if explicitly set)
ENV_ARGS="-e SQLITE_DB=/data/${DOMAIN}.db"
[ -n "$MQTT_HOST" ]    && ENV_ARGS="$ENV_ARGS -e MQTT_HOST=$MQTT_HOST"
[ -n "$MQTT_PORT" ]    && ENV_ARGS="$ENV_ARGS -e MQTT_PORT=$MQTT_PORT"
[ -n "$NATS_SERVER" ]  && ENV_ARGS="$ENV_ARGS -e NATS_SERVER=$NATS_SERVER"
[ -n "$VMRT_KB_SITE" ] && ENV_ARGS="$ENV_ARGS -e VMRT_KB_SITE=$VMRT_KB_SITE"

docker run \
    --name "$CONTAINER_NAME" \
    --restart unless-stopped \
    --network host \
    -v "$SQLITE_DATA:/data:ro" \
    $ENV_ARGS \
    nanodatacenter/ros-planner:latest
