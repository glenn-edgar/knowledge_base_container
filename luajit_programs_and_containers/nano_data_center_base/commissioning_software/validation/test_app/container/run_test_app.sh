#!/usr/bin/env bash
# run_test_app.sh -- build (if needed) + run the test-app container locally.
#
# Networking: bridge + -p 8080:8080 -p 8081:8081 by default. This works on
# Docker Desktop / WSL2 where --network=host does NOT expose container ports
# to WSL's localhost (host networking there binds inside the Docker VM's
# netns, not WSL's). PG_HOST defaults to host.docker.internal so the
# container can reach the pg-vector container publishing 5432 on the host.
# Set DOCKER_NETWORK=host to switch to Linux-native host networking.
#
# Usage:
#   ./run_test_app.sh              run (build if image missing)
#   ./run_test_app.sh --rebuild    force rebuild before run
#   ./run_test_app.sh --stop       docker rm -f the running container

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CONTAINER_NAME="${CONTAINER_NAME:-test_app}"
APP_SITE="${APP_SITE:-dev}"
APP_CPU_ID="${APP_CPU_ID:-cpu_01}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-knowledge_base}"
PG_USER="${PG_USER:-gedgar}"
NATS_URL="${NATS_URL:-}"
MQTT_HOST="${MQTT_HOST:-}"
DOCKER_NETWORK="${DOCKER_NETWORK:-bridge}"
IMAGE_TAG="${IMAGE_TAG:-nanodatacenter/test-app:latest}"
WEB_PORT_EXCEPTION="${WEB_PORT_EXCEPTION:-8080}"
WEB_PORT_LOG="${WEB_PORT_LOG:-8081}"

# Default PG_HOST depends on networking mode. For host networking, pg on
# the same machine at 127.0.0.1 is directly reachable. For bridge, we need
# host.docker.internal (created via --add-host below).
if [[ "$DOCKER_NETWORK" == "host" ]]; then
    PG_HOST="${PG_HOST:-127.0.0.1}"
else
    PG_HOST="${PG_HOST:-host.docker.internal}"
fi

RUN_NAME="${CONTAINER_NAME}"

case "${1:-run}" in
    --stop)
        docker rm -f "$RUN_NAME" 2>/dev/null || echo "no running container named $RUN_NAME"
        exit 0
        ;;
    --rebuild)
        REBUILD=1; shift ;;
    run|"")
        REBUILD=0 ;;
    *)
        echo "usage: $0 [run|--rebuild|--stop]" >&2
        exit 2 ;;
esac

SECRETS_FILE="${HOME}/.config/nanodatacenter/secrets.env"
if [[ -f "$SECRETS_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$SECRETS_FILE"
else
    echo "warning: $SECRETS_FILE not found; PG_PASSWORD must be in env" >&2
fi
PG_PASSWORD="${PG_PASSWORD:-${POSTGRES_PASSWORD:-}}"
if [[ -z "$PG_PASSWORD" ]]; then
    echo "ERROR: PG_PASSWORD unset" >&2
    exit 1
fi

if ! docker image inspect "$IMAGE_TAG" >/dev/null 2>&1 || [[ "$REBUILD" == "1" ]]; then
    echo "=== Building $IMAGE_TAG ==="
    docker build -t "$IMAGE_TAG" "$SCRIPT_DIR"
fi

docker rm -f "$RUN_NAME" 2>/dev/null || true

NET_ARGS=()
PORT_ARGS=()
HOST_ARGS=()
if [[ "$DOCKER_NETWORK" == "host" ]]; then
    NET_ARGS=(--network host)
    # Host networking: no port publishing possible.
else
    if [[ "$DOCKER_NETWORK" != "bridge" && -n "$DOCKER_NETWORK" ]]; then
        NET_ARGS=(--network "$DOCKER_NETWORK")
    fi
    PORT_ARGS=(-p "${WEB_PORT_EXCEPTION}:8080" -p "${WEB_PORT_LOG}:8081")
    # Guarantee host.docker.internal resolves even on native-Linux Docker
    # (on Docker Desktop it already exists; --add-host is idempotent).
    HOST_ARGS=(--add-host "host.docker.internal:host-gateway")
fi

echo "=== Running $IMAGE_TAG as $RUN_NAME ==="
echo "  site=$APP_SITE cpu=$APP_CPU_ID pg=$PG_HOST:$PG_PORT/$PG_DB (user=$PG_USER)"
echo "  network=${DOCKER_NETWORK}"
if [[ "$DOCKER_NETWORK" == "host" ]]; then
    echo "  web: http://localhost:8080 (exception_web)  http://localhost:8081 (log_web)"
else
    echo "  web: http://localhost:${WEB_PORT_EXCEPTION} (exception_web)  http://localhost:${WEB_PORT_LOG} (log_web)"
fi
echo "  ctrl-C / docker stop $RUN_NAME to exit cleanly"
echo

exec docker run --rm --name "$RUN_NAME" \
    "${NET_ARGS[@]}" \
    "${PORT_ARGS[@]}" \
    "${HOST_ARGS[@]}" \
    -e CONTAINER_NAME="$CONTAINER_NAME" \
    -e APP_SITE="$APP_SITE" \
    -e APP_CPU_ID="$APP_CPU_ID" \
    -e PG_HOST="$PG_HOST" \
    -e PG_PORT="$PG_PORT" \
    -e PG_DB="$PG_DB" \
    -e PG_USER="$PG_USER" \
    -e PG_PASSWORD="$PG_PASSWORD" \
    -e NATS_URL="$NATS_URL" \
    -e MQTT_HOST="$MQTT_HOST" \
    "$IMAGE_TAG"
