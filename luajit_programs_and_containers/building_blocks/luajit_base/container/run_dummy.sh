#!/usr/bin/env bash
# run_dummy.sh -- Build (if needed) and run the dummy smoke-test container.
#
# Sources ~/.config/nanodatacenter/secrets.env for PG_PASSWORD (same file
# DCS start.sh uses). Other pg-connection values come from env with
# sensible defaults matching the local dev pg-vector container.
#
# Usage:
#   ./run_dummy.sh              run (builds image first if missing)
#   ./run_dummy.sh --rebuild    force rebuild of dummy image before run
#   ./run_dummy.sh --stop       docker stop + rm the running container
#
# Env overrides:
#   CONTAINER_NAME (default: dummy_test)
#   APP_SITE       (default: dev)
#   APP_CPU_ID     (default: cpu_01)
#   PG_HOST        (default: 127.0.0.1 or host.docker.internal if unset on linux)
#   PG_PORT        (default: 5432)
#   PG_DB          (default: knowledge_base)
#   PG_USER        (default: postgres)
#   DOCKER_NETWORK (default: host; use "planner-net" to join that network,
#                   or empty for default bridge)
#   IMAGE_TAG      (default: nanodatacenter/luajit-dummy:latest)
#
# Host networking is the default so the container can reach pg on
# 127.0.0.1:<PG_PORT> the same way DCS on the host does -- matches the
# "apps run on a CPU, pg runs wherever, reachable over the wire"
# deployment shape.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CONTAINER_NAME="${CONTAINER_NAME:-dummy_test}"
APP_SITE="${APP_SITE:-dev}"
APP_CPU_ID="${APP_CPU_ID:-cpu_01}"
PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-knowledge_base}"
PG_USER="${PG_USER:-gedgar}"
NATS_URL="${NATS_URL:-}"
MQTT_HOST="${MQTT_HOST:-}"
DOCKER_NETWORK="${DOCKER_NETWORK:-host}"
IMAGE_TAG="${IMAGE_TAG:-nanodatacenter/luajit-dummy:latest}"

RUN_NAME="${CONTAINER_NAME}"

case "${1:-run}" in
    --stop)
        docker rm -f "$RUN_NAME" 2>/dev/null || echo "no running container named $RUN_NAME"
        exit 0
        ;;
    --rebuild)
        REBUILD=1
        shift
        ;;
    run|"")
        REBUILD=0
        ;;
    *)
        echo "usage: $0 [run|--rebuild|--stop]" >&2
        exit 2
        ;;
esac

# ---------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------
SECRETS_FILE="${HOME}/.config/nanodatacenter/secrets.env"
if [[ -f "$SECRETS_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$SECRETS_FILE"
else
    echo "warning: $SECRETS_FILE not found; PG_PASSWORD must be in env" >&2
fi

# Accept either PG_PASSWORD or POSTGRES_PASSWORD from secrets.env.
PG_PASSWORD="${PG_PASSWORD:-${POSTGRES_PASSWORD:-}}"
if [[ -z "$PG_PASSWORD" ]]; then
    echo "ERROR: PG_PASSWORD unset (not in $SECRETS_FILE and not in env)" >&2
    exit 1
fi

# ---------------------------------------------------------------
# Build dummy image if missing or --rebuild
# ---------------------------------------------------------------
if ! docker image inspect "$IMAGE_TAG" >/dev/null 2>&1 || [[ "$REBUILD" == "1" ]]; then
    echo "=== Building $IMAGE_TAG ==="
    # The dummy Dockerfile COPYs dummy_app/ from build context root, so we
    # run docker build from the container/ dir (this script's dir).
    docker build -f dummy_app/Dockerfile -t "$IMAGE_TAG" .
fi

# ---------------------------------------------------------------
# Clean up any previous run with the same name
# ---------------------------------------------------------------
docker rm -f "$RUN_NAME" 2>/dev/null || true

# ---------------------------------------------------------------
# Run
# ---------------------------------------------------------------
NET_ARGS=()
if [[ -n "$DOCKER_NETWORK" ]]; then
    NET_ARGS=(--network "$DOCKER_NETWORK")
fi

echo "=== Running $IMAGE_TAG as $RUN_NAME ==="
echo "  site=$APP_SITE cpu=$APP_CPU_ID pg=$PG_HOST:$PG_PORT/$PG_DB (user=$PG_USER)"
echo "  network=${DOCKER_NETWORK:-<host>}"
echo "  ctrl-C / docker stop $RUN_NAME to exit cleanly"
echo

exec docker run --rm --name "$RUN_NAME" \
    "${NET_ARGS[@]}" \
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
