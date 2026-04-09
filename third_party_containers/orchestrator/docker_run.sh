#!/bin/bash
# docker_run.sh -- Run the orchestrator container (one-shot).
#
# Usage:
#   ./docker_run.sh startup                    # start all containers
#   ./docker_run.sh shutdown                   # stop all containers
#   ./docker_run.sh shutdown --exclude postgres # stop all except postgres
#   ./docker_run.sh kb_build                   # rebuild KB + restart
#
# The container runs one command, then exits and is auto-removed.

set -e

MODE="${1:-startup}"
shift 2>/dev/null || true

NETWORK="${DOCKER_NETWORK:-planner-net}"
SQLITE_DATA="${SQLITE_DATA:-/home/gedgar/Sqlite_Data}"

# Map mode to Lua entry point
case "$MODE" in
    startup)  SCRIPT="startup.lua" ;;
    shutdown) SCRIPT="shutdown.lua" ;;
    kb_build) SCRIPT="kb_build.lua" ;;
    *)
        echo "Usage: $0 {startup|shutdown|kb_build} [-- extra args...]"
        exit 1
        ;;
esac

echo "=== Orchestrator: $MODE ==="

# Docker socket is required for container management
# Host network so orchestrator can reach Postgres on localhost
docker run --rm \
    --name "orchestrator-${MODE}" \
    --network host \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$SQLITE_DATA:/data" \
    -e PG_HOST="${PG_HOST:-127.0.0.1}" \
    -e PG_PORT="${PG_PORT:-5432}" \
    -e PG_DBNAME="${PG_DBNAME:-knowledge_base}" \
    -e PG_USER="${PG_USER:-gedgar}" \
    -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}" \
    -e SQLITE_DATA="/data" \
    nanodatacenter/orchestrator:latest \
    luajit "$SCRIPT" "$@"
