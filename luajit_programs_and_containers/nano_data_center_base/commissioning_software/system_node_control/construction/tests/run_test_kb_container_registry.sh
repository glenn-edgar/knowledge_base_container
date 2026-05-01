#!/usr/bin/env bash
# run_test_kb_container_registry.sh -- Smoke-test the container registry
# helper (kb_container_registry.lua) against the live pg.
#
# Assumes ./build_kb.sh has been run at least once (so the knowledge_base
# and knowledge_base_status tables exist). The test writes two rows under
# the master CPU's CONTAINER_REGISTRY namespace, verifies them, and cleans
# up regardless of outcome.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
# tests live at nanodatacenter_dcs/construction/tests/; runtime/dcs_host
# modules needed: pg_connector, kb_container_registry.

SECRETS_FILE="${HOME}/.config/nanodatacenter/secrets.env"
if [[ ! -f "$SECRETS_FILE" ]]; then
    echo "missing $SECRETS_FILE" >&2
    exit 1
fi
# shellcheck disable=SC1090
source "$SECRETS_FILE"
export POSTGRES_PASSWORD="${PG_PASSWORD:-${POSTGRES_PASSWORD:-}}"
if [[ -z "$POSTGRES_PASSWORD" ]]; then
    echo "PG_PASSWORD not set after sourcing $SECRETS_FILE" >&2
    exit 1
fi

# runtime/dcs_host holds pg_connector + kb_container_registry;
# topology.lua lives at construction/catalogs/topology.lua.
HOST_PROCESSES="$SCRIPT_DIR/../../runtime/dcs_host"
CATALOGS="$SCRIPT_DIR/../catalogs"
export LUA_PATH="$HOST_PROCESSES/?.lua;$CATALOGS/?.lua;$SCRIPT_DIR/?.lua;?.lua;;"

cd "$SCRIPT_DIR"
exec luajit test_kb_container_registry.lua
