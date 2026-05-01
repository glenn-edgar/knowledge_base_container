#!/usr/bin/env bash
# build_kb.sh -- Run construct_dcs_kb.lua with LUA_PATH wired to the postgres
# construct_kb library. Sources operator secrets so POSTGRES_PASSWORD is set.
#
# Usage: ./build_kb.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# NDC_BASE = nano_data_center_base/ root. Default = climb 3 levels (this script
# lives at <NDC_BASE>/commissioning_software/system_node_control/construction/).
NDC_BASE="${NDC_BASE:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
PG_KB_DIR="$NDC_BASE/commissioning_software/kb/postgres/construct_kb"

if [[ ! -d "$PG_KB_DIR" ]]; then
    echo "expected postgres construct_kb at $PG_KB_DIR" >&2
    exit 1
fi

SECRETS_FILE="${HOME}/.config/nanodatacenter/secrets.env"
if [[ ! -f "$SECRETS_FILE" ]]; then
    echo "missing $SECRETS_FILE -- run setup_secrets.sh first" >&2
    exit 1
fi
# shellcheck disable=SC1090
source "$SECRETS_FILE"

if [[ -z "${PG_PASSWORD:-}" ]]; then
    echo "PG_PASSWORD not set after sourcing $SECRETS_FILE" >&2
    exit 1
fi

# the postgres construct lib expects POSTGRES_PASSWORD
export POSTGRES_PASSWORD="$PG_PASSWORD"
export LUA_PATH="$PG_KB_DIR/?.lua;$SCRIPT_DIR/?.lua;?.lua;;"

cd "$SCRIPT_DIR"
exec luajit build_kb.lua "$@"
