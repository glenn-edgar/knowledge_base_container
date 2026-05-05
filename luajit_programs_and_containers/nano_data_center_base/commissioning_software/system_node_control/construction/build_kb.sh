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
DCS_RT_DIR="$NDC_BASE/commissioning_software/system_node_control/runtime/dcs_host"
AB_FW_DIR="$NDC_BASE/commissioning_software/apps_builder_framework"
# NDC_INSTANCE_BASE = nano_data_center_instance/ root, sibling of NDC_BASE.
# Apps_builder subsystem reads this to find each app's package directory.
NDC_INSTANCE_BASE="${NDC_INSTANCE_BASE:-$(cd "$NDC_BASE/.." && pwd)/nano_data_center_instance}"

if [[ ! -d "$PG_KB_DIR" ]]; then
    echo "expected postgres construct_kb at $PG_KB_DIR" >&2
    exit 1
fi
if [[ ! -d "$DCS_RT_DIR" ]]; then
    echo "expected dcs_host runtime dir at $DCS_RT_DIR" >&2
    exit 1
fi
if [[ ! -d "$AB_FW_DIR" ]]; then
    echo "expected apps_builder_framework at $AB_FW_DIR" >&2
    exit 1
fi
# NDC_INSTANCE_BASE is optional -- apps_builder skips gracefully if unset.
# Warn but don't fail: a fresh tree might not have any ported apps yet.
if [[ ! -d "$NDC_INSTANCE_BASE" ]]; then
    echo "warning: NDC_INSTANCE_BASE=$NDC_INSTANCE_BASE not found; apps_builder will skip" >&2
    NDC_INSTANCE_BASE=""
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
export NDC_INSTANCE_BASE
export LUA_PATH="$PG_KB_DIR/?.lua;$DCS_RT_DIR/?.lua;$AB_FW_DIR/?.lua;$SCRIPT_DIR/?.lua;?.lua;;"

cd "$SCRIPT_DIR"
exec luajit build_kb.lua "$@"
