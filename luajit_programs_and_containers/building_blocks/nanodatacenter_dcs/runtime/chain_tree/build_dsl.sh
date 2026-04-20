#!/usr/bin/env bash
# build_dsl.sh -- Compile dcs_dsl.lua to JSON IR (dcs.json) + debug YAML.
# Usage: ./build_dsl.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CT_LUAJIT="$SCRIPT_DIR/../../../chain_tree_luajit"
CT_DSL="$CT_LUAJIT/lua_dsl"

[[ -d "$CT_DSL" ]] || { echo "missing $CT_DSL" >&2; exit 1; }

export LUA_PATH="$CT_DSL/?.lua;$CT_DSL/lua_support/?.lua;$CT_DSL/luajit_pipeline/?.lua;$SCRIPT_DIR/?.lua;?.lua;;"

cd "$SCRIPT_DIR"
rm -f dcs.json dcs_debug.yaml
luajit dcs_dsl.lua dcs.json
ls -l dcs.json dcs_debug.yaml 2>/dev/null || true
