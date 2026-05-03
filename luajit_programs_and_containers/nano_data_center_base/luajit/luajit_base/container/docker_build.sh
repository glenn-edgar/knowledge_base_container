#!/usr/bin/env bash
# docker_build.sh -- Stage prebuilt libs + chain-tree runtime + DCS helpers
# into the build context, then `docker build` the luajit-base image.
#
# Sources (read-only):
#   ../../chain_tree_luajit/runtime/              -> chain_tree/
#   ../../chain_tree_luajit/lua_dsl/              -> chain_tree/lua_dsl/
#   ../../nanodatacenter_dcs/runtime/dcs_host/*.lua -> lua share/
#   /usr/local/lib/lua/5.1/dbd/postgresql.so
#   /usr/local/share/lua/5.1/{DBI,dkjson}.lua
#   /usr/local/lib/ltree.so
#
# Output: docker image nanodatacenter/luajit-base:latest

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# NDC_BASE = nano_data_center_base/. This script lives at
# <NDC_BASE>/luajit/luajit_base/container/, so default = climb 3 levels.
NDC_BASE="${NDC_BASE:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"

CT="$NDC_BASE/commissioning_software/engines/chain_tree"
DCS_HP="$NDC_BASE/commissioning_software/system_node_control/runtime/dcs_host"

for d in "$CT/runtime" "$CT/lua_dsl" "$DCS_HP"; do
    [[ -d "$d" ]] || { echo "ERROR: missing source: $d" >&2; exit 1; }
done

echo "=== Staging prebuilt artifacts into $SCRIPT_DIR ==="

rm -rf "$SCRIPT_DIR/prebuilt_libs" \
       "$SCRIPT_DIR/prebuilt_lua_libs" \
       "$SCRIPT_DIR/prebuilt_lua_share"

mkdir -p "$SCRIPT_DIR/prebuilt_libs" \
         "$SCRIPT_DIR/prebuilt_lua_libs/dbd" \
         "$SCRIPT_DIR/prebuilt_lua_share/chain_tree" \
         "$SCRIPT_DIR/prebuilt_lua_share/chain_tree/lua_dsl" \
         "$SCRIPT_DIR/prebuilt_lua_share/chain_tree/lua_dsl/lua_support" \
         "$SCRIPT_DIR/prebuilt_lua_share/chain_tree/lua_dsl/luajit_pipeline"

# chain_tree runtime (.lua) -> share/chain_tree/
cp "$CT/runtime/"*.lua "$SCRIPT_DIR/prebuilt_lua_share/chain_tree/"

# chain_tree DSL (used by the bundler if it ever compiles a controller DSL;
# included for parity with orchestrator's staging pattern).
cp "$CT/lua_dsl/"*.lua                       "$SCRIPT_DIR/prebuilt_lua_share/chain_tree/lua_dsl/"
cp "$CT/lua_dsl/lua_support/"*.lua           "$SCRIPT_DIR/prebuilt_lua_share/chain_tree/lua_dsl/lua_support/" 2>/dev/null || true
cp "$CT/lua_dsl/luajit_pipeline/"*.lua       "$SCRIPT_DIR/prebuilt_lua_share/chain_tree/lua_dsl/luajit_pipeline/" 2>/dev/null || true

# DCS helpers reused verbatim as base-image libraries.
# ndc_paths.lua is a dependency of bit_mask_helpers.lua (path composer);
# must ship together or supervisor boot fails with module-not-found.
for f in posix_time.lua pg_connector.lua kb_status.lua kb_stream.lua kb_exception.lua bit_mask_helpers.lua ndc_paths.lua; do
    cp "$DCS_HP/$f" "$SCRIPT_DIR/prebuilt_lua_share/"
done

# App-side helpers that ship with luajit_base. Source-of-truth lives in
# app_lib/ (not under prebuilt_lua_share/, which gets wiped at the start
# of every build). All .lua files are staged into /usr/local/share/lua/5.1/
# inside the image so app-tier code can `require("...")` them.
if [[ -d "$SCRIPT_DIR/app_lib" ]]; then
    cp "$SCRIPT_DIR/app_lib/"*.lua "$SCRIPT_DIR/prebuilt_lua_share/"
fi

# sqlite3_helpers lives under knowledge_base/sqlite3; the supervisor +
# bundler both import it via LUA_PATH.
cp "$NDC_BASE/commissioning_software/kb/sqlite3/construct_kb/sqlite3_helpers.lua" \
   "$SCRIPT_DIR/prebuilt_lua_share/"

# Phase 6.4: kb_sync_queue is the runtime client used by container_rpc_client.
cp "$NDC_BASE/commissioning_software/kb/postgres/data_structures/kb_sync_queue.lua" \
   "$SCRIPT_DIR/prebuilt_lua_share/"

# DSL -> JSON IR compile now happens INSIDE the image (see Dockerfile
# RUN step). No host-side luajit/chain_tree dependency required to build.
# Stale artifacts from previous runs are removed here for hygiene.
rm -f "$SCRIPT_DIR/supervisor/controller.json" \
      "$SCRIPT_DIR/supervisor/controller_debug.yaml"

# DBI + dkjson + cjson + ltree extension (built on host; reused).
cp /usr/local/lib/lua/5.1/dbd/postgresql.so  "$SCRIPT_DIR/prebuilt_lua_libs/dbd/"
cp /usr/local/share/lua/5.1/DBI.lua          "$SCRIPT_DIR/prebuilt_lua_share/"
cp /usr/local/lib/lua/5.1/cjson.so           "$SCRIPT_DIR/prebuilt_lua_libs/"
[[ -f /usr/local/share/lua/5.1/dkjson.lua ]] && \
    cp /usr/local/share/lua/5.1/dkjson.lua   "$SCRIPT_DIR/prebuilt_lua_share/"
[[ -f /usr/local/lib/ltree.so ]] && \
    cp /usr/local/lib/ltree.so               "$SCRIPT_DIR/prebuilt_libs/"

echo "  Staged $(find "$SCRIPT_DIR/prebuilt_lua_share" -name '*.lua' | wc -l) Lua files"

echo ""
echo "=== Building Docker image ==="
docker build -t nanodatacenter/luajit-base:latest "$SCRIPT_DIR"

docker images nanodatacenter/luajit-base:latest \
    --format "  Image: {{.Repository}}:{{.Tag}}  Size: {{.Size}}"
