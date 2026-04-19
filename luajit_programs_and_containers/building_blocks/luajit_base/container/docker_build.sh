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
BB_DIR="$SCRIPT_DIR/../.."   # building_blocks/

CT="$BB_DIR/chain_tree_luajit"
DCS_HP="$BB_DIR/nanodatacenter_dcs/runtime/dcs_host"

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
for f in posix_time.lua pg_connector.lua kb_status.lua kb_stream.lua kb_exception.lua bit_mask_helpers.lua; do
    cp "$DCS_HP/$f" "$SCRIPT_DIR/prebuilt_lua_share/"
done

# sqlite3_helpers lives under knowledge_base/sqlite3; the supervisor +
# bundler both import it via LUA_PATH.
cp "$BB_DIR/knowledge_base/sqlite3/construct_kb/sqlite3_helpers.lua" \
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
