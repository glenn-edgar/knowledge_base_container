#!/usr/bin/env bash
# docker_build.sh -- Fetch vendored web assets (htmx) if absent, then build
# the openresty-base image on top of luajit-base.
#
# Requires: nanodatacenter/luajit-base:latest already built (see ../luajit_base).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ASSETS_DIR="$SCRIPT_DIR/prebuilt_web_assets"
LUALIB_DIR="$SCRIPT_DIR/prebuilt_openresty_lualib"

HTMX_VERSION="1.9.12"
HTMX_URL="https://unpkg.com/htmx.org@${HTMX_VERSION}/dist/htmx.min.js"
# SSE extension ships inside the htmx package under dist/ext/. Paired
# version with HTMX_VERSION so the two are always compatible.
HTMX_SSE_URL="https://unpkg.com/htmx.org@${HTMX_VERSION}/dist/ext/sse.js"

PGMOON_VERSION="1.16.0"
PGMOON_URL="https://github.com/leafo/pgmoon/archive/refs/tags/v${PGMOON_VERSION}.tar.gz"

# lua-resty-openssl supplies pgmoon's scram-sha-256 auth path (hmac/digest/
# kdf/rand via luaossl FFI). pg17's default password_encryption is
# scram-sha-256, so without this vendor drop pgmoon cannot authenticate.
LUA_RESTY_OPENSSL_VERSION="1.7.1"
LUA_RESTY_OPENSSL_URL="https://codeload.github.com/fffonion/lua-resty-openssl/tar.gz/refs/tags/${LUA_RESTY_OPENSSL_VERSION}"

# ---- 1. Confirm luajit-base is present ------------------------------------
if ! docker image inspect nanodatacenter/luajit-base:latest >/dev/null 2>&1; then
    echo "ERROR: nanodatacenter/luajit-base:latest not found." >&2
    echo "  Build it first: ../../luajit_base/container/docker_build.sh" >&2
    exit 1
fi

# ---- 2. Stage vendored web assets -----------------------------------------
mkdir -p "$ASSETS_DIR/htmx"
if [[ ! -f "$ASSETS_DIR/htmx/htmx.min.js" ]]; then
    echo "=== Fetching htmx ${HTMX_VERSION} ==="
    curl -fSL "$HTMX_URL" -o "$ASSETS_DIR/htmx/htmx.min.js"
    echo "${HTMX_VERSION}" > "$ASSETS_DIR/htmx/VERSION"
fi

echo "  Staged htmx: $(ls -lh "$ASSETS_DIR/htmx/htmx.min.js" | awk '{print $5}')"

if [[ ! -f "$ASSETS_DIR/htmx/sse.js" ]]; then
    echo "=== Fetching htmx-ext-sse (paired with htmx ${HTMX_VERSION}) ==="
    curl -fSL "$HTMX_SSE_URL" -o "$ASSETS_DIR/htmx/sse.js"
fi
echo "  Staged htmx-ext-sse: $(ls -lh "$ASSETS_DIR/htmx/sse.js" | awk '{print $5}')"

# ---- 3. Stage vendored openresty lualib (pgmoon) -------------------------
mkdir -p "$LUALIB_DIR"
if [[ ! -f "$LUALIB_DIR/pgmoon/init.lua" ]]; then
    echo "=== Fetching pgmoon ${PGMOON_VERSION} ==="
    TMP_TARBALL="$(mktemp)"
    curl -fSL "$PGMOON_URL" -o "$TMP_TARBALL"
    # Tarball layout: pgmoon-<version>/pgmoon/*.lua  (we want just pgmoon/).
    tar -xzf "$TMP_TARBALL" -C "$LUALIB_DIR" --strip-components=1 \
        "pgmoon-${PGMOON_VERSION}/pgmoon"
    rm -f "$TMP_TARBALL"
    echo "${PGMOON_VERSION}" > "$LUALIB_DIR/pgmoon/VERSION"
fi
echo "  Staged pgmoon: $(find "$LUALIB_DIR/pgmoon" -name '*.lua' | wc -l) lua files"

# ---- 3b. Stage lua-resty-openssl (scram-sha-256 deps for pgmoon) --------
if [[ ! -f "$LUALIB_DIR/resty/openssl.lua" ]]; then
    echo "=== Fetching lua-resty-openssl ${LUA_RESTY_OPENSSL_VERSION} ==="
    TMP_TARBALL="$(mktemp)"
    curl -fSL --retry 3 "$LUA_RESTY_OPENSSL_URL" -o "$TMP_TARBALL"
    # Tarball layout: lua-resty-openssl-<ver>/lib/resty/openssl.lua
    #                 lua-resty-openssl-<ver>/lib/resty/openssl/*.lua
    # --strip-components=2 strips the top dir and lib/, so resty/openssl*
    # lands at the extract root and merges cleanly with other prebuilt files.
    tar -xzf "$TMP_TARBALL" -C "$LUALIB_DIR" --strip-components=2 \
        "lua-resty-openssl-${LUA_RESTY_OPENSSL_VERSION}/lib/resty"
    rm -f "$TMP_TARBALL"
    echo "${LUA_RESTY_OPENSSL_VERSION}" > "$LUALIB_DIR/resty/openssl/VERSION"
fi
echo "  Staged lua-resty-openssl: $(find "$LUALIB_DIR/resty/openssl" -name '*.lua' | wc -l) lua files"

# ---- 4. Build ------------------------------------------------------------
echo ""
echo "=== Building Docker image ==="
docker build -t nanodatacenter/openresty-base:latest "$SCRIPT_DIR"

docker images nanodatacenter/openresty-base:latest \
    --format "  Image: {{.Repository}}:{{.Tag}}  Size: {{.Size}}"
