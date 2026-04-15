# nanodatacenter/luajit-base -- platform image for multi-app LuaJIT containers.
#
# Contract:
#   - App image is FROM nanodatacenter/luajit-base:latest
#   - App build drops one or more app artifacts into /opt/apps/<name>/
#     then invokes /usr/local/bin/bundle_controller to build controller.db
#   - Base image CMD runs the supervisor which reads controller.db,
#     spawns each app per its command_map row, supervises for life of
#     container.
#
# Required runtime env (set by docker run from node_control):
#   CONTAINER_NAME  APP_SITE  APP_CPU_ID
#   PG_HOST  PG_PORT  PG_DB  PG_USER  PG_PASSWORD
# Optional: NATS_URL  MQTT_HOST

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
        luajit libpq5 libsqlite3-0 sqlite3 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && SQLITE_SO="$(find /usr/lib -maxdepth 3 -name 'libsqlite3.so.0' | head -1)" \
    && [ -n "$SQLITE_SO" ] \
    && ln -sf "$SQLITE_SO" "$(dirname "$SQLITE_SO")/libsqlite3.so"

# Prebuilt shared libraries + Lua modules (staged by docker_build.sh).
COPY prebuilt_libs/              /usr/local/lib/
COPY prebuilt_lua_libs/           /usr/local/lib/lua/5.1/
COPY prebuilt_lua_share/          /usr/local/share/lua/5.1/
RUN ldconfig

# Supervisor (chain-tree controller) + bundler (image-build-time tool).
WORKDIR /opt/luajit_base
COPY supervisor/  supervisor/
COPY bundler/     bundler/

# /opt/apps is where app artifacts land. Apps self-contained per dir.
# /opt/luajit_base/controller.db is the bundler output consumed at boot.
RUN mkdir -p /opt/apps /opt/luajit_base/state

ENV LUA_PATH="/opt/luajit_base/supervisor/?.lua;\
/opt/luajit_base/bundler/?.lua;\
/usr/local/share/lua/5.1/?.lua;\
/usr/local/share/lua/5.1/chain_tree/?.lua;\
?.lua;;"

ENV LUA_CPATH="/usr/local/lib/lua/5.1/?.so;\
/usr/local/lib/lua/5.1/dbd/?.so;\
/usr/local/lib/?.so;;"

ENV LD_LIBRARY_PATH="/usr/local/lib"

# Bundler shim (used by app Dockerfiles in a multi-stage build).
RUN ln -s /opt/luajit_base/bundler/bundle.sh /usr/local/bin/bundle_controller

WORKDIR /opt/luajit_base
CMD ["luajit", "supervisor/entrypoint.lua", "/opt/luajit_base/controller.db"]
