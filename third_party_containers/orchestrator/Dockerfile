# Orchestrator Container — one-shot lifecycle management
#
# Modes (set via CMD override):
#   startup   — start containers in dependency order
#   shutdown  — stop containers in reverse dependency order
#   kb_build  — rebuild KB then restart containers
#
# Requires: Docker socket mount (-v /var/run/docker.sock)
#           Postgres accessible from container network

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    luajit libpq5 docker.io sqlite3 \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/lib/aarch64-linux-gnu/libsqlite3.so.0 /usr/lib/aarch64-linux-gnu/libsqlite3.so

# ---------------------------------------------------------------
# Prebuilt libraries (from host)
# ---------------------------------------------------------------
COPY prebuilt_libs/ /usr/local/lib/
COPY prebuilt_lua_libs/ /usr/local/lib/lua/5.1/
COPY prebuilt_lua_share/ /usr/local/share/lua/5.1/
RUN ldconfig

# ---------------------------------------------------------------
# Lua modules
# ---------------------------------------------------------------
WORKDIR /opt/orchestrator

COPY lua/ lua/

ENV LUA_PATH="/opt/orchestrator/lua/?.lua;\
/opt/orchestrator/lua/orchestrator/?.lua;\
/opt/orchestrator/lua/kb_search/?.lua;\
/opt/orchestrator/lua/kb_construct/?.lua;\
/opt/orchestrator/lua/sqlite_kb/?.lua;\
?.lua;;"

ENV LUA_CPATH="/usr/local/lib/lua/5.1/?.so;\
/usr/local/lib/lua/5.1/dbd/?.so;\
/usr/local/lib/?.so;;"

ENV LD_LIBRARY_PATH="/usr/local/lib"

WORKDIR /opt/orchestrator/lua

CMD ["luajit", "startup.lua"]
