# ROS Planner Container — generic planner image
#
# Uses prebuilt .so files from host (same aarch64 architecture).
# Build context: this directory (third_party_containers/ros_planner/)
# Lua modules copied from building_blocks/ via docker_build.sh staging.
#
# Build:  ./docker_build.sh
# Run:    ./docker_run.sh [domain]

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    luajit libsqlite3-0 libcjson1 libmosquitto1 libssl3t64 \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/lib/aarch64-linux-gnu/libsqlite3.so.0 /usr/lib/aarch64-linux-gnu/libsqlite3.so

# ---------------------------------------------------------------
# Prebuilt C shared libraries (from host)
# ---------------------------------------------------------------
COPY prebuilt_libs/ /usr/local/lib/
RUN ldconfig

# ---------------------------------------------------------------
# Lua modules layout: /opt/planner/lua/
# ---------------------------------------------------------------
WORKDIR /opt/planner

# -- ros_planner_ii modules --
COPY lua/action_server/    lua/action_server/
COPY lua/global_planner/   lua/global_planner/
COPY lua/local_planner/    lua/local_planner/
COPY lua/protocol/         lua/protocol/
COPY lua/hub_dsl.lua       lua/hub_dsl.lua
COPY lua/hub_functions/    lua/hub_functions/
COPY lua/kb/               lua/kb/
COPY lua/kb_construct/     lua/kb_construct/
COPY lua/runtime/          lua/runtime/
COPY lua/test_robot/       lua/test_robot/

# -- MQTT robot --
COPY lua/mqtt_robot/       lua/mqtt_robot/

# -- ChainTree runtime --
COPY lua/ct_runtime/       lua/ct_runtime/
COPY lua/ct_json/          lua/ct_json/
COPY lua/ct_dsl/           lua/ct_dsl/
COPY lua/lua_support/      lua/lua_support/

# -- Knowledge base Lua wrappers --
COPY lua/nats/             lua/nats/
COPY lua/mqtt/             lua/mqtt/
COPY lua/sqlite_kb/        lua/sqlite_kb/

# -- Entrypoint + server --
COPY lua/planner_server.lua      lua/planner_server.lua
COPY lua/bootstrap_container.lua lua/bootstrap_container.lua

# ---------------------------------------------------------------
# LUA_PATH / LUA_CPATH / LD_LIBRARY_PATH
# ---------------------------------------------------------------
ENV LUA_PATH="/opt/planner/lua/action_server/?.lua;\
/opt/planner/lua/global_planner/?.lua;\
/opt/planner/lua/local_planner/?.lua;\
/opt/planner/lua/protocol/?.lua;\
/opt/planner/lua/?.lua;\
/opt/planner/lua/hub_functions/?.lua;\
/opt/planner/lua/kb/?.lua;\
/opt/planner/lua/kb_construct/?.lua;\
/opt/planner/lua/sqlite_kb/?.lua;\
/opt/planner/lua/runtime/?.lua;\
/opt/planner/lua/test_robot/?.lua;\
/opt/planner/lua/mqtt_robot/?.lua;\
/opt/planner/lua/ct_runtime/?.lua;\
/opt/planner/lua/ct_json/?.lua;\
/opt/planner/lua/ct_dsl/?.lua;\
/opt/planner/lua/lua_support/?.lua;\
/opt/planner/lua/nats/?.lua;\
/opt/planner/lua/nats/lib/?.lua;\
/opt/planner/lua/mqtt/?.lua;\
/opt/planner/lua/mqtt/lib/?.lua;\
?.lua;;"

ENV LUA_CPATH="/usr/local/lib/?.so;;"

ENV LD_LIBRARY_PATH="/usr/local/lib"

WORKDIR /opt/planner/lua

CMD ["luajit", "/opt/planner/lua/bootstrap_container.lua"]
