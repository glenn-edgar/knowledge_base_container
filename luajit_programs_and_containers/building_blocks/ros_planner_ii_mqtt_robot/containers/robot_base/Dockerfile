# mycorp/robot_base -- platform image for robot containers.
#
# Inherits luajit-base (chain-tree runtime, signal-safe spawn primitives).
# Adds:
#   - robot supervisor chain-tree (sync -> render_config -> spawn_sim ->
#     wait_for_ready -> spawn_mqtt_robot -> register_peer -> monitor)
#   - ENV validators (ROBOT_ID, ROBOT_CLASS, DONGLE_INSTANCE required)
#   - config template render helper
#   - upward_peer.lua stub (Phase 2 plug-in point)
#
# Class images derive: `FROM mycorp/robot_base:1.0`

FROM nanodatacenter/luajit-base:latest

WORKDIR /opt/robot_base
COPY supervisor/  supervisor/

# Compile the supervisor chain-tree DSL to JSON IR inside the image,
# matching the pattern luajit-base uses for self-contained builds.
RUN cd /opt/robot_base/supervisor && \
    LUA_PATH="/usr/local/share/lua/5.1/chain_tree/lua_dsl/?.lua;\
/usr/local/share/lua/5.1/chain_tree/lua_dsl/lua_support/?.lua;\
/usr/local/share/lua/5.1/chain_tree/lua_dsl/luajit_pipeline/?.lua;\
/usr/local/share/lua/5.1/chain_tree/?.lua;\
/usr/local/share/lua/5.1/?.lua;;" \
    luajit dsl.lua robot_supervisor.json && \
    test -s robot_supervisor.json

# Class images drop their artifacts here.
RUN mkdir -p /opt/apps /run/robot

# Supervisor needs to find env_validate/config_render/process_helpers
# alongside chain-tree runtime modules. Append its dir to LUA_PATH.
ENV LUA_PATH="/opt/robot_base/supervisor/?.lua;\
/opt/luajit_base/supervisor/?.lua;\
/usr/local/share/lua/5.1/?.lua;\
/usr/local/share/lua/5.1/chain_tree/?.lua;\
?.lua;;"

# Class image will install /opt/apps/<class>/ before container start.
# Default CMD assumes the class image set ROBOT_CLASS_BAKED via ENV; the
# supervisor reads ROBOT_CLASS from runtime ENV and validates against
# the baked value (fail-stop on mismatch).
CMD ["luajit", "/opt/robot_base/supervisor/entrypoint.lua"]
