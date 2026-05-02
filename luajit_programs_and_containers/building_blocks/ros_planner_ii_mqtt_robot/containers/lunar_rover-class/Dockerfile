# mycorp/lunar_rover-class:1.0 -- one rover class on top of dongle_base.
#
# After the 2026-05-02 refactor, this image is slim: class-specific
# C (drive_base impl + sim host main), per-class manifest.bin,
# tunables.bin, mission state machine, and class manifests/configs.
# Everything generic lives in dongle_base.
#
# Multi-stage:
#   Stage 1 (builder): FROM mycorp/dongle_base:1.0 + build-essential.
#                      Final-links robot_sim against dongle_base's
#                      libcomm.a + libphysics.a.
#                      Bakes tunables.bin via build_drive_base_tunables.
#   Stage 2 (runtime): FROM mycorp/dongle_base:1.0; copies artifacts
#                      into /opt/apps/lunar_rover/.

# -----------------------------------------------------------------------------
# Stage 1 -- builder
# -----------------------------------------------------------------------------
FROM mycorp/dongle_base:1.0 AS builder

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential luajit \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY stage/ /build/

# Static-link robot_sim from class C + dongle_base archives.
RUN make -f Makefile

# Bake tunables.bin from physics_config.json. json_util ships in
# dongle_base under /opt/dongle_base/lua/.
RUN LUA_PATH="/opt/dongle_base/lua/?.lua;;" \
    luajit /build/build_drive_base_tunables.lua \
        /build/physics_config.json /build/tunables.bin && \
    test -s /build/tunables.bin

# -----------------------------------------------------------------------------
# Stage 2 -- runtime
# -----------------------------------------------------------------------------
FROM mycorp/dongle_base:1.0

WORKDIR /opt/apps/lunar_rover

# Final-linked robot_sim binary.
COPY --from=builder /build/robot_sim/robot_sim ./robot_sim/robot_sim

# Class-specific build artifacts.
COPY --from=builder /build/tunables.bin        ./
COPY stage/comm_manifest.bin                   ./
COPY stage/comm_manifest_ffi.lua               ./
COPY stage/remote.json                         ./
COPY stage/physics_config.json                 ./
COPY stage/sim_map.json                        ./
COPY stage/capabilities.lua                    ./

# Class manifests consumed by robot_base supervisor.
COPY stage/config.template.json /opt/apps/lunar_rover/
COPY stage/class_processes.json /opt/apps/lunar_rover/

# Prepend the class app dir to LUA_PATH so per-class Lua resolves
# first. dongle_base's path entries follow.
ENV LUA_PATH="/opt/apps/lunar_rover/?.lua;\
/opt/dongle_base/lua/?.lua;\
/opt/dongle_base/lua/runtime_dict/?.lua;\
/opt/dongle_base/lua/lib/?.lua;\
/opt/robot_base/supervisor/?.lua;\
/opt/luajit_base/supervisor/?.lua;\
/usr/local/share/lua/5.1/?.lua;\
/usr/local/share/lua/5.1/chain_tree/?.lua;\
?.lua;;"

# Class identity baked at image build; runtime ROBOT_CLASS must match.
ENV ROBOT_CLASS_BAKED=lunar_rover

# Default ENV. ROBOT_ID and DONGLE_INSTANCE have no defaults; missing
# => fail-stop on boot.
ENV ROBOT_CLASS=lunar_rover \
    MQTT_HOST=localhost \
    MQTT_PORT=1883 \
    VMRT_KB_SITE=moonbase.alpha.surface_ops \
    DONGLE_TYPE=1 \
    SLAVE_ADDR=1 \
    SPEED_FACTOR=1.0 \
    HAL_MODE=dongle \
    ENERGY_MAX=10000 \
    ENERGY_INFINITE=false \
    WIRE_FORMAT=json

# Inherits robot_base's CMD (luajit /opt/robot_base/supervisor/entrypoint.lua).
