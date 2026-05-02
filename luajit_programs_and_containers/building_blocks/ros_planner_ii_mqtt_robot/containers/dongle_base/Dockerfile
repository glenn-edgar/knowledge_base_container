# mycorp/dongle_base:1.0 -- USB protocol + internal software bus +
# virtual robot framework + master-side Lua infrastructure.
#
# Layer sandwich:
#   nanodatacenter/luajit-base   (LuaJIT + chain-tree runtime)
#     mycorp/robot_base          (supervisor + ENV gate + render-config)
#       mycorp/dongle_base       (THIS IMAGE — comm + physics + Lua)
#         mycorp/<class>-class   (one image per logical_robot type)
#
# Class images consume:
#   - /opt/dongle_base/lib/{libcomm.a,libphysics.a}  for static link
#   - /opt/dongle_base/include/                       headers
#   - /opt/dongle_base/lua/                           generic Lua modules
#   - /usr/local/lib/{libcomm.so,libphysics.so,libmqtt_pubsub.so}
#                                                     for ffi.load
# Class images own:
#   - robot_sim main.c + dongle_threads.c (linked against libs above
#     until vtable abstraction lands)
#   - logical_robot impl (drive_base_robot.c et al.)
#   - per-class manifest -> comm_manifest.bin
#   - tunables.bin, remote.json, capabilities, configs

# -----------------------------------------------------------------------------
# Stage 1 -- builder: produces libcomm.{a,so} + libphysics.{a,so}
# -----------------------------------------------------------------------------
FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY stage/ /build/

RUN make -f Makefile

# -----------------------------------------------------------------------------
# Stage 2 -- runtime
# -----------------------------------------------------------------------------
FROM mycorp/robot_base:1.0

# libmosquitto1 backs libmqtt_pubsub.so (used by mqtt_pubsub.lua FFI).
RUN apt-get update && apt-get install -y --no-install-recommends \
        libmosquitto1 \
    && rm -rf /var/lib/apt/lists/*

# Static archives + headers (consumed by class image's robot_sim build)
RUN mkdir -p /opt/dongle_base/lib /opt/dongle_base/include/libcomm \
             /opt/dongle_base/lua /opt/dongle_base/test_harness
COPY --from=builder /build/libcomm.a    /opt/dongle_base/lib/
COPY --from=builder /build/libphysics.a /opt/dongle_base/lib/
COPY --from=builder /build/libcomm/*.h         /opt/dongle_base/include/libcomm/
COPY --from=builder /build/comm_manifest.h     /opt/dongle_base/include/
COPY --from=builder /build/comm_manifest_bin.h /opt/dongle_base/include/
COPY --from=builder /build/physics_pipe.h      /opt/dongle_base/include/

# Shared libs for Lua FFI consumers (master side: comm_ffi, physics_ffi,
# mqtt_pubsub). ldconfig'd so ffi.load("libcomm") / ffi.load("mqtt_pubsub")
# resolve without LD_LIBRARY_PATH gymnastics.
COPY --from=builder /build/libcomm.so    /usr/local/lib/
COPY --from=builder /build/libphysics.so /usr/local/lib/
COPY stage/native_libs/libmqtt_pubsub.so /usr/local/lib/
RUN ldconfig

# Generic Lua (master-side, class-agnostic).
COPY stage/lua_modules/ /opt/dongle_base/lua/

# Test harness (mock planner + paths runner). Class images inherit
# this and add their own scenario specifics.
COPY stage/test_harness/ /opt/dongle_base/test_harness/

# LUA_PATH precedence: class /opt/apps/<class>/ first (set by class
# image), then dongle_base lua, then luajit_base supervisor, then
# system. CT runtime_dict + lib/ subdirs flattened so ct_*.lua and
# lib/mqtt_pubsub.lua are reachable.
ENV LUA_PATH="/opt/dongle_base/lua/?.lua;\
/opt/dongle_base/lua/runtime_dict/?.lua;\
/opt/dongle_base/lua/lib/?.lua;\
/opt/robot_base/supervisor/?.lua;\
/opt/luajit_base/supervisor/?.lua;\
/usr/local/share/lua/5.1/?.lua;\
/usr/local/share/lua/5.1/chain_tree/?.lua;\
?.lua;;"

# /usr/local/lib already on default ldconfig path; class image won't
# need to touch this.
ENV LD_LIBRARY_PATH="/usr/local/lib"

# Inherits robot_base's CMD (luajit /opt/robot_base/supervisor/entrypoint.lua).
