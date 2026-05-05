# nanodatacenter/mission-planner -- two-process container (Phase B Layer A).
#
# Processes (supervised children of luajit-base's chain-tree controller):
#   1. planner    -- lua worker shell (heartbeats only; planner library
#                    integration lands in Phase B.2).
#   2. planner_ui -- openresty, internal port 8090, placeholder page.
#
# Manifest + container_spec at the package root
# (../manifest.lua, ../container_spec.lua) -- this Dockerfile builds the
# runtime image; the apps-builder framework consumes manifest.lua at
# commission time to write KB rows under
# system.<sys>.site.<S>.app_containers.mission_planner_01.spec.manifest.*

FROM nanodatacenter/openresty-base:latest

# Apt deps for the FFI wrappers shipped below:
#   libcjson1     -- runtime dep of libnats_job_queue.so (cJSON parsing)
#   libmosquitto1 -- runtime dep of libmqtt_pubsub.so (MQTT 3.1.1 client)
# Vendoring the .so's alone wouldn't pull these in; apt manages their
# shared deps (libssl3, libcrypto, etc.) properly. Install BEFORE copying
# the wrapper .so's so ldconfig sees the full graph.
RUN apt-get update && \
    apt-get install -y --no-install-recommends libcjson1 libmosquitto1 && \
    rm -rf /var/lib/apt/lists/*

# Vendor FFI .so files (Phase B.2.A.3).
# Sourced from:
#   - NATS:    third_party_containers/ros_planner/prebuilt_libs/
#   - MQTT:    nano_data_center_base/commissioning_software/kb/mqtt/
#
# Vendored per-app (here) for now; promote to luajit-base/prebuilt_libs/
# when a second app needs them.
#
# NATS:
#   - libnats.so.3.13.0 (official C client)
#   - libnats_key_store.so + libnats_job_queue.so (FFI wrappers used by
#     action_server). pubsub/rpc/kb_store/stream wrappers NOT vendored:
#     pubsub had an unsafe ffi.cast async pattern (project_phase6_transport
#     memory) and the others aren't used.
#
# MQTT:
#   - libmqtt_pubsub.so (FFI wrapper used by runtime/mqtt_transport.lua,
#     mqtt_hub_transport.lua via lib.mqtt_pubsub).
#   - liblua_cbor.so (FFI wrapper used by link_client / link_manager /
#     mqtt_transport via lib.lua_cbor).
#
# Dockerfile recreates the libnats SONAME chain (cp -L flattened it).
COPY prebuilt_libs/libnats.so.3.13.0      /usr/local/lib/
COPY prebuilt_libs/libnats_key_store.so   /usr/local/lib/
COPY prebuilt_libs/libnats_job_queue.so   /usr/local/lib/
COPY prebuilt_libs/libmqtt_pubsub.so      /usr/local/lib/
COPY prebuilt_libs/liblua_cbor.so         /usr/local/lib/
RUN ln -sf libnats.so.3.13.0 /usr/local/lib/libnats.so.3.13 && \
    ln -sf libnats.so.3.13   /usr/local/lib/libnats.so      && \
    ldconfig

COPY planner/    /opt/apps/planner/
COPY planner_ui/ /opt/apps/planner_ui/

# nginx default error-log path is tried before the config override; create
# logs/ to silence the cosmetic alert at startup.
RUN mkdir -p /opt/apps/planner_ui/logs

RUN /usr/local/bin/bundle_controller

# CMD inherited from luajit-base (supervisor/entrypoint.lua).
