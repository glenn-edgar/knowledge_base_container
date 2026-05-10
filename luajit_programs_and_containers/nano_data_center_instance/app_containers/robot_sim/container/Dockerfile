# nanodatacenter/robot-sim -- single-process robot simulator (Phase 7 ROBSIM C2).
#
# Connects to MQTT, performs URLP link handshake to register with the
# planner, and echoes ack + kb_done for every RPC command (drive
# packets + activate actions). No physics, no movement -- the planner
# sees every action complete successfully (blind echo).
#
# Code shape mirrors nano_data_center_base/.../scripts/mock_mqtt_robot.lua
# (the Phase 3a C1 host-side test fixture) but container-fitted:
#   - env-driven instead of CLI args
#   - no SIGINT handler (docker SIGTERM kills the process)
#   - no --once mode (loops until killed)
#
# Per-instance robot identity (ROBOT_ID, PLANNER_NAMESPACE, MQTT_HOST,
# MQTT_PORT) sourced from env at startup -- see container_spec.env_required.
# Operator passes -e flags at `docker run` time during cluster smoke until
# node_control's params->env mapping lands.

FROM nanodatacenter/luajit-base:latest

# Apt deps for libmqtt_pubsub.so (libmosquitto1 is its runtime dep;
# vendoring the .so alone wouldn't pull it in).
RUN apt-get update && \
    apt-get install -y --no-install-recommends libmosquitto1 && \
    rm -rf /var/lib/apt/lists/*

# Vendor FFI .so files (sourced from mission_planner's prebuilt_libs;
# shared codecs across the planner-and-robot pair):
#   libmqtt_pubsub.so -- MQTT pub/sub wrapper
#   liblua_cbor.so    -- CBOR codec, needed to decode v2 drive packets
#                        that the planner sends as raw CBOR bytes.
COPY prebuilt_libs/libmqtt_pubsub.so /usr/local/lib/
COPY prebuilt_libs/liblua_cbor.so    /usr/local/lib/
RUN ldconfig

# Copy the simulator source tree.
#   /opt/apps/robot_sim/main.lua            -- supervised process entry
#   /opt/apps/robot_sim/app.manifest.json   -- bundle_controller entry
#   /opt/apps/robot_sim/lib/                -- mock_mqtt_robot_lib.lua
#                                              + lib/mqtt_pubsub.lua
COPY robot_sim/ /opt/apps/robot_sim/

RUN /usr/local/bin/bundle_controller

# CMD inherited from luajit-base (supervisor/entrypoint.lua).
