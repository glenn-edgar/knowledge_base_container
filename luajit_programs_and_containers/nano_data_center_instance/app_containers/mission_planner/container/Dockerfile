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

# libcjson1 is a runtime dep of libnats_job_queue.so (the FFI wrapper
# parses CBOR/JSON via cJSON). Apt-installed because it's the upstream
# package on Debian; vendoring the lib alone wouldn't pull in its own
# shared deps (which apt does manage). Install BEFORE copying the .so's
# so ldconfig sees them all together.
RUN apt-get update && \
    apt-get install -y --no-install-recommends libcjson1 && \
    rm -rf /var/lib/apt/lists/*

# Vendor the NATS C client + selected FFI wrappers (Phase B.2.A.3).
# Sourced from third_party_containers/ros_planner/prebuilt_libs/ at build
# time. Only key_store + job_queue wrappers are shipped -- action_server
# uses those, and pubsub had an unsafe ffi.cast async pattern (per the
# project_phase6_transport memory). Dockerfile creates the SONAME chain
# (libnats.so -> libnats.so.3.13 -> libnats.so.3.13.0) so ffi.load("nats*")
# resolves correctly.
#
# When a SECOND app needs NATS, promote these libs from per-app vendoring
# here into luajit-base/prebuilt_libs/ instead.
COPY prebuilt_libs/libnats.so.3.13.0      /usr/local/lib/
COPY prebuilt_libs/libnats_key_store.so   /usr/local/lib/
COPY prebuilt_libs/libnats_job_queue.so   /usr/local/lib/
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
