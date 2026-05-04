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

COPY planner/    /opt/apps/planner/
COPY planner_ui/ /opt/apps/planner_ui/

# nginx default error-log path is tried before the config override; create
# logs/ to silence the cosmetic alert at startup.
RUN mkdir -p /opt/apps/planner_ui/logs

RUN /usr/local/bin/bundle_controller

# CMD inherited from luajit-base (supervisor/entrypoint.lua).
