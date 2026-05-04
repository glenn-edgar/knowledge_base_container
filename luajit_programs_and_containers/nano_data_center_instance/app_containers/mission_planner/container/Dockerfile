# nanodatacenter/ros-mission-planner-ii -- two-process shell container.
#
# Processes (supervised children of luajit-base's chain-tree controller):
#   1. planner    -- lua worker shell (heartbeats only; real planner later)
#   2. planner_ui -- openresty, internal port 8080, one page
#
# Populated later. For now this exists so the gateway tab UI, the
# container registry path, and node_control's instantiation have a second
# non-test_app application to exercise.

FROM nanodatacenter/openresty-base:latest

COPY planner/    /opt/apps/planner/
COPY planner_ui/ /opt/apps/planner_ui/

# nginx default error-log path is tried before the config override; create
# logs/ to silence the cosmetic alert at startup.
RUN mkdir -p /opt/apps/planner_ui/logs

RUN /usr/local/bin/bundle_controller

# CMD inherited from luajit-base (supervisor/entrypoint.lua).
