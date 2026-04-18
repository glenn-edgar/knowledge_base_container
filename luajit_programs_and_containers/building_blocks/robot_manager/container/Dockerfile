# nanodatacenter/robot-manager -- two-process shell container.
#
# Processes (supervised children of luajit-base's chain-tree controller):
#   1. manager    -- lua worker shell (heartbeats only; real fleet mgr later)
#   2. manager_ui -- openresty, internal port 8080, one page
#
# Populated later. For now this exists so the gateway tab UI has a third
# non-dcs-console application available for routing tests.

FROM nanodatacenter/openresty-base:latest

COPY manager/    /opt/apps/manager/
COPY manager_ui/ /opt/apps/manager_ui/

RUN mkdir -p /opt/apps/manager_ui/logs

RUN /usr/local/bin/bundle_controller

# CMD inherited from luajit-base (supervisor/entrypoint.lua).
