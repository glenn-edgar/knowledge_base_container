# nanodatacenter/test-app -- four-process shell container for exercising
# the registration/deregistration path through node_control + gateway.
#
# Processes (supervised children of luajit-base's chain-tree controller):
#   1. exception_aggregator -- lua worker shell (prints ticks)
#   2. log_aggregator       -- lua worker shell (prints ticks)
#   3. exception_web        -- openresty, internal port 8080, one page
#   4. log_web              -- openresty, internal port 8081, one page
#
# None of the logic is real. The point is: does the supervisor spawn four
# heterogeneous processes cleanly, do the two web ports bind, and does
# node_control end-to-end instantiation + port attach + registry write work.

FROM nanodatacenter/openresty-base:latest

# Drop each app artifact at the convention location.
COPY exception_aggregator/ /opt/apps/exception_aggregator/
COPY log_aggregator/       /opt/apps/log_aggregator/
COPY exception_web/        /opt/apps/exception_web/
COPY log_web/              /opt/apps/log_web/

# Nginx's default error-log path (<prefix>/logs/error.log) is tried before
# the config override takes effect. Create the dirs to silence the noisy
# but non-fatal alert at startup.
RUN mkdir -p /opt/apps/exception_web/logs /opt/apps/log_web/logs

# Bundle the controller.db that luajit-base's supervisor will read at boot.
RUN /usr/local/bin/bundle_controller

# CMD inherited from luajit-base (supervisor/entrypoint.lua).
