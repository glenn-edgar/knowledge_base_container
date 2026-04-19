# nanodatacenter/observability -- SCADA-style observability hub.
# Singleton container on master CPU. Four supervised processes:
#
#   1. exception_analyzer  -- SYS_EXCEPTION lifecycle janitor
#                             (ack-lease expiry, flap detection).
#   2. log_analyzer        -- KB_LOG sample ingestion, live_stats,
#                             rule evaluation, rollup compaction.
#   3. exception_web       -- openresty, internal :8080, external :19007.
#                             Alarm ops UI (active, detail, shelve, journal).
#   4. log_web             -- openresty, internal :8081, external :19008.
#                             Strip charts + rule inventory.
#
# Phase 5 (this build) -- SHELLS. Analyzers print heartbeats; web servers
# serve stub pages. Real logic lands in Phases 6-8.

FROM nanodatacenter/openresty-base:latest

COPY exception_analyzer/ /opt/apps/exception_analyzer/
COPY log_analyzer/       /opt/apps/log_analyzer/
COPY exception_web/      /opt/apps/exception_web/
COPY log_web/            /opt/apps/log_web/

# Nginx tries <prefix>/logs/error.log before the config override kicks in;
# create the dirs to silence the cosmetic startup alert.
RUN mkdir -p /opt/apps/exception_web/logs /opt/apps/log_web/logs

# Bundle the controller.db that luajit-base's supervisor will read at boot.
RUN /usr/local/bin/bundle_controller

# CMD inherited from luajit-base (supervisor/entrypoint.lua).
