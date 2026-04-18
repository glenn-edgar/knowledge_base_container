# nanodatacenter/dcs-console -- two-web-server container. Both start as
# shells serving a stub page; content gets filled in incrementally:
#   gateway (internal :8080, external :19003) -- stub now, reverse proxy later
#   admin   (internal :8081, external :19004) -- stub now, real pg-backed ops UI later
#
# Both processes are supervised by the luajit-base chain-tree controller;
# neither is PID 1.

FROM nanodatacenter/openresty-base:latest

COPY gateway/ /opt/apps/gateway/
COPY admin/   /opt/apps/admin/

# nginx default-path error_log lookup happens before the config override
# kicks in; create logs/ under each prefix to silence the cosmetic alert.
RUN mkdir -p /opt/apps/gateway/logs /opt/apps/admin/logs

RUN /usr/local/bin/bundle_controller

# CMD inherited from luajit-base (supervisor/entrypoint.lua).
