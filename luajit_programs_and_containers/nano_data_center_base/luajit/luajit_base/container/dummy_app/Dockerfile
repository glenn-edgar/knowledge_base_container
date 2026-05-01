# Smoke-test image: extends luajit-base with a single hello-world app.
#
# Build:
#   docker build -f dummy_app/Dockerfile -t nanodatacenter/luajit-dummy:latest .
# Run (set at minimum CONTAINER_NAME + pg envs, or run without pg and watch
# ctrl state machine trip ERR_PG_UNREACHABLE cleanly):
#   docker run --rm \
#       -e CONTAINER_NAME=dummy_test -e APP_SITE=dev -e APP_CPU_ID=cpu_01 \
#       -e PG_HOST=... -e PG_PORT=5432 -e PG_DB=... -e PG_USER=... \
#       -e PG_PASSWORD=... \
#       nanodatacenter/luajit-dummy:latest

FROM nanodatacenter/luajit-base:latest

# Drop the app artifact at the conventional location and let the bundler
# write /opt/luajit_base/controller.db from it.
COPY dummy_app/ /opt/apps/dummy/
RUN /usr/local/bin/bundle_controller

# CMD is inherited from the base image.
