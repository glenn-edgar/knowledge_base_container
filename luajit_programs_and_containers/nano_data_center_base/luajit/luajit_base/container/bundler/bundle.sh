#!/usr/bin/env bash
# bundle.sh -- wrapper invoked by app Dockerfiles via
#              /usr/local/bin/bundle_controller.
#
# Reads /opt/apps/*/app.manifest.json, writes /opt/luajit_base/controller.db.
set -euo pipefail
exec luajit /opt/luajit_base/bundler/bundler.lua "$@"
