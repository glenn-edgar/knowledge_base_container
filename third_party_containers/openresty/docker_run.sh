#!/bin/bash
# Shell directory to mount as static files
SHELL_DIR="$(cd "$(dirname "$0")/../../luajit_programs_and_containers/building_blocks/system_api/shell" && pwd)"

docker run --name openresty-gateway -d \
  -p 8080:8080 \
  -v "$SHELL_DIR:/srv/shell:ro" \
  -e PG_HOST=172.17.0.1 \
  -e PG_PORT=5432 \
  -e PG_DBNAME=knowledge_base \
  -e PG_USER=gedgar \
  -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-ready2go}" \
  --link pg-vector:pg-vector \
  nanodatacenter/openresty-gateway:latest
