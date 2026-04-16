# openresty_base — Continuation Plan

## Status

Design-only. No code. Full architecture in
`../nanodatacenter_gateway/continue.md` (single design doc for
gateway + ops_ui + openresty_base + DCS schema changes).

## What this is

Shared base Docker image for every openresty-based app UI in the
system. Ensures Docker layer sharing of `FROM
openresty/openresty:alpine-fat` + luarocks (`pgmoon`,
`lua-resty-openssl`) + the `resty_kb_client` Lua library + vendored
htmx across every descendant image.

Downstream images: `nanodatacenter/gateway`,
`nanodatacenter/dcs_ops_ui`, and the future ros_planner_ii web
gateway.

## First session plan

1. `Dockerfile` — apt-fat base, luarocks install, COPY
   `resty_kb_client/` → `/usr/local/openresty/lualib/resty_kb_client/`,
   COPY `htmx.min.js` → `/usr/local/openresty/nginx/html/vendor/`.
2. `docker_build.sh` — stages `resty_kb_client/` (from this dir or
   a sibling `building_blocks/resty_kb_client/`) + a vendored htmx
   tarball.
3. `resty_kb_client/` skeleton:
   - `pg.lua` — pgmoon wrapper with connection + keepalive + query
     + escape helpers. Reads PG_HOST/PORT/DB/USER/PASSWORD from env.
   - `ltree.lua` — path builders (`system.site.<S>.cpu.<id>....`).
   - Grow it (e.g. `registry.lua` for registry reads) as downstream
     consumers show up.
4. Smoke-test: `docker build`, `docker run` the image, exec into it
   and `luajit -e 'local pg = require"resty_kb_client.pg"; print(pg)'`
   — just confirm the library is importable.

## Non-goals for v1

- No resty NATS client — registry is pg-backed; NATS not needed here.
- No auth helpers.
- No htmx app logic — just ship the library file.
