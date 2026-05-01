# OpenResty Base Container — Continuation Plan

## Session 2 (2026-04-17) — stage 1 code written, awaiting smoke test

Design pivoted from the 2026-04-15 plan. That plan had openresty_base
`FROM openresty/openresty:alpine-fat` as an independent base, and
shipped a `resty_kb_client/` lua library for apps to call into. Every
app image would `FROM openresty-base` (no luajit-base underneath).

**New direction:** every container gets both luajit-base AND
openresty. Concretely, openresty_base `FROM nanodatacenter/luajit-base:latest`
and layers OpenResty + pgmoon + htmx on top. The chain-tree supervisor
from luajit-base remains PID 1; openresty runs as one of N supervised
child processes launched from an app's `app.manifest.json`.

### Why the pivot

- Apps often want both a chain-tree worker process AND an openresty
  web process in the same container (e.g., test_app's worker + web
  pair, dcs_ops_ui's background poller + web).
- Having two parallel bases forces apps to pick one and lose the
  other, or run two containers that share state over a socket.
- The luajit-base supervisor already does everything we'd need a
  lightweight init system for; openresty doesn't need to be PID 1.
- Image size is not a concern — the user confirmed at least one web
  server will always be present, so shipping openresty in every
  container is fine.

### What's in the image beyond luajit-base

- `/usr/local/openresty/` — OpenResty install (apt package from
  `openresty.org/package/ubuntu`). Includes its own embedded LuaJIT,
  isolated from the host-side luajit at `/usr/bin/luajit`.
- `/usr/local/openresty/site/lualib/pgmoon/` — pgmoon (pure-lua pg
  client for nginx workers), installed via `opm get leafo/pgmoon`.
- `/usr/local/openresty/site/assets/htmx/htmx.min.js` — htmx v1.9.12,
  vendored by `docker_build.sh` (curl from unpkg, cached in
  `container/prebuilt_web_assets/`).
- `/opt/openresty_base/conf.d/` — app nginx configs drop here
  (scaffolding; empty in the base).
- `/opt/openresty_base/logs/` — per-process pid/error/access dir.

### Files written (in `container/`)

- `Dockerfile` — 4-step add-layer on top of luajit-base.
- `docker_build.sh` — prereq check (luajit-base present), fetch htmx
  if absent, `docker build`.

### App author contract (new, relative to luajit-base)

An app image that wants a web process:
1. `FROM nanodatacenter/openresty-base:latest`.
2. Drop `main.lua` + `app.manifest.json` into
   `/opt/apps/<appname>/` like any luajit app.
3. Drop the app's `nginx.conf` into `/opt/apps/<appname>/conf/` and
   have the manifest's `argv` start openresty pointed at it:
   ```json
   {
     "name": "mysite_web",
     "argv": ["/usr/local/openresty/bin/openresty",
              "-p", "/opt/apps/mysite/",
              "-c", "conf/nginx.conf",
              "-g", "daemon off;"]
   }
   ```
4. `RUN /usr/local/bin/bundle_controller` to produce controller.db.

Openresty is NOT PID 1. Supervisor catches its exit via waitpid and
respawns per `restart_policy`. `daemon off;` is mandatory — without
it openresty forks+exits, and the supervisor sees an immediate exit
and enters a respawn loop.

### Smoke test (awaiting user run)

```bash
# From openresty_base/container/
./docker_build.sh
```

Expected:
- htmx fetch (~50 KB) caches to `prebuilt_web_assets/htmx/`.
- Docker build succeeds. Key step: `opm get leafo/pgmoon` resolves.
- Final size report.

No runtime test of the base alone — it has no bundled app, so
`docker run` would fail to open controller.db (same as luajit-base
with no app). The real runtime validation happens when test_app
(stage 2) is built and run.

### Deferred (retained from earlier design)

- `resty_kb_client/` library extraction. Once test_app + dcs_ops_ui
  both inline the same pgmoon patterns (connect, registry read, ltree
  path builders), pull the duplication out into a shared
  `/usr/local/openresty/site/lualib/resty_kb_client/` module and
  refactor callers. Skip until the duplication is visible.

## Next session

Stage 2 — `test_app/` with 4 shell processes (exception_aggregator,
log_aggregator, exception_web, log_web) to exercise the registration /
deregistration path end-to-end.
