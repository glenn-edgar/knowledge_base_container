# dcs_console Container — Continuation Plan

## Session 1 (2026-04-17) — stage 1: two blank web servers, registration exercise

## Purpose

Second-pass validation of the container_registry path. Same pattern as
`test_app` but with two openresty processes instead of
two-lua-workers-plus-two-web-pages. Both servers are shells; their
SLOT NAMES (`gateway`, `admin`) are the final names so that step 2
(fill in the reverse proxy) and step 3+ (fill in the real admin UI)
can replace content in place without churning CONTAINER_REGISTRY rows.

## Processes

1. `gateway` (`start_order: 10`) — openresty, internal port `8080`,
   external `19003`. Shell. **Eventually:** site-wide reverse proxy
   that polls CONTAINER_REGISTRY and proxies
   `/ui/<container>/<slot>/` to the registered upstream.
2. `admin` (`start_order: 20`) — openresty, internal port `8081`,
   external `19004`. Shell. **Eventually:** real operator admin UI
   (pgmoon reads of knowledge_base + knowledge_base_status).

Each shell serves a static stub page at `/` and JSON metadata at
`/__info__`. pid + temp paths pinned under `/tmp` (same as test_app
convention).

## Build graph

```
luajit-base:latest  ←  openresty-base:latest  ←  dcs-console:latest
```

## DSL entries

Definition (in `nanodatacenter_dcs/provisioning/laptop/definitions.lua`):

```lua
dcs_console = {
  kind    = "application",
  runtime = "docker",
  image   = "nanodatacenter/dcs-console:latest",
  restart_policy = "unless-stopped",
  port_spec = {
    gateway = { internal = 8080, protocol = "tcp", purpose = "ui",
                description = "Site-wide reverse proxy (shell)" },
    admin   = { internal = 8081, protocol = "tcp", purpose = "ui",
                description = "DCS operator admin UI (shell)" },
  },
},
```

Placement (in `topology.lua`, cpu_01.instances):

```lua
{ name = "dcs_console_01", def = "dcs_console",
  ports = { gateway = 19003, admin = 19004 } },
```

## Smoke test (awaiting user run)

```bash
# 1. Build the container image.
cd building_blocks/dcs_console/container
./docker_build.sh

# 2. Re-run DSL construct + slice so DCS picks it up.
cd ../../nanodatacenter_dcs/provisioning/laptop
./build_kb.sh
./slice_bootstrap.sh

# 3. Restart DCS (ctrl-C the watchdog if running, clean stale, start fresh).
docker rm -f test_app_01 dcs_console_01 2>/dev/null
rm -f ../../build_output/cpu_01/error.log
cd ../../build_output/cpu_01 && ./start.sh
```

Expected log lines:
```
NODE_READ_OWN_CONFIG: 2 assignment(s) [test_app_01, dcs_console_01]
START test_app_01 -> <id>
REGISTER test_app_01
START dcs_console_01 -> <id>
REGISTER dcs_console_01
```

Expected curls:
- `curl http://localhost:19001` → test_app exception_web page
- `curl http://localhost:19002` → test_app log_web page
- `curl http://localhost:19003` → dcs_console gateway shell page
- `curl http://localhost:19004` → dcs_console admin shell page

Expected registry: two rows under
`system.site.moonbase.alpha.dcs.cpu.cpu_01.CONTAINER_REGISTRY.{test_app_01, dcs_console_01}`,
each with the full ports record list.

## Session 1 — stage 1 VERIFIED (2026-04-17 end of day)

End-to-end validation with DCS: `./build_kb.sh` + `./slice_bootstrap.sh`
+ restart DCS → node_control loaded 2 assignments `[test_app_01,
dcs_console_01]`, STARTed both, REGISTERed both. Four external ports
(19001/19002 for test_app, 19003/19004 for dcs_console) all serve
their expected shell pages. Registry has both rows with full
port_spec records.

Nothing to patch in the container itself. Move to step 2 next
session.

## Next session (tomorrow, 2026-04-18) — step 2: fill the gateway slot

User intent: "construct the reverse proxy and get it out of the way."
Same container, same slot name (`gateway`), same external port
(`19003`). Only `gateway/conf/nginx.conf` contents change; everything
else stays. Registry row auto-updates on next REGISTER (spec pulled
fresh from the def anyway).

User explicit: design session FIRST, then code. Do NOT start writing
nginx.conf until these are locked in conversation:

### Design questions to work through before coding

1. **Polling cadence.** Gateway polls pg `CONTAINER_REGISTRY` to build
   the route table. Default ~2s from earlier design doc. Too fast for
   a tiny site; too slow for a fleet. What do we actually want?
2. **URL scheme.** `/ui/<container>/<slot>/` → upstream. What does the
   gateway's own `/` show? A landing page that lists all routable
   UIs? A redirect to the admin slot? Pick.
3. **Upstream addressing.** Two options:
   - `host.docker.internal:<external>` -- uses published ports, works
     regardless of whether gateway shares a network with the target.
   - `<container_name>:<internal>` -- uses docker DNS, requires shared
     network, bypasses the port-publish hop.
   For v1, `host.docker.internal:<external>` is simpler and matches
   how every other DCS container already talks to pg. Leaning that
   way but open.
4. **Path rewriting.** Strip `/ui/<container>/<slot>` prefix before
   forwarding so apps don't need to know they're behind a proxy, OR
   pass full path and apps handle the prefix. Prefix-stripping is
   the common htmx-friendly choice (hx-push-url and relative URLs
   work naturally). Confirm.
5. **Cache layer.** `lua_shared_dict ui_registry` (e.g., 1 MB) updated
   by a timer-driven pgmoon poll. Per-request lookup is O(1) dict
   hit. Fallback on cold cache: serve 503 or block on first fetch?
6. **Error modes.**
   - Unknown `<container>/<slot>` → 404 with listing of known routes?
   - pg unreachable → serve last-known-good cache + log once per
     minute? Or hard-fail?
   - Upstream unreachable → 502 with gateway-branded error page?
7. **Trailing slashes.** `/ui/foo/bar` vs `/ui/foo/bar/`? Redirect
   missing-trailing-slash to with-trailing-slash (common nginx
   pattern). Confirm.
8. **WebSocket proxying.** Out of scope for v1 (the only WS consumer
   right now is mosquitto on its own port). Defer to step 3+.
9. **Gateway self-registration.** Gateway is in the registry (slot=
   `gateway`, purpose=`ui`). Should `/ui/dcs_console/gateway/` route
   back to ourselves (loop risk)? Solutions: (a) skip any slot named
   `gateway` at routing time, (b) skip the container that hosts the
   gateway process, (c) detect self-proxy at request time and 404.
   Pick.
10. **Auth / TLS.** Still nothing for v1 (trusted LAN). Confirm.
11. **Poll task lifecycle.** `init_worker_by_lua_block` spawns a
    single ngx.timer.at that re-arms itself. Need to guard against
    multiple worker_processes triggering multiple pollers (set a
    shared_dict flag; only worker 0 runs the poll task).

### Shape of what we'll write (rough)

- `gateway/conf/nginx.conf`:
  - `lua_shared_dict ui_registry 1m;`
  - `init_worker_by_lua_file` → starts the pg-poll timer.
  - server block with `/ui/<container>/<slot>/.*` regex location →
    lua `content_by_lua_block` that looks up in shared_dict, sets
    `ngx.var.upstream`, then `proxy_pass http://$upstream/...`.
  - Fallback `/` location returning a landing page (htmx).
- `gateway/lua/poller.lua`: opens pgmoon, SELECTs every
  CONTAINER_REGISTRY row, writes the routing map into shared_dict.
- Maybe `gateway/lua/route.lua`: helper for the content block.

Not touching supervisor/app.manifest.json at all -- the supervisor
just runs openresty with its conf; everything lives in the conf +
lua files dropped into `gateway/`.

### Rough edges that might bite

- The `CONTAINER_REGISTRY` rows right now don't include a `site` or
  `cpu_id` in a way the gateway can use to decide which CPU's copy
  of an app to route to in multi-CPU deployments. For single-CPU
  v1, every row is reachable; for multi-CPU we'd want to prefer the
  local CPU's copy. That's a step 3+ concern.
- pgmoon's scram-sha-256 auth dep (lua-resty-openssl) is still not
  vendored into openresty-base. Postgres 14+ default is scram; if
  our dev pg is using md5 auth, we're fine; if scram, need openssl.
  Check `SHOW password_encryption;` on the dev pg before writing
  the poller.

## Session 2 (2026-04-18) — stage 2: gateway filled in, VERIFIED

All 7 design questions from session 1 locked in conversation, plus an
8th on WebSocket support (folded into v1). Gateway built, smoke test
green.

### Design decisions locked

| # | Decision |
|---|----------|
| 1 | Poll cadence 15s, KB-configurable at `system.site.<SITE>.KB_STATUS_FIELD.gateway_poll_interval_sec`. Poller reads it each tick; COALESCE to schema default then to 15. |
| 2 | Gateway `/` serves htmx-ready listing page of every registry row with `purpose=ui`. |
| 3 | Upstream = `<ip>:<external>`. Poller resolves `host.docker.internal` from `/etc/hosts` at startup and stores the literal IP in the route table; no nginx `resolver` directive needed (Docker Desktop's 127.0.0.11 is unreliable on default bridge). |
| 4 | Strip prefix — upstream sees path after `/ui/<container>/<slot>`. |
| 5a | Cold start → 503 until first successful poll. |
| 5b | Any pg error → emit SYS_EXCEPTION (via SIGTERM to nginx master → supervisor sees process die) + exit non-zero. No stale-cache mode. |
| 6a | Unknown route → 404 with landing-page body (self-recovering). |
| 6b | Upstream unreachable → plain 502. |
| 7 | Poller skips rows where `slot == "gateway"` at build time. Loop impossible. |
| 8 | WebSocket proxy supported in v1: `map $http_upgrade $connection_upgrade`, `proxy_set_header Upgrade/Connection`, `proxy_read_timeout 1h`. Same `/ui/<c>/<s>/ws/...` URL. |

### Files

- `container/gateway/conf/nginx.conf` — lua_shared_dict + init_worker
  poller + landing location + 301 trailing-slash + `/ui/` regex with
  access_by_lua + WS headers + 502 fallback + `env` directives for
  PG_*/APP_*/CONTAINER_NAME (lesson below).
- `container/gateway/lua/poller.lua` — resolves host.docker.internal
  via /etc/hosts → pgmoon connect with IP → reads
  gateway_poll_interval_sec + CONTAINER_REGISTRY → writes JSON blob
  to shared_dict. On any pg error: SIGTERM master via pidfile.
- `container/gateway/lua/route.lua` — access phase, 503/404 short-
  circuits, sets `$gw_upstream` for proxy_pass.
- `container/gateway/lua/landing.lua` — renders listing (used at /
  status 200 and as 404 body).

### Shared-infra changes

- `openresty_base/container/docker_build.sh` — vendor
  lua-resty-openssl 1.7.1 (pg17 uses scram; pgmoon's scram path needs
  resty.openssl.{hmac,digest,kdf,rand}). Tarball drop into
  `prebuilt_openresty_lualib/resty/`. No Dockerfile change.
- `nanodatacenter_dcs/provisioning/laptop/construct_dcs_kb.lua` —
  site-level `gateway_poll_interval_sec` status field (default 15).

### Smoke test — all green

- `curl /` → landing page with 3 rows (test_app_01/{exceptions_ui,
  logs_ui}, dcs_console_01/admin). Gateway slot correctly excluded.
- `curl /ui/test_app_01/exceptions_ui/` → proxies to test_app's
  exception_web page.
- `curl -I /ui/test_app_01/exceptions_ui` (no trailing slash) →
  301 redirect.
- `curl /ui/bogus/bogus/` → 404 with landing-page body.
- `docker logs` shows `gateway poll ok; next in 15s` every 15s.

### Lessons (3-iteration rebuild cost)

1. `$upstream_addr` is a nginx built-in (proxy module). Can't `set`
   it. Renamed to `$gw_upstream`.
2. Docker Desktop's 127.0.0.11 embedded DNS is refused from default-
   bridge containers. openresty cosockets don't read /etc/hosts.
   Resolved host.docker.internal in lua from /etc/hosts and stored
   the IP literally in both pgmoon config + route table.
3. **nginx strips env vars from workers by default.** `os.getenv("X")`
   returns nil in init_worker unless `env X;` is declared at the main
   (top-level) context of nginx.conf -- above events{} and http{}.
   Saved to memory as feedback_nginx_env_directive.md.

### Session 2 addendum (same day) — tab workspace at /

The plain table at `/` works but makes the gateway feel "single-page-
forward": clicking a route navigates the whole browser away from the
gateway. Replaced with a multi-tab workspace modelled on a browser's
tab strip.

**Layout.** Sidebar catalog on the left (registered UIs, one row per
`purpose=ui` registry entry). Workspace on the right = tab strip +
stacked iframes. Each iframe stays mounted even when inactive so the
app's internal state and scroll position survive tab switches.

**Interaction.**
- Click a sidebar item → open as new tab; if that route is already
  open, focus the existing tab (no duplicates, confirmed with user).
- Each tab has a `×` close button.
- Sidebar highlights routes that are currently open.
- Tabs + active index persist in `localStorage`
  (`dcs_gateway_tabs_v1`), so refresh restores the workspace.

**Implementation.** Pure frontend. Added `M.render_tabs()` to
`landing.lua` alongside the existing `M.render(status)`. nginx.conf's
`location = /` now dispatches to `render_tabs()`; the 404 path in
`route.lua` still uses the plain table so a bad direct URL doesn't
render the full workspace. No backend logic changed; sidebar is
server-rendered from the same shared_dict menu the plain table used.

**Verified in browser:** three sidebar rows show, click opens tab,
click-again focuses, `×` closes, refresh restores. No backend
restart required for UI-only iteration.

### Next session — step 3: fill the admin slot

Real operator UI backed by pgmoon reads of `knowledge_base` +
`knowledge_base_status`. Per earlier design: registered containers,
active SYS_EXCEPTIONs, per-CPU heartbeat freshness, site-wide
system_ready flag. Same container, same slot name (`admin`), same
external port (`19004`); only `admin/conf/nginx.conf` + lua files
change. Registry row auto-updates.

Existing plumbing to reuse:
- pgmoon + lua-resty-openssl vendored in openresty_base.
- `env PG_*; env APP_*;` pattern for nginx.conf.
- host.docker.internal → IP resolution from /etc/hosts.
- Access pattern documented in `nanodatacenter_dcs/ops_ui/continue.md`.
