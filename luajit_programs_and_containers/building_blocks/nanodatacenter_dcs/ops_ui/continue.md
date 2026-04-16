# dcs_ops_ui — Continuation Plan

## Status

Design-only, no code yet. **This is build step 1 of 5** in the
revised order — see
`../../nanodatacenter_gateway/continue.md` for full context and
the 5-step sequencing rationale.

## What this is

The DCS microservice UI — a per-app openresty container that
serves DCS-specific observability + control pages (exceptions,
heartbeats, sample streams, ack/clear writes). Doubles as the
first real app-container DCS manages end-to-end, so step 1 is
"build the web server standalone" and steps 2-3 teach DCS to run
it.

## Tomorrow's session (step 1) — concrete plan

### Reuse the system_api/shell frontend template

`building_blocks/system_api/` has a working operator shell from
2026-04-04:
- `system_api/shell/index.html` — nav strip + tree sidebar +
  iframe tabs. Dark-mode monospace aesthetic. **Includes a
  vanilla-JS tree widget** (the jQuery-no-tree-control gap
  already solved here).
- `system_api/shell/tabs/*.html` — one file per tab.
- `system_api/shell/vendor/{alpine.min.js, nats.js, nats.min.js}`
  — already-vendored JS deps.
- `third_party_containers/openresty/nginx.conf` — 6 pg-backed
  API endpoints using pgmoon + cosocket. The pattern to mirror.

**Don't copy verbatim** — system_api/shell is ros_planner-
oriented. Do borrow:
- `index.html` nav + tree + iframe structure + CSS.
- The nginx.conf inline-pgmoon pattern.
- The vendor/ layout convention.
- The `run_ops_ui.sh` shape from `luajit_base/container/run_dummy.sh`.

### Files to create

```
building_blocks/nanodatacenter_dcs/ops_ui/
├── Dockerfile                FROM openresty/openresty:alpine-fat
│                              + luarocks install pgmoon + lua-resty-openssl
│                              + COPY nginx.conf + shell/
├── docker_build.sh           builds nanodatacenter/dcs-ops-ui:latest
├── run_ops_ui.sh             standalone dev harness
├── nginx.conf                listen 8080; static from /shell/; DCS API
└── shell/
    ├── index.html            cloned from system_api/shell/index.html,
    │                          trimmed: keep nav+tree+iframe, swap tabs
    ├── vendor/               copy system_api/shell/vendor/* as-is
    └── tabs/
        ├── exceptions.html   cards for each SYS_EXCEPTION row,
        │                      ack button does POST /api/dcs/ack
        ├── heartbeats.html   grid of CPU cards w/ heartbeat age
        ├── samples.html      knowledge_base_stream viewer
        │                      (basic: last 60 samples of selected path)
        └── system.html       system_ready status + dashboard summary
```

### DCS API endpoints (nginx.conf content_by_lua_block)

- `GET /api/dcs/exceptions` — SELECT path, data from
  knowledge_base_status joined with knowledge_base WHERE
  label='SYS_EXCEPTION' AND status=false.
- `GET /api/dcs/heartbeats` — SELECT from knowledge_base_bit_mask
  WHERE label='heartbeat'; compute `age_s = now() - (bit_mask / 1e9)`
  client-side or server-side.
- `GET /api/dcs/system_ready` — SELECT from knowledge_base_status
  at `system.site.<S>.KB_STATUS_FIELD.system_ready`.
- `GET /api/dcs/samples?path=&limit=` — SELECT from
  knowledge_base_stream WHERE path=... ORDER BY recorded_at DESC
  LIMIT N.
- `POST /api/dcs/ack` — body `{path}`: UPDATE knowledge_base_status
  SET data=jsonb_set(data, '{acknowledged}', 'true') WHERE path=...

### Env contract (at docker run)

Same as system_api's openresty-gateway:
- `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD`.
- No `CONTAINER_NAME` etc. — step 1 doesn't register into anything.

### Test plan for step 1

1. `docker_build.sh` → `nanodatacenter/dcs-ops-ui:latest`.
2. `./run_ops_ui.sh` — starts container, `--network=host`,
   PG_HOST=127.0.0.1.
3. `curl http://127.0.0.1:8080/api/dcs/exceptions` — returns JSON
   array of current exceptions (may be empty if none logged).
4. `curl http://127.0.0.1:8080/api/dcs/heartbeats` — returns per-CPU
   heartbeat ages.
5. Visit `http://127.0.0.1:8080/` in a browser — see nav + tree +
   panes with live data.
6. Exercise: trigger a SYS_EXCEPTION (maybe bounce a broker container
   that DCS is watching); UI should show it; click ack; row in pg
   flips `acknowledged=true`.

## Dependencies for later steps (NOT tomorrow)

- **Step 2 (schema)**: `container_registry` anchor + port_spec in
  definitions + ports in topology (see DCS continue.md Session 9).
- **Step 3 (node_control)**: RECONCILE / CHECK_PORT_CONFLICT /
  REGISTER / DEREGISTER user functions; wire into
  START/STOP_ASSIGNED_CONTAINERS.
- **Step 4 (openresty_base retrofit)**: extract inlined pgmoon
  into `resty_kb_client/pg.lua`; refactor Dockerfile to
  `FROM openresty-base`.
- **Step 5 (gateway)**: portal + reverse-proxy sends browser to
  dcs_ops_ui via `/ui/dcs_ops/...`. Ops_ui's HTML needs to use
  relative URLs (or root-relative that survive prefix stripping).

## Non-goals (all sessions)

- NATS live push — htmx polls every 2s. NATS websocket is a later
  nice-to-have.
- Auth / TLS — trusted LAN v1.
- RPC queue writes (ack is a direct status-field mutation, not an
  RPC).

## Placement (when step 3 lands)

```lua
-- topology.lua
{ name="dcs_ops", definition="dcs_ops_ui", ports={ui=8080} }
```

On master CPU. Owned by system_control (infra-tier) — site-scoped,
single instance. node_control-on-master can also own it; pick when
we get to step 3.
