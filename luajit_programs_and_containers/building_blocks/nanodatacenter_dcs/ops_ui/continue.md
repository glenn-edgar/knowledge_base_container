# dcs_ops_ui — Continuation Plan

## Status

Design-only. No code. Full architecture in
`../../nanodatacenter_gateway/continue.md` (single design doc for
gateway + ops_ui + openresty_base + DCS schema changes).

## What this is

The DCS microservice UI — a per-app web server container that serves
DCS-specific observability + control pages (exceptions, heartbeats,
sample streams, ack/clear writes). Consumed through the gateway's
reverse proxy. Registered by node_control (via
`container_registry`) just like any other UI-bearing app.

## First session plan

1. `Dockerfile` — `FROM nanodatacenter/openresty-base:latest`,
   COPY nginx.conf + static/ + lua/.
2. `nginx.conf` — `listen 8080;`, api endpoints:
   - `GET /api/exceptions` — SELECT from knowledge_base_status
     where label='SYS_EXCEPTION'
   - `GET /api/heartbeats` — read bit_mask_table per CPU, compute
     age
   - `GET /api/system_ready` — status field read
   - `POST /api/ack` — UPDATE status.data.acknowledged=true
3. `static/index.html` — htmx-driven single-page with exception
   cards + heartbeat grid.
4. `run_ops_ui.sh` — standalone dev harness (mirrors
   luajit_base/container/run_dummy.sh): sources secrets.env,
   --network=host, PG_HOST=127.0.0.1.

## Dependencies

- Blocked on openresty_base existing and built.
- Can be tested standalone (direct `http://127.0.0.1:8080/...`)
  without the gateway. Full end-to-end test requires gateway +
  DCS schema extension + node_control registration user functions.

## Non-goals for v1

- No NATS live push. Browser polls `/api/*` every 2s via htmx.
- No auth.
- No rpc queue writes (just ack/clear, which are direct status
  field mutations).

## Placement

In topology.lua as an instance on master CPU:
```lua
{ name = "dcs_ops", definition = "dcs_ops_ui",
  ports = { ui = 8080 } }
```

Owned by system_control (infra-tier), not node_control
(app-tier) — site-scoped, single instance, no per-CPU symmetry.
