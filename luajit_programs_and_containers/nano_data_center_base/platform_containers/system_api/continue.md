# System API - Continue Notes

## Status: Working prototype (2026-04-04)

## What Was Built Today

### Infrastructure
- **NATS WebSocket** — added ws port 9222 to nats-js-ram container (confirmed working)
- **OpenResty container** — nanodatacenter/openresty-gateway:latest on :8080
  - alpine-fat image + pgmoon + lua-resty-openssl
  - Serves static shell files from volume mount
  - HTTP API endpoints query Postgres directly (non-blocking cosocket)
  - PG_HOST=172.17.0.1 (Docker bridge to host Postgres)
- **KB Sidecar** — standalone LuaJIT NATS RPC server (can be killed, UI no longer needs it)
  - Located at system_api/kb_sidecar/
  - Uses corrected FFI binding (nats_rpc_server.lua) for libnats_rpc.so
  - Still useful for non-HTTP clients querying KB over NATS

### KB DSL Build System
- **kb_dsl/** directory with parameterized site_config.lua
- **site_config.lua** — declarative: CPUs → containers → services, domains, robots
  - Physical model: CPU has containers, containers have services
  - Each CPU has master leaf, master holds Postgres
  - SQLite is container data, not a service
  - Currently 1 CPU (cpu_01), all containers on it
  - 4 domains: surface_ops, fleet, telemetry, warehouse_ops
- **master_build.lua** — rebuilds Postgres from scratch (drops/recreates tree tables, preserves data tables)
  - Just run it, no manual cleanup needed
  - Produces 2 KBs: system (physical) + subsystems (logical)
- **SQLite extraction** — per-domain .db files in sqlite_dbs/
  - Same ltree namespace as Postgres
  - Includes system infrastructure + domain subtree
- **construct_surface_ops_merged.lua** — merged SQLite with both system infra + full domain content
  - Namespace: moonbase.alpha.services.robot_control.surface_ops
  - Includes status fields, stream fields, bitmasks, virtual nodes
  - Active planner DB (hub_dsl/kb_construct/surface_ops.db) NOT touched

### Federated Web Shell
- **Nav strip** — EDC brand, Robot Controllers tab with domain dropdown, KB Console tab, NATS status
- **Domain dropdown** — populated from HTTP /api/domains (Postgres via OpenResty)
- **Iframe isolation** — each tab is its own HTML page
- **Domains load via HTTP fetch** (instant), NATS connects lazily in background

### HTTP API Endpoints (OpenResty → Postgres)
- `/api/domains` — list all domains (tab discovery)
- `/api/containers?cpu=cpu_01` — list containers on a CPU
- `/api/robots?domain=surface_ops` — list robots for a domain
- `/api/services?container_path=...` — list services for a container
- `/api/node?kb=system&path=...` — get single node by path
- `/api/subtree?kb=system&path=...` — get all nodes under a path

### Surface Ops Panel (working prototype)
1. **Robot status cards** — click to select, shows status/energy/location/worker/transport
2. **Board viewer** — SVG landing zone graph (8 nodes, 9 edges), robot markers, click-to-select nodes
3. **Mission launcher** — select robot + from/to (dropdown or click board), Launch/Stop/Park/Recharge
4. **Planner status bar** — shows active state/robot/mission
5. All currently mocked with setTimeout — real planner will connect via NATS

## Running Services
```
Container          Port   Purpose
pg-vector          5432   Postgres 17 + pgvector (master KB)
nats-js-ram        4222   NATS JetStream
                   9222   NATS WebSocket
openresty-gateway  8080   HTTP gateway + static files + API
```

## File Layout
```
system_api/
  continue.md              ← this file
  shell/
    index.html             shell frame (nav strip + iframe)
    shell.js               (unused now, was NATS-based shell)
    shell.css              (unused now, styles in index.html)
    vendor/
      nats.js              nats.ws ESM bundle (377KB)
      alpine.min.js        Alpine.js (45KB, not used yet)
    tabs/
      surface_ops.html     robot cards + board viewer + mission launcher
      warehouse_ops.html   splash with 3 robots (forklift x2, sorter)
      fleet.html           placeholder
      telemetry.html       placeholder
      kb_console.html      HTTP-based query tool (was NATS, now fetch)
  kb_sidecar/
    main.lua               NATS RPC server entry point
    handlers.lua           KB query handlers
    nats_rpc_server.lua    corrected FFI binding for libnats_rpc.so
    run.sh                 launch script (sets LD_LIBRARY_PATH)

kb_dsl/
  README.lua
  scripts/
    site_config.lua                  declarative site definition
    master_build.lua                 entry point (Postgres rebuild)
    physical_tree.lua                CPU → container → service builder
    software_tree.lua                domain → robot builder
    sqlite_extract.lua               per-domain SQLite extraction
    containers_tree.lua              (obsolete, kept for reference)
    bindings.lua                     (obsolete, merged into physical tree)
    construct_surface_ops_merged.lua standalone merged SQLite builder
  sqlite_dbs/
    surface_ops.db                   system infra extract
    warehouse_ops.db
    fleet.db
    telemetry.db
    surface_ops_merged.db            full merged (system + domain content)
  templates/                         (empty, for later)

third_party_containers/openresty/
  Dockerfile                         alpine-fat + pgmoon + resty-openssl
  nginx.conf                         static files + 6 API endpoints
  docker_build.sh
  docker_run.sh                      mounts shell dir, links pg-vector

third_party_containers/nats/
  nats.conf                          now includes websocket block (:9222)
  Dockerfile
  docker_build.sh
  docker_run.sh                      publishes 4222 + 8222 + 9222
```

## Next Steps (morning of 2026-04-05)

### Surface Ops Panel — Wire to Real Planner
1. Replace mock robot state with NATS KeyStore watches (live status)
2. Mission launch → submit to NATS JobQueue (same as planner uses)
3. Board view updates in real-time as robots move
4. Bitmask display (init_check, path_spline bits)
5. Telemetry stream viewer (heartbeat history)

### Architecture Refinements
- Move board data into Postgres (via site_config or domain DSL)
- Merge the domain DSL (construct_surface_ops) into the master build pipeline
- Consider killing the KB sidecar permanently (OpenResty handles all queries)
- Add error handling to OpenResty API endpoints (SQL injection protection)

### New Features
- Mission history (Postgres stream table for bookend events)
- Multi-step missions (route planner finds path through board graph)
- Energy budget display (predicted vs actual)
- Dashboard tab (system-wide health across all domains)

## Key Design Decisions
1. OpenResty queries Postgres directly (pgmoon cosocket) — no sidecar needed for HTTP
2. NATS WebSocket for live push only (robot status, telemetry) — not for page load
3. HTTP fetch for initial data load (fast, no NATS dependency)
4. Board data currently embedded in HTML — needs to move to API
5. Namespace: moonbase.alpha.services.robot_control.{domain}
6. Physical tree: site → CPU → container → service (containers attached to CPUs)
7. SQLite is local to the container (in data field), not a service
8. master_build.lua is idempotent — drops/recreates tree tables, preserves data tables
