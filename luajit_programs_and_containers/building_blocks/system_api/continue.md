# System API - Design Notes

## Status: Design phase (2026-04-03)

## What This Is

A multi-microservice web API that fronts multiple robot planner domains
(moonbase.alpha.*) and other subsystems. Thin HTTP gateway that translates
requests into NATS messages and Postgres KB queries.

## UI Architecture

### Federated Micro Frontends
- Top navigation bar with tabbed interface
- Each subsystem (surface_ops, subsurface, fleet, telemetry, etc.) owns
  its own UI panel and backend API routes
- Shell is thin: just nav bar + tab container that loads active subsystem
- Backend routes namespaced per subsystem: /api/surface_ops/*, /api/fleet/*
- Each subsystem panel loaded via iframe for full isolation
- Subsystem list discovered from Postgres at page load (/api/domains)

### Real-Time: NATS WebSocket
- Browser connects directly to NATS via WebSocket (nats.ws JS library)
- Subscribes to KeyStore watches for live robot status
- Same subjects action servers publish to (moonbase.alpha.*.action_server.*.status)
- HTTP path for commands/queries/history, NATS WS for live push

```
Browser
  +-- HTTP  -> OpenResty/nginx (commands, history, Postgres queries)
  +-- NATS WS:9222 -> NATS JetStream (live status, KeyStore watches)
```

## Web Framework: OpenResty + nginx

### Decision: OpenResty (nginx + LuaJIT)
- nginx handles static file serving, TLS, reverse proxy natively
- Lua scripting runs in LuaJIT (same as existing planner code)
- Each subsystem is a nginx location block -- federation for free
- Containerizes naturally (openresty/openresty image)
- Room for growth: rate limiting, caching, load balancing, auth

### Frontend Stack: Alpine.js + Shoelace + nats.ws
- No Node, no npm, no bundler -- vendored static files served by nginx
- **Alpine.js** -- reactivity, data binding, tab state, NATS data binding
- **Shoelace** (web components) -- UI component library, framework-agnostic
  - sl-tree: virtual node tree control with multi-select, lazy loading
  - sl-dialog: modal popup dialogs (node selection, mission config)
  - sl-tab-group: tab navigation for shell and subsystem panels
  - sl-select: multi-select dropdowns
  - sl-badge, sl-alert: status indicators
- **nats.ws** -- NATS WebSocket client for live status subscriptions
- All loaded from vendored files, no CDN dependency in production:
  ```
  shell/vendor/alpine.min.js
  shell/vendor/shoelace/       (themes + autoloader)
  shell/vendor/nats.min.js
  ```
- Each subsystem delivers own self-contained HTML/JS/CSS bundle
  using the same shared vendor libs
- iframe per tab for subsystem isolation

### Service Architecture
OpenResty never touches Postgres directly. All Postgres access goes through
a KB query sidecar over NATS request/reply. OpenResty is purely:
- Static file serving
- NATS message passing (request/reply to sidecars and action servers)
- JSON in, JSON out

```
Browser -> OpenResty (thin, non-blocking)
              |
              NATS
              |
     +--------+--------+--------+
     |        |        |        |
  KB query  action   action   (future
  sidecar   server   server   sidecars)
  (DBI)     (surf)   (sub)
```

### KB Query Sidecar
- Standalone LuaJIT process using existing kb_data_structures.lua (DBI/libpq)
- Listens on NATS request/reply subjects:
  - system_api.kb.query.domains       -> domain list (discovery)
  - system_api.kb.query.robots        -> robot registry
  - system_api.kb.query.history       -> mission history (stream reads)
  - system_api.kb.query.capabilities  -> robot capabilities
- No new Postgres code -- reuses existing facade unchanged
- Avoids blocking OpenResty's non-blocking event loop

### Directory Layout
```
system_api/
  nginx.conf                -- shell routing, static files, location blocks
  shell/
    index.html              -- nav bar + tab container
    shell.js                -- tab switching, domain discovery, nats.ws
    shell.css
  subsystems/
    surface_ops/            -- own HTML/JS/CSS + own Lua API handlers
      static/
      api/
    fleet/
      static/
      api/
    telemetry/
      static/
      api/
  api/
    discovery.lua           -- /api/domains -> NATS req to KB sidecar
    mission.lua             -- /api/{domain}/missions -> NATS JobQueue
    history.lua             -- /api/{domain}/history -> NATS req to KB sidecar
  lib/
    nats_client.lua         -- NATS request/reply helper for OpenResty
    json_util.lua           -- JSON encode/decode
  kb_sidecar/
    main.lua                -- KB query sidecar entry point
    handlers.lua            -- NATS subject handlers -> kb_data_structures calls
```

### nginx Routing Sketch
```
location /                    -> shell/index.html
location /app/{subsystem}/    -> subsystems/{subsystem}/static/
location /api/domains         -> api/discovery.lua
location /api/{domain}/*      -> subsystems/{domain}/api/*.lua
```

## Embedded Data Center Architecture

### Physical/Software KB Trees
The Postgres KB has two virtual trees plus a binding layer:

**Physical tree** -- hardware topology:
```
system.site.cpu_01              (system controller)
  +-- nats_server: {port: 4222, ws: 9222}
  +-- mqtt_server: {port: 1883}
  +-- postgres: {port: 5432}
system.site.cpu_02              (node controller)
system.site.cpu_03              (node controller)
...
```

**Software tree** -- subsystem definitions:
```
subsystems.surface_ops
  +-- container_def: {image: ..., deps: [...]}
  +-- action_server config
  +-- robot definitions
subsystems.subsurface
subsystems.logistics
...
```

**Binding layer** -- instantiation:
```
container_defs          (abstract: what software exists)
    |
instantiated_containers (concrete: this software on this CPU)
    maps: surface_ops -> cpu_02
          subsurface  -> cpu_03
```

### System Controller vs Node Controllers
- **System controller** (one node): owns Postgres, OpenResty, KB sidecar,
  NATS server, DSL container. The central authority.
- **Node controllers** (per CPU): own local SQLite, action servers, robots.
  Register with system controller. Report health via NATS.
- NATS/MQTT server endpoints are specified in the physical KB tree,
  not hardcoded -- node controllers look up their NATS server from KB.

### DSL Container (build phase)
- Runs master DSL script that constructs everything:
  1. Physical tree (hardware, service endpoints)
  2. Software tree (subsystem defs, container specs)
  3. Binding (container instances -> CPU nodes)
  4. SQLite extracts (one per action server domain)
- Single source of truth: master DSL script -> Postgres -> everything else

### Web API Discovery
- Tab list = query software subsystems tree
- Node list = query physical tree CPUs
- Where to route = follow binding (subsystem -> CPU -> NATS endpoint)
- All runtime discovery from KB, zero config files

### API Hierarchy
```
/api/system/nodes                  -- list all CPUs from physical tree
/api/system/domains                -- list all subsystems across nodes
/api/system/health                 -- system-wide health

/api/nodes/{node_id}/status        -- node controller health
/api/nodes/{node_id}/domains       -- subsystems on this node
/api/nodes/{node_id}/{domain}/*    -- missions, robots, status
```

## Architecture Decisions

### Data Flow Split
- **NATS** -- real-time operational bus
  - Mission submit (via NATS JobQueue to action servers)
  - Live status per robot (NATS KeyStore reads)
  - Mission cancel
- **Postgres** -- durable bookends and long-term data
  - Mission start/complete/fail records with error detail
  - Domain catalog (discovery: which moonbase.alpha.* domains exist)
  - Robot registry, capabilities, fleet config
  - Mission history queries ("what happened last week?")

### Tech Stack
- **Web framework**: OpenResty (nginx + LuaJIT)
- **Frontend shell**: Vanilla HTML/JS, nats.ws for live updates
- **Postgres 17** (pgvector): localhost:5432 -- already running
- **NATS JetStream** (RAM): localhost:4222 -- already running
- **Mosquitto**: localhost:1883 -- edge/hardware facing (MQTT)
- **SQLite**: local per action server for board-local planning data

### Source of Truth
- Postgres KB DSL is the single source of truth
- SQLite per action server is a subset extract of its domain
- Construction phase uses ltree + stack-based DSL (construct_kb.lua)
- Use phase via kb_data_structures.lua facade (10 sub-modules)

### Existing Infrastructure
- Action servers already publish status to NATS KeyStore
- Action servers already accept jobs via NATS JobQueue
- Postgres KB already has: search, status, job queue, stream,
  RPC client/server, bitmask, JSONB document, links

### Postgres Bookend Writes (new)
- Action servers write to Postgres stream on bookend events only:
  mission_started, mission_completed, mission_failed
- Lightweight addition to existing action server code

### Discovery
- Web API queries Postgres KB to discover active domains
- Each domain has a NATS subject prefix (e.g., moonbase.alpha.surface_ops)
- No config files -- Postgres is the registry

## Containers
- pg-vector (pgvector/pgvector:pg17) -- port 5432
- nats-js-ram (nanodatacenter/nats-js-ram) -- port 4222, 8222
  - Current: NATS v2.10.29, no WebSocket enabled
  - TODO: Rebuild this container to add WebSocket support (port 9222)
  - The `nats:latest` image had trouble previously; build a custom image
    based on nanodatacenter/nats-js-ram with WebSocket config added
  - Config addition needed:
    ```
    websocket {
        port: 9222
        no_tls: true
    }
    ```
- mosquitto-ram-ws -- port 1883, 9001
- system_api (OpenResty) -- containerize later, host process for now

## Decisions Locked In

1. Backend: OpenResty (nginx + LuaJIT)
2. Frontend: Alpine.js + Shoelace web components + nats.ws (all vendored, no npm/Node)
3. Postgres access: KB query sidecar over NATS (existing DBI code untouched)
4. Real-time: NATS WebSocket direct to browser
5. Federation: nginx location blocks per subsystem, iframe isolation
6. Source of truth: Postgres KB DSL (master DSL script -> Postgres -> everything)
7. NATS container: rebuild nanodatacenter/nats-js-ram with WebSocket support
8. No auth for this version (demo only)
9. Embedded data center model: system controller + node controllers
10. Two KB trees: physical (hardware/topology) + software (subsystems/containers)
11. Binding layer maps container defs to CPU nodes
12. DSL container generates both Postgres KB and per-domain SQLite extracts
13. Tabs/subsystems discovered at runtime from KB, not hardcoded

## Open Design Questions

1. Mission history schema in Postgres (which stream module?)
2. Multi-domain coordination (cross-domain missions?)
3. NATS client for OpenResty (evaluate existing C library, lua-resty wrapper)
4. REST endpoint detail per subsystem
5. KB sidecar NATS subject design (request/reply schema)
6. Master DSL script format and structure
7. Node controller registration/heartbeat protocol
8. Container instantiation process (who starts containers on remote nodes?)

## Build Strategy

### Bottom-up, not top-down
- Previous version of this system was built in Go, top-down. It worked but
  the web interface layer slowed things down.
- This time: building bottom-up. The planner (ros_planner_ii) is already
  working with 293 tests. The KB DSL, NATS transport, action server,
  sequencer, global planner are all proven.
- The top-level architecture described here WILL CHANGE as we build upward.
  These notes capture the vision; implementation will refine it.
- Next: continue building upward from the existing working pieces.

## Next Steps (morning of 2026-04-04)
- Review these notes fresh
- Decide where to start building next (likely: KB sidecar or DSL container,
  since those are closest to existing working code)
- Design master DSL script structure (physical + software + binding trees)
- Design REST endpoint map (system-level + per-node + per-domain)
- Design KB sidecar NATS request/reply subjects and payload schemas
- Design node controller registration and health protocol
- Prototype: shell + system view + one subsystem round-trip
