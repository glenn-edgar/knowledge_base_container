# ROS Planner II — Continuation Plan

## Session Summary (2026-04-08, session 4)

### KB Sidecar Removed (complete)

- Deleted `kb_sidecar` container + domain from `site_config.lua`
- Deleted `system_api/kb_sidecar/` directory and `shell.js`

### Container Lifecycle Orchestrator (complete)

**Full deployment manifest in site_config.lua:**
- Every container has: `depends_on`, `docker_name`, `image`, `ports`, `volumes`, `tmpfs`, `env`, `network`, `restart`, `links`
- `env` contains container-own config only — service endpoints resolved at runtime from KB
- `env_names` overrides protocol→env var mapping per container (e.g. kv-bridge uses `NATS_URL` not `NATS_SERVER`)

**Service discovery (Postgres KB is single source of truth):**
- Orchestrator reads dependency containers' service nodes from Postgres
- Only injects env for protocols the consumer declares with `role=client` in its services
- Protocol → env var mapping: nats→NATS_SERVER, mqtt→MQTT_HOST/PORT, postgres→PG_HOST/PORT/DBNAME/USER
- `env_names` allows per-container override (e.g. `{ nats = "NATS_URL" }`)
- Container-own env wins on conflict
- `$VAR` references expand from host environment

**Stale container detection:**
- `docker_inspect_config` reads existing container's image + env via `docker inspect`
- `is_stale` compares against KB spec (image + resolved env)
- `docker_ensure`: stale running → stop + rm + run. Stale stopped → rm + run. Fresh → start.
- No more manual `docker rm` needed when KB spec changes

**Orchestrator modules (`building_blocks/orchestrator/`):**
- `graph.lua` — KB query, dependency graph, topo sort, level grouping, service discovery, env_names, stale detection, docker run builder, docker_ensure
- `startup.lua` — One-shot: create-or-start (or recreate if stale) in topo order
- `shutdown.lua` — One-shot: reverse topo sort, stops leaves first
- `kb_build.lua` — One-shot: stop → master_build → SQLite extract → start

**Bug fixes:**
- `docker_exists` — LuaJIT `os.execute` returns 0/256, not true/false
- `run_cmd` — LuaJIT `io.popen:close()` always returns true; detect errors via output
- MQTT client ID collision — two planners shared `"planner_hub"`, now `planner_<site_name>`
- `bootstrap_container.lua` + `planner_server.lua` — env vars override KB for MQTT/NATS (Docker DNS vs localhost)
- Domain descriptions empty in KB — `construct_kb.add_header_node` 5th arg overwrites properties; fixed by passing description as 5th arg

### KB-Driven Multi-Domain UI (complete)

- `software_tree.lua` stores `planner_data` flag in domain data
- `/api/domains` returns `site`, `has_planner_data` per domain
- `index.html` builds sidebar tree dynamically from `/api/domains`
- Domains with `planner_data` get full 5-panel set (Robots, Mission Planner, Mission Log, Telemetry, Board)
- Domains without planner_data get legacy iframe
- `registry.js` — `_ctxForPanel` merges domain-specific `site`/`siteBucket` per panel
- `robots.js` — dynamic domain name in API call
- Planner bar rewires KV watcher when switching domains
- Board name discovered dynamically from `kb_export` KV (no hardcoded `landing_zone`)
- All panels wrapped KV watch/loadAll in try/catch (uncaught exceptions killed init flow)
- `kv-manager.js` `loadAll` collects keys first, then fetches values (iterator/get interleaving fix)

### Robot Position Tracking (complete)

- `link_manager.lua` — new `write_position(robot_id, node_name)` and `get_position(robot_id)` methods
- `action_server.lua` — discovers initial board node at startup, writes `current_node` to KV when robot goes live
- `action_server.lua` — updates `current_node` in KV after each navigation leg completes (action_index → leg destination lookup)
- Position stored in `robot_status` KV bucket: `{site}.robots.{id}.status.position`
- `mission-planner.js` — reads `current_node` from KV, auto-fills start node (no dropdown)
- `board.js` — renders robot markers at their actual current node (not hardcoded lander_pad)
- Position updates live via KV watch as robot moves

### Cleanup (complete)

- Removed `warehouse_ops_planner` container + domain (placeholder, no real image)
- Removed `fleet_manager` container + domain (work in progress, will add when ready)
- Removed bookend checkbox — `init_check` + `idle` always happen, no option
- `mission_builder.lua` — `start` field optional (defaults to first stop's node)
- Fixed HTTP monitoring env injection — only injects for protocols consumer needs
- `env_names` for kv-bridge: `{ nats = "NATS_URL" }` instead of default `NATS_SERVER`

### Tests: 248 assertions, 0 failures
- Orchestrator graph: 91 pass (topo sort, levels, exclude, service discovery, env_names, env merge, env refs, build_run_cmd, is_stale)
- Planner: 57 pass
- Link manager: 39 pass
- Link client: 43 pass
- KV writer: 16 pass

---

## Previous Sessions

### Session 3 (2026-04-08): Orchestrator + Sidecar Removal
- Initial orchestrator implementation (before stale detection, env_names)
- See session 4 for final state

### Session 2 (2026-04-08): Robot Instances Removed from KB
- Dynamic robot registration via link protocol
- Class-based KB (capabilities, energy from robot class, not instance)
- 155 assertions, 0 failures

### Session 1 (2026-04-08): Web UI Panel Architecture
- Single-page panel UI (6 panels), co-located VN support, mobile responsive

### Previous (2026-04-06/07)
- Robot controller refactor, unified KB build, container networking, production hardening

---

## Tests: Current State

```bash
cd ros_planner_ii/tests
bash ./run_tests.sh planner        # 57 assertions
bash ./run_tests.sh link_manager   # 39 assertions
bash ./run_tests.sh link_client    # 43 assertions
bash ./run_tests.sh kv_writer      # 16 assertions

cd orchestrator
luajit test_graph.lua              # 91 assertions
```

## How to Run

### Full System (Container)
```bash
cd ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks

# First time: build orchestrator + planner images, rebuild KB, start all
bash start_planner_system.sh rebuild

# Normal start (orchestrator image already built)
bash start_planner_system.sh start

# KB rebuild only (stop → master_build → SQLite extract → start)
bash start_planner_system.sh build

# Web UI
open http://localhost:8080/

# Start robot (Terminal 2)
bash ros_scripts/start_robot.sh ros_planner_ii_mqtt_robot/rover_1_config.json

# Stop everything (except Postgres)
bash start_planner_system.sh stop

# Direct orchestrator usage
cd orchestrator
BB=.. LUA_PATH="./?.lua;$BB/knowledge_base/postgres/data_structures/?.lua;;" \
  luajit shutdown.lua --exclude postgres
  luajit startup.lua --exclude postgres
```

---

## Next Session: KB-Defined Path Segments

### Design

The path between board nodes is **map data defined in the KB**, not computed by the planner.

**Current (broken model):**
- Route builder computes coordinates from node positions
- Inserts `path_rotate` VNs based on heading calculations
- Every edge is a single straight-line segment

**New model:**
- Board edges have a required `path` field — array of navigation segments
- Each segment specifies: VN type (path_spline, path_line, path_wall), waypoints, speed
- The KB IS the map — no coordinate math in the planner
- Dijkstra finds which edges to traverse (graph routing only)
- Route builder reads each edge's `path` array and passes segments to the robot as-is
- The robot handles all navigation including rotation — no `path_rotate` VN
- The planner is a pass-through for path data

**Changes needed:**
1. `surface_ops_planner_data.lua` — add `path` arrays to all board edges
2. `route_builder.lua` — remove heading/rotate logic, emit path segments from KB edge data
3. `mission_builder.lua` — same pass-through approach
4. `global_planner.lua` — Dijkstra unchanged, just passes edge data through
5. Remove `path_rotate` VN definition (robot handles rotation internally)
6. Tests — update route expectations

**Example edge with path:**
```lua
{ from = "lander_pad", to = "habitat_site", weight = 800,
  path = {
    { vn = "path_spline", speed = 150, distance = 800,
      waypoints = { {x=0,y=0}, {x=200,y=50}, {x=600,y=-30}, {x=800,y=0} } },
  }
}
```

---

## File Locations

```
orchestrator/
  graph.lua                     — dependency graph, topo sort, service discovery, stale detection
  startup.lua                   — one-shot: start containers in topo order
  shutdown.lua                  — one-shot: stop containers in reverse topo order
  kb_build.lua                  — one-shot: stop → master_build → start
  test_graph.lua                — 91 assertions

shell/
  index.html                    — dynamic tree from /api/domains, per-domain panels
  panels/
    registry.js                 — panel lifecycle, per-domain ctx override
    kv-manager.js               — shared KV bucket cache, collect-then-fetch loadAll
    robots.js                   — live robot status cards, dynamic domain API
    mission-planner.js          — route editor, auto-start from robot position KV
    mission-log.js              — last 50 missions from KV history
    live-telemetry.js           — JetStream event stream
    board.js                    — popup SVG board viewer, robot markers at current_node
  tabs/
    kb_console.html             — HTTP query tool
  vendor/
    nats.js                     — NATS WebSocket client (ESM module)

ros_planner_ii/
  runtime/
    link_manager.lua            — link protocol, write_position, KV writes
    kv_writer.lua               — coalescing KV queue
    mqtt_hub_transport.lua      — domain-specific MQTT client ID
  action_server/lib/
    action_server.lua           — position tracking, init_node discovery
    mission_builder.lua         — always init_check + idle, optional start
  hub_dsl/kb_construct/
    construct_surface_ops.lua   — board with co-located VNs
    kb_exporter.lua             — exports to NATS KV (kb_export bucket)
    kb_query.lua                — list_boards, get_board, class-based API

kb_dsl/scripts/
  site_config.lua               — containers with full run spec, env_names, depends_on
  physical_tree.lua             — stores run spec + env_names in KB
  software_tree.lua             — stores planner_data flag, description as 5th arg
  master_build.lua              — Postgres + SQLite extract

ros_scripts/
  planner_server.lua            — env var override for MQTT/NATS (Docker DNS)
  bootstrap_container.lua       — env var override for MQTT/NATS (Docker DNS)

third_party_containers/
  ros_planner/                  — planner container (docker_build.sh, docker_run.sh, Dockerfile)
  orchestrator/                 — lifecycle orchestrator (docker_build.sh, docker_run.sh, Dockerfile)
  openresty/                    — web gateway (nginx.conf with /api/domains site+has_planner_data)
```

## Required Infrastructure
- Postgres: `pg-vector` container (bridge network) — KB source of truth
- NATS: `nats-js-ram` container (bridge + planner-net, port 9222 WebSocket)
- MQTT: `mosquitto-ram-ws_main` container (bridge + planner-net)
- KV bridge: `kv-bridge` container (planner-net, env_names: nats→NATS_URL)
- Web gateway: `openresty-gateway` container (bridge, port 8080, mounts shell/)
- Planner: `surface_ops-planner` container (planner-net, mounts Sqlite_Data/)
- Orchestrator: `nanodatacenter/orchestrator` image (one-shot, Docker socket mount, host network)

## Minor Items
- `telemetry_collector` placeholder in site_config — no real image, remove or build
- Mission log KV bucket must have `history=50` — delete stale bucket if history=1, planner recreates on restart
