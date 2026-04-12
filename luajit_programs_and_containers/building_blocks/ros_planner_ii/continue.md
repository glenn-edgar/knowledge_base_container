# ROS Planner II — Continuation Plan

## Session Summary (2026-04-11, session 6)

### Transit Nodes + Operation Points (complete)

**Transit vs operation node types:**
- Nodes with `type = "transit"` render as small dashed squares on the board map. Not clickable in mission planner. Dijkstra routes through them silently.
- All other node types are operation points — render as circles, clickable, each click adds a mission step with the node's type as the operation.
- `global_planner:is_transit(node_name)` checks node type. `mission_builder` rejects transit stops.
- CSS color-codes operation circles by type (green=base/pass_gate, yellow=recharge, red=deliver_part, purple=inspection_scan, orange=load_shipping).
- `board-graph.js` inset: transit nodes shown as dimmed squares with "(transit)" label, not clickable in planner mode.

**Mining station transit ring:**
- 4 transit nodes (`transit_mine_w/n/e/s`) form a bypass loop around mining_zone_a and inspection_scan.
- Operation points offset from the main road with single spur edges in.
- Through-traffic (e.g., junction_north → mining_zone_b) routes around via the ring without entering operation points.

### Simplified Route Format (complete)

**Nav types simplified:**
- Edge `nav` field IS the `kb_name` directly (`path_spline`, `path_line`). Removed the old `spline_follow`/`line_follow`/`wall_follow` → kb_name mapping (`NAV_TO_KB`).
- Removed `path_rotate` insertion from route_builder. Removed heading tracking from mission_builder and mission-planner.js.
- Removed `compute_heading`, `heading_diff` from route_builder. Removed `computeHeading`, `headingDiff` from JS.

**Path array on edges:**
- `build_board()` DSL helper processes edges: author provides `path = {}` (straight line) or `path = {{x=,y=}, ...}` (intermediate waypoints). DSL auto-prepends from-node coords and appends to-node coords. Produces flat `[x1,y1, x2,y2, ...]` array.
- Empty path auto-interpolates 2 intermediate points at 1/3 and 2/3 (every edge gets 4-point paths).
- `global_planner.build_graph` carries `path` through adjacency list. Reverse edges get reversed path.
- Route params are just `{speed, path}` — removed `from_x/from_y/to_x/to_y/distance/segment_index/total_segments`.

**Wire format to robot:**
```json
{"packet_type": 2, "speed": 150, "path": [0, 0, 267, 0, 533, 0, 800, 0]}
{"packet_type": 20, "operation_type": "deliver_part", "data": {"arm_target": -45}}
{"packet_type": 11}
```

### Generic Operation VN (complete)

**Single operation VN (packet_type=20) replaces 8 individual operation VNs:**
- KB VN definition: `operation` with `packet_type_id = 20`, schema: `operation_type` (string) + `data` (object).
- Robot class: `virtual_nodes` now lists only path + lifecycle VNs (`init_check`, `path_spline`, `path_line`, `operation`, `idle`). New `operation_types` list declares supported operations (`base`, `deliver_part`, `paint_sample`, `load_shipping`, `pass_gate`, `inspection_scan`, `recharge`).
- `kb_query:get_class_operation_types(class_name)` returns the list.
- `mission_builder.build()` validates stop actions against `operation_types`. Emits `{kb_name="operation", params={operation_type=X, data={...}}}`.
- `action_server.lua` fetches `operation_types` from KB class, passes to mission_builder for validation.
- Mission planner UI: preflight checks `operation_types` from robot config.

**Node type IS the operation:**
- Board node `type` field is the operation name (`deliver_part`, `inspection_scan`, `recharge`, `base`, `pass_gate`, `load_shipping`) or `transit`.
- Clicking a circle auto-attaches the operation. Step display shows `mining zone a [deliver part]`.
- Mission submission includes `action` field on each stop.

**MQTT robot:**
- `command_packets.lua`: `TYPE_OPERATION = 20`.
- `robot_controller.lua`: `worker_by_packet_type[20] = "worker_operation"` + energy cost.
- `remote_dsl.lua`: sparse test_list with `worker_operation` at index 20.
- `remote_user_functions.lua`: `WKR_OPERATION_INIT/MAIN` — prints operation_type, simulates 20-tick execution.
- `capabilities.lua`: updated with `operation` VN + `operation_types` list.

### Container Build Fixes (complete)

**Three pre-existing container build issues fixed:**
1. `kb_build.lua` — `master_build.lua` path didn't resolve in flat container layout (added fallback to `script_dir`).
2. `sqlite_extract.lua` — SQLite module path and `ltree.so` path didn't resolve in container (added fallback detection for both).
3. `Dockerfile` — missing `libsqlite3.so` symlink (only `.so.0` present, added `ln -sf`).

**Volume mount fix:**
- `site_config.lua` used `SQLITE_DATA` env which resolved to container-internal `/data` path instead of host path for `docker -v` mounts.
- Added `SQLITE_HOST_PATH` env var; `site_config.lua` prefers it over `SQLITE_DATA`.
- `start_planner_system.sh` passes `SQLITE_HOST_PATH` to orchestrator container.

**Planner image build fix:**
- `docker_build.sh` `cp` to root-owned `Sqlite_Data/` made non-fatal (orchestrator handles the authoritative copy).

### Board Builder Module (complete)

- Shared `board_builder.lua` extracted from both construct files.
- Validation: required fields (name, x, y, type on nodes; from, to, nav, speed, weight, path on edges), duplicate edges, unknown node references, waypoint structure.
- `board_builder.build(nodes, edges, opts)` — resolves paths, returns `{nodes, edges}`.
- Both `construct_surface_ops.lua` and `surface_ops_planner_data.lua` import from it.
- Staged in orchestrator image via `docker_build.sh`.

### Complex Operation Center (complete)

- Construction zone: 4 transit nodes (`transit_build_n/w/e/s`) forming approach lattice around `construction_bay` and `paint_station`.
- Hand-authored curved docking paths: 3-waypoint arcs from transit nodes into operation points (north arc, west sweep, south-east curve).
- `paint_station` (type=`paint_sample`, params: `arm_target=15, hold_time=500`).
- 20 nodes total, ~27 edges. Multiple approach angles to construction_bay.

### Wire Payload Cleanup (complete)

- `hub_runtime.lua` strips `next_test` from JSON sent to robot.
- Robot receives: `packet_type` + `seq` + `test_id` + action-specific params only.
- `seq` and `test_id` kept for ack/kb_done matching between planner and robot.

### Operation Params from Board Nodes (complete)

- Nodes carry optional `params` table (e.g., `mining_zone_a` has `{arm_target=-45, payload_type=1}`).
- `board_builder` passes params through. `global_planner.build_graph` stores `node.params`.
- `global_planner:get_node_params(node_name)` accessor.
- `mission_builder` merges: node default params ← stop-level overrides → operation VN data.
- Verified end-to-end: robot receives `{"data":{"arm_target":-45,"payload_type":1}}` for `mining_zone_a`.

### CBOR Wire Format (complete)

- `lua_cbor.c` upgraded: recursive `enc_json_value()` and `dec_cbor_value()` handle JSON arrays and nested maps. Original was flat maps only.
- `mqtt_hub_transport.lua`: CBOR encode/decode per robot via `wire_formats[robot_id]`, graceful fallback with debug logging.
- `rover_1_cbor_config.json`: CBOR robot config (`wire_format: "cbor"`). Just change config to switch.
- Path arrays encode to ~29 bytes CBOR vs 80+ bytes JSON.
- Full pipeline: link handshake announces `wire_format=cbor`, planner auto-switches, commands + responses both CBOR.
- Both JSON and CBOR robots can run simultaneously (per-robot wire format).

### Energy Budget from Path (complete)

- VN definitions: `energy_factor` for path VNs (path_spline=1.0, path_line=1.2), `energy_cost` for fixed VNs (init_check=50, operation=100, idle=10).
- Robot class: `energy_rate` — energy per unit distance (lunar_rover=0.5, construction_arm=0.3).
- `route_builder.path_distance(flat)` — computes total distance from flat path array by summing segment lengths.
- Each route action gets `energy` field: `distance * factor * rate` (path VNs) or fixed cost (other VNs).
- `mission_builder` sums `total_energy` across all actions. Returns in `plan_info`.
- Example: lander_pad → mining_zone_b = 2154 energy (800+400+894 path + 50 init + 10 idle) at rate 1.0.
- VN `json_schema` cleaned up: path VNs now list `speed` + `path` only (old from_x/to_x/distance/segment fields removed).

### Tests: 72 assertions, 0 failures
- Planner: path array format, no rotations, direct nav types, transit validation, operation VN emission, operation_types validation, unsupported operation rejection, node params merge
- Plus all prior session totals (orchestrator graph 91, link manager 39, link client 43, kv writer 16)

---

## Session Summary (2026-04-09, session 5)

### Mission Log Expansion + Mission Planner Preflight (complete)

**Mission planner preflight (client-side, `mission-planner.js`)**
- New shared module `system_api/shell/panels/board-graph.js` exporting `computeClusters` (union-find on nodes within `2*radius`), `bucketEdges`, `externalForCluster`, `clusterLabel`, `buildInsetSvg`.
- Mission planner board uses cluster-based rendering: singletons → circles, multi → diamonds at the centroid, external edges aggregated with `×N` count labels.
- Each mission step now expandable (`▶`/`▼`) showing the underlying actions: client-side Dijkstra over board edges + capability check against robot's `/api/robots` config + energy budget check against KV `_robot_status`.
- Failed actions render red; mission-level failure (insufficient energy / unsupported VN) shown in expanded display + a top-of-table preflight summary banner (ok/warn/fail).

**Mission log expansion (`mission-log.js` + `action_server.lua`)**
- Server: `_publish_mission_log` now stores rich payload — `route` (compact action summary), `legs`, `fault {reason, detail, action_index, kb_name}`, `unsupported`, `energy_required`, `energy_remaining`. Result fields attached at planning failure / energy failure / successful run paths.
- Client: each row gets a chevron toggle. Expanded view shows fault banner + per-action list with the failed action red and subsequent actions dimmed/`(not run)`. Backwards-compat with pre-upgrade entries (graceful "no route data" fallback).
- Mission log `.log-scroll` got `min-height: 0` to fix the flexbox scrollbar trap.

### Board Subgraph Inset (complete)

- Replaced the old text picker with a richer inset built by `buildInsetSvg`.
- Click a diamond → small popup shows internal edges + member node circles + dashed amber stub edges pointing toward each external neighbour cluster (labeled with neighbour name).
- A clickable member-name list under the SVG handles the picker job (the SVG circles overlap when nodes are co-located, so the list is the reliable picker).
- Same inset in both panels: planner (interactive — click member adds it as a step) and viewer (informational only).
- Robot markers retarget to clusters with a count badge when multiple robots share a cluster.
- Close mechanisms: click diamond again, click outside, Escape key, X button.

### Board DSL Builder + Path Field (complete)

**New module `ros_planner_ii/hub_dsl/kb_construct/board_builder.lua`** (≈190 lines)
- API: `BoardBuilder.new(label, name, properties)`, `add_node(name, x, y, type)`, `add_edge {table}`, `has_node`, `get_node`, `finalize(kb, description)`.
- Authors write maps incrementally with helper calls instead of one giant inline `kb:add_info_node` literal. User-written LuaJIT functions act as macros (no DSL vocabulary needed for paths).
- Single-table arg form for `add_edge` with named fields: `{from, to, nav, speed, weight, path}` — all required.
- Path is a flat list of `{x=,y=}` waypoint tables in node-local coordinates (from-node is `(0,0)`).
- Validation at `finalize()`: required fields present, endpoints in node table, first waypoint `(0,0)`, last waypoint = to-node local offset, no duplicate undirected edges, ≥2 waypoints.
- Dual storage produced from the named-key DSL form:
  - `path.waypoints` — `[{x,y}, ...]` for tools / human inspection
  - `path.flat`      — `[x1, y1, x2, y2, ...]` packed int array, optimized for embedded CBOR decode on the robot
- Unit tests (`test_board_builder.lua`) — 36 assertions, all pass.

**Migrated `construct_surface_ops.lua`** to use the builder. Every existing edge now carries a `path` field with the new dual form. For now all paths are 2-point endpoints-only — behaviour is byte-identical to the previous inline-table form because route_builder still computes its own geometry from node coords. The `line_follow` edge gets the same shape; the robot ignores it until line/wall paths are designed.

**Wire payload to robot (planned, not yet implemented)**: only `vn` name + `flat` array + speed cross to the robot. The `waypoints` named form stays in the KB for tooling.

### Tests: 341 assertions, 0 failures
- Board builder: 36 pass (NEW)
- Planner: 57 pass
- Plus all prior session totals (orchestrator graph 91, link manager 39, link client 43, kv writer 16, etc.)

---

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
bash ./run_tests.sh planner        # 72 assertions
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

# Start robot (Terminal 2) — JSON wire format
bash ros_scripts/start_robot.sh ros_planner_ii_mqtt_robot/rover_1_config.json

# Start robot (Terminal 2) — CBOR wire format
bash ros_scripts/start_robot.sh ros_planner_ii_mqtt_robot/rover_1_cbor_config.json

# Stop everything (except Postgres)
bash start_planner_system.sh stop

# Direct orchestrator usage
cd orchestrator
BB=.. LUA_PATH="./?.lua;$BB/knowledge_base/postgres/data_structures/?.lua;;" \
  luajit shutdown.lua --exclude postgres
  luajit startup.lua --exclude postgres
```

---

## Next Session (2026-04-12): Distributed KB + Fleet Management

### Task 1 — Distributed Knowledge Base Generation

Extend `site_config.lua` and `master_build.lua` to support multiple CPUs with per-CPU KB extracts. Currently all containers live on `cpu_01` with a single SQLite extract. The distributed model:

- Each CPU gets its own SQLite extract containing only the containers/services/domains assigned to it.
- `master_build.lua` generates per-CPU extracts alongside the current per-domain extracts.
- Container management becomes per-CPU: each CPU's orchestrator manages only its own containers.

### Task 2 — Fleet Manager Architecture

Two-tier fleet management:

- **Centralized fleet manager** — runs on the main CPU. Owns the mission queue, assigns missions to robot controllers, monitors fleet-wide status, handles multi-robot coordination (collision avoidance, priority).
- **Robot controllers** — run on per-robot CPUs (Pi Zero 2 W Linux platforms). Each controller manages one physical robot: hardware drivers, local navigation, sensor fusion, heartbeat. Connects to fleet manager via NATS.

The split: fleet manager decides WHERE the robot goes (mission planning, Dijkstra routing). Robot controller decides HOW to get there (motor control, path following, obstacle avoidance).

### Followup (queued)

- **Real robot driver** — replace sim workers with hardware drivers. Pi Zero 2 W target. `robot_controller.lua` is already robot-independent.
- **Telemetry collector** — placeholder in site_config, no real image.

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
    action_server.lua           — position tracking, init_node discovery, operation_types validation
    mission_builder.lua         — init_check + path VNs + operation VNs + idle
  global_planner/lib/
    global_planner.lua          — board graph, Dijkstra, is_transit(), path on edges
    route_builder.lua           — node path → {kb_name, params:{speed, path}} actions
  hub_dsl/kb_construct/
    board_builder.lua           — shared board DSL: build(), interpolate(), validation
    construct_surface_ops.lua   — board with transit ring, operation types, node params
    kb_exporter.lua             — exports to NATS KV (kb_export bucket)
    kb_query.lua                — list_boards, get_board, get_class_operation_types
  hub_dsl/protocol/
    command_packets.lua         — TYPE_OPERATION=20 added

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
- Robot link protocol still announces old 12-capability count — cosmetic, doesn't affect dispatch
