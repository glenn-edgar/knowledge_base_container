# ROS Planner II — Continuation Plan

## Session Summary (2026-04-05)

A major session covering six areas:

### 1. MQTT-only architecture (complete)
- All robot communication uses MQTT. NATS KV for external consumers only.
- Bridge, NATS robot, socket transport — all code removed.
- NATS transport (nats_transport.lua) deleted. Socket transport (vmrt.c, transport.lua) deleted.
- 24 files removed total (legacy tests, transports, bridge dir, NATS robot dir).

### 2. Real-time hub tick path (complete)
- Zero NATS calls on the hub tick loop. All blocking KV writes removed.
- In-memory blackboard replaces ks_blackboard (no cross-process sharing needed).
- Status published via `kv_bridge/write` MQTT topic (fire-and-forget, microseconds).
- Wall-clock timeouts: 5s ack, 10s kb_done (os.time deadlines in event_handlers.lua).
- Root cause fixed: tick-count timeouts burned instantly with tick_usleep=0 in coroutine mode.

### 3. kv-bridge Go container (complete)
- `third_party_containers/kv_bridge/` — Go binary in alpine Docker (~5MB).
- Subscribes to `kv_bridge/write` MQTT topic. Writes to NATS KV async.
- JSON envelope: `{"bucket":"...","key":"...","value":{...}}` — value is raw JSON passthrough.
- Delete support: `{"op":"delete"}`. Bucket handles cached. 30s stats logging.
- `docker start kv-bridge` / `docker stop kv-bridge`. `--restart unless-stopped`.

### 4. Client/server split (complete)
- `ros_scripts/start_server.sh` — builds KB, exports to NATS, runs persistent planner.
- `ros_scripts/start_robot.sh` — starts MQTT robot from config JSON.
- `ros_scripts/submit_mission.sh` — submits mission JSON, polls NATS KV for result.
- `ros_scripts/planner_server.lua` — persistent action_server (drain_nats=true, stays alive).
- `ros_scripts/mission_client.lua` — reads mission JSON, submits via NATS JobQueue, polls status/result.
- Example missions: `missions/deliver_sample.json` (tested), `missions/full_circuit.json` (tested).
- Both missions pass end-to-end: client → NATS queue → server → MQTT → robot → result.

### 5. CBOR wire format end-to-end (complete)
- `remote_mqtt_ct.lua` accepts `VMRT_WIRE_FORMAT=cbor` env var.
- Hub outbound: `send_rpc()` encodes to CBOR per robot's wire_format.
- Hub inbound: `poll()` decodes CBOR per robot's wire_format.
- 87/87 tests pass with CBOR, identical route to JSON test.
- No separate robot directory — same code, env var switch.

### 6. KB DSL updated for MQTT-first architecture
- `kb_dsl/scripts/site_config.lua` updated:
  - Removed mqtt_bridge container (eliminated).
  - Added kv_bridge container with mqtt_client + nats_client services.
  - All robots transport="mqtt". rover_1 JSON, rover_2 CBOR.
  - Action server planners have mqtt_client service (planner owns MQTT directly).
  - Robot capabilities match actual virtual node names.
  - Added robot_class field, replaced nats_prefix with site field.
- `kb_dsl/scripts/software_tree.lua` updated for new fields.
- `kb_dsl/scripts/master_build.lua` updated: resolves paths via script_dir, outputs SQLite to `SQLITE_DATA` env var (default `/home/gedgar/Sqlite_Data`).
- Master build tested: Postgres KB populated, 4 SQLite extracts generated.

---

## Tests: Current State

```bash
cd ros_planner_ii/tests

./run_tests.sh all            # 351 assertions, 0 failures, 6 suites

# Individual suites:
./run_tests.sh planner        # 60/60 — graph, routing, KB schema
./run_tests.sh mqtt_direct    # 87/87 — planner → MQTT (JSON) → robot
./run_tests.sh mqtt_cbor      # 87/87 — planner → MQTT (CBOR) → robot
./run_tests.sh hub_rt         # 87/87 — hub_runtime API + mission telemetry
./run_tests.sh sequencer      # 16/16 — route execution + telemetry
./run_tests.sh action         # 14/14 — mission builder + coroutine + direct
./run_tests.sh kv_writer      # 16/16 — coalescing queue (unit, run separately)
./run_tests.sh link_manager   # 25/25 — link state machine (unit, run separately)
```

---

## Planner Container (DONE — 2026-04-06)

### Design decision: Single generic image
- ONE image: `nanodatacenter/ros-planner:latest`
- SQLite DBs pre-built on host by master_build.lua → `/home/gedgar/Sqlite_Data/`
- Container bind-mounts: `-v /home/gedgar/Sqlite_Data:/data`
- Env vars specify: `SQLITE_DB`, `MQTT_HOST`, `MQTT_PORT`, `NATS_URL`
- Same image for any domain — change env vars and mount for different deployments

### What the Dockerfile needs
The container packages the LuaJIT runtime and all Lua/C modules:

**C shared libraries (.so):**
- `knowledge_base/nats/libnats.so` — NATS C client
- `knowledge_base/mqtt/libmqtt_pubsub.so` — MQTT PubSub C wrapper
- `knowledge_base/mqtt/liblua_cbor.so` — JSON↔CBOR codec

**LuaJIT + Lua modules (many directories):**
- `ros_planner_ii/runtime/` — mqtt_hub_transport, mqtt_transport, kv_writer, link_manager, queue_monitor, ks_blackboard (kept as fallback)
- `ros_planner_ii/local_planner/lib/` — hub_runtime, sequencer, mission
- `ros_planner_ii/action_server/lib/` — action_server, mission_builder
- `ros_planner_ii/global_planner/lib/` — global_planner, route_builder
- `ros_planner_ii/hub_dsl/` — hub_dsl, protocol/, hub_functions/, kb/
- `ros_planner_ii/hub_dsl/kb_construct/` — construct_surface_ops.lua, kb_runtime, kb_exporter, kb_query
- `ros_planner_ii/robots/test_robot/` — remote_mqtt_ct.lua, remote_dsl.lua, remote_user_functions.lua, build.sh, capabilities.lua
- `ros_planner_ii_mqtt_robot/` — mqtt_robot_main.lua, mqtt_robot_config.lua, remote_mqtt_ct.lua
- `chain_tree_luajit/runtime_dict/` — ct_runtime, ct_engine, ct_loader_pure, ct_definitions, ct_builtins, fn_registry
- `chain_tree_luajit/lua_dsl/` — lua_support/, luajit_pipeline/ (json_util lives here)
- `knowledge_base/nats/` + `nats/lib/` — nats_key_store, nats_job_queue
- `knowledge_base/mqtt/` + `mqtt/lib/` — mqtt_pubsub Lua wrapper, lua_cbor Lua wrapper
- `knowledge_base/sqlite3/construct_kb/` — sqlite3_helpers, construct_data_tables
- `ros_scripts/planner_server.lua` — entrypoint

**Entrypoint flow:**
1. Read env vars (SQLITE_DB, MQTT_HOST, MQTT_PORT, NATS_URL, VMRT_KB_SITE)
2. Build DSL artifacts if needed (hub.json, remote.json from KB data)
3. Export KB to NATS KV (kb_exporter)
4. Run planner_server.lua

### Docker run:
```bash
docker run --name surface-ops-planner \
  --restart unless-stopped \
  --network host \
  -v /home/gedgar/Sqlite_Data:/data \
  -e SQLITE_DB=/data/surface_ops.db \
  -e MQTT_HOST=localhost \
  -e MQTT_PORT=1883 \
  -e NATS_URL=nats://127.0.0.1:4222 \
  -e VMRT_KB_SITE=moonbase.alpha.surface_ops \
  nanodatacenter/ros-planner:latest
```

### Build order:
1. Create `third_party_containers/ros_planner/Dockerfile` (multi-stage: build DSL artifacts, copy runtime)
2. Create entrypoint script (`entrypoint.sh` or `entrypoint.lua`)
3. Create `docker_build.sh`, `docker_run.sh`
4. Build image
5. Test: start container → submit mission from host client → verify result

### Test plan:
```bash
# 1. Ensure infrastructure is running:
docker start nats-js-ram
docker start mosquitto-ram-ws_main
docker start kv-bridge

# 2. Ensure KB is built:
export POSTGRES_PASSWORD
cd kb_dsl/scripts && luajit master_build.lua

# 3. Start robot on host (not containerized yet):
cd ros_scripts && ./start_robot.sh <config.json>

# 4. Start planner container:
cd third_party_containers/ros_planner && ./docker_run.sh

# 5. Submit mission from host:
cd ros_scripts && ./submit_mission.sh missions/deliver_sample.json
```

---

## File Locations

```
ros_planner_ii/
  runtime/
    mqtt_hub_transport.lua  — shared MQTT client, CBOR decode, publish_kv
    mqtt_transport.lua      — per-robot MQTT (JSON or CBOR via opts)
    kv_writer.lua           — coalescing KV queue (used by link_manager)
    link_manager.lua        — URLP v1 state machine
    queue_monitor.lua       — transport ↔ ChainTree events
  local_planner/lib/
    hub_runtime.lua         — in-memory blackboard, MQTT auto-poll, zero NATS
    sequencer.lua           — mqtt_hub pass-through
  action_server/lib/
    action_server.lua       — persistent serve(), mqtt_hub pass-through
  hub_dsl/hub_functions/
    event_handlers.lua      — wall-clock timeouts (os.time deadlines)
  robots/test_robot/        — test robot DSL + user functions
  tests/                    — 6 suites, 351 assertions

ros_planner_ii_mqtt_robot/
  remote_mqtt_ct.lua        — MQTT robot (VMRT_WIRE_FORMAT=json|cbor)
  mqtt_robot_main.lua       — independent robot process
  mqtt_robot_config.lua     — config loader + status publisher

ros_scripts/
  start_server.sh           — planner server launcher
  start_robot.sh            — robot launcher
  submit_mission.sh         — mission client launcher
  planner_server.lua        — persistent action server
  mission_client.lua        — submit + poll client
  missions/                 — example mission JSON files

kb_dsl/
  scripts/
    site_config.lua         — declarative site definition (MQTT-first)
    master_build.lua        — Postgres + SQLite build (outputs to SQLITE_DATA)
    physical_tree.lua       — hardware topology builder
    software_tree.lua       — domain/robot builder
    sqlite_extract.lua      — per-domain SQLite extractor

third_party_containers/
  kv_bridge/                — Go MQTT→NATS KV bridge (built, tested, running)
  ros_planner/              — TO BUILD: generic planner container

Data directories (bind mounts, outside repo):
  /home/gedgar/Postgres_Data/vector/  — Postgres data
  /home/gedgar/Sqlite_Data/           — SQLite DBs (surface_ops.db, fleet.db, etc.)
```

## Required Infrastructure (all must be running before planner)
- Postgres: `pg-vector` container, port 5432 (only for master_build, not runtime)
- NATS: `nats-js-ram` container, port 4222
- MQTT: `mosquitto-ram-ws_main` container, port 1883
- KV bridge: `kv-bridge` container (MQTT→NATS KV async)
