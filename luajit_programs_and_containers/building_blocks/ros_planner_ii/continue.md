# ROS Planner II — Continuation Plan

## Session Summary (2026-04-03)

### Infinite energy option (step 0) — DONE
`energy_infinite` config for plugged-in robots. Action server skips budget
check, hub_runtime and robot skip deduction. Set per class in DSL or per
robot in config JSON.

### Remote test mode (step 1) — DONE
`./run_tests.sh remote` assumes robot already running on NATS. No spawn,
no cleanup, no shutdown. 13 test checks via coroutine missions.

### MQTT robot + bridge — DONE
Complete MQTT deployment path for ESP32/Pico 2 W robots.

**New C libraries (in knowledge_base/mqtt/):**
- `libmqtt_pubsub.so` — streaming pub/sub with thread-safe ring buffer.
  Subscribe once, poll for messages. No re-subscribe overhead. (16/16 tests)
- `liblua_cbor.so` — JSON↔CBOR codec for flat map messages.
  25% size savings on typical robot protocol messages. (42/42 tests)

**MQTT transport layer:**
- `runtime/mqtt_transport.lua` — drop-in replacement for nats_transport.
  Same API: hub_side(), remote_side(), loopback(). Backed by PubSub.
  Supports `wire_format` option: "json" (default) or "cbor".

**Multi-robot NATS↔MQTT bridge:**
- `mqtt_bridge/mqtt_bridge.lua` — single bridge handles ALL MQTT robots.
  Wildcard MQTT subscriptions discover robots dynamically.
  Per-robot wire_format detection (JSON or CBOR).
  Three channels: rpc (NATS→MQTT), stream (MQTT→NATS), status (MQTT retained→NATS KV).

**Independent MQTT robot process:**
- `robots/mqtt_robot/mqtt_robot_main.lua` — standalone process, ESP32/Pico template.
- `robots/mqtt_robot/mqtt_robot_config.lua` — config loader, status publisher.
  Config JSON includes wire_format, energy, capabilities.
  Status published as MQTT retained messages (state, energy, bitmask).

**Architecture (three deployment paths):**
```
NATS:    Planner → NATS(JSON) → Robot(JSON)
MQTT/JSON: Planner → NATS(JSON) → Bridge → MQTT(JSON) → Robot
MQTT/CBOR: Planner → NATS(JSON) → Bridge → MQTT(CBOR) → Robot (ESP32/Pico)
```
Planner is identical in all three. Bridge is transparent.

---

## Tests: 422+ passing across 15 suites

```bash
cd ros_planner_ii/tests

# Core suites (293 tests)
./run_tests.sh planner     # 60/60 — Dijkstra, route builder, VN KB, exporter
./run_tests.sh action      # 14/14 — coroutine scheduler + direct execution
./run_tests.sh sequencer   # 16/16 — KB-driven route execution
./run_tests.sh hub_rt      # 87/87 — hub runtime + mission telemetry
./run_tests.sh nats_ct     # 72/72 — original full stack regression
./run_tests.sh nats        # 32/32 — NATS loopback
./run_tests.sh robot       # 12/12 — independent NATS robot process

# External robot
./run_tests.sh remote      # 13/13 — remote robot (assumes robot on NATS)

# MQTT suites
./run_tests.sh mqtt_bridge    # 72/72 — full 18-action route through bridge
./run_tests.sh mqtt_robot     # 12/12 — independent MQTT robot (JSON wire)
./run_tests.sh mqtt_robot_cbor # 12/12 — independent MQTT robot (CBOR wire)
./run_tests.sh mqtt_multi     # 8/8  — two robots (JSON+CBOR) through one bridge

# MQTT library tests (in knowledge_base/mqtt/)
# test/test_pubsub.lua    # 16/16 — PubSub streaming module
# test/test_cbor.lua       # 42/42 — JSON↔CBOR codec
# test/test_kv_store.lua   # existing KV store tests
# test/test_queue.lua      # existing queue tests

./run_tests.sh all         # core suites (except remote/mqtt — need brokers)

# C robot suites (in ros_planner_ii_robot/)
# test_c_robot.sh cbor      # 12/12 — C robot through bridge (CBOR wire)
# test_c_robot.sh json      # 12/12 — C robot through bridge (JSON wire)
```

---

## Next Steps

### 2. C Robot (standalone MQTT/CBOR) — DONE
Built at `/home/gedgar/knowledge_base_assembly/c_programs_and_containers/build_blocks/ros_planner_ii_robot/`:
- Standalone C binary: `robot_main` (~500 lines of C, zero warnings)
- Includes `libmqtt_pubsub` source (ring buffer pub/sub) and `cbor_codec` source (JSON↔CBOR)
- Same protocol as LuaJIT MQTT robot (ack, heartbeat, kb_done, status)
- Both JSON and CBOR wire formats tested through existing bridge
- 12/12 tests pass (status mirroring, mission execution, energy tracking, bitmask)
- Drop-in replacement for LuaJIT MQTT robot on the bridge
- Test: `./test_c_robot.sh [json|cbor]`

### 2b. ChainTree C robot integration (next)
- Add ChainTree C runtime to the C robot for event-driven processing
- MQTT packets → ChainTree CBOR events → user function dispatch
- Replace simple state machine with ChainTree KBs per action type

### 3. Thread bridge (this subsystem)
Continue from thread_bridge/continue.md plan:
- Simulated Thread robot (NATS loopback, no CBOR/SLIP)
- Bridge process (coroutine-based, routes planner↔robot)
- End-to-end loopback test
- Then CBOR codec, SLIP framing, real serial

### 4. Four deployment paths (same planner protocol)
```
NATS:     Planner → NATS(JSON) → Robot(JSON)                        [Pi Zero 2, LuaJIT]
MQTT/Lua: Planner → NATS(JSON) → Bridge → MQTT(JSON|CBOR) → Robot   [ESP32, Pico 2 W, LuaJIT]
MQTT/C:   Planner → NATS(JSON) → Bridge → MQTT(JSON|CBOR) → Robot   [Pi Zero 2, C binary]
Thread:   Planner → NATS(JSON) → Bridge → Dongle(CBOR) → Mesh → Robot [Thread mesh]
```
All look identical to the planner. Translation boundary moves based on robot capability.

### 4b. Pi Zero 2 deployment
No cross-compile needed — Snapdragon dev laptop and Pi Zero 2 are both arm64.
Copy binary + config, install libmosquitto/libcjson on Pi, run.

### 5. Production hardening
- Recharge scheduler (external app, monitors energy, submits recharge missions)
- Dashboard/monitoring app (reads NATS status board + MQTT status via bridge)
- Error recovery, watchdog, graceful shutdown
- Bridge health monitoring (auto-restart on disconnect)

---

## NATS Topic Layout

```
moonbase.alpha.surface_ops.
  robots.rover_1.rpc                    ← hub→robot commands (JobQueue)
  robots.rover_1.stream_bus             ← robot→hub events (JobQueue)
  robots.rover_1.status.state           ← connected, wire_format, active_kb
  robots.rover_1.status.energy          ← energy_max, energy_remaining
  robots.rover_1.status.bitmask         ← raw, decoded fields, heartbeat
  robots.rover_1.status.connection      ← comm_type, nats_server, topics
  robots.rover_1.stream.telemetry       ← heartbeat circular buffer
  robots.rover_1.abort                  ← abort mission signal
  boards.landing_zone                   ← waypoint graph
  virtual_nodes.definitions.vn_type.*   ← VN schemas
  robot_class.lunar_rover.infra         ← class config (energy_max, capabilities)
  planner.route_planner.status.*        ← planner state
  action_server.rover_1.status          ← mission progress
  action_server.rover_1.result          ← mission outcome
  action_server.missions                ← mission submission queue
```

## MQTT Topic Layout (bridged to NATS)

```
moonbase/alpha/surface_ops/
  robots/rover_1/rpc                    ← bridge→robot commands
  robots/rover_1/stream_bus             ← robot→bridge events
  robots/rover_1/status/state           ← connected, wire_format (retained)
  robots/rover_1/status/energy          ← energy_max, energy_remaining (retained)
  robots/rover_1/status/bitmask         ← raw, fields, heartbeat (retained)
```

Bridge subscribes: `+/stream_bus`, `+/status/#` (wildcard, all robots)
Bridge mirrors: MQTT retained status → NATS KeyStore (dot notation)

## File Locations

```
ros_planner_ii/
  action_server/lib/          ← action_server.lua, mission_builder.lua
  global_planner/lib/         ← dijkstra.lua, route_builder.lua, global_planner.lua
  local_planner/lib/          ← hub_runtime.lua, mission.lua, sequencer.lua
  hub_dsl/                    ← KB plugins, protocol, hub_functions, kb_construct
  robots/test_robot/          ← test robot (spawned by test scripts)
  robots/robot_process/       ← independent NATS robot (standalone, Pi template)
  robots/mqtt_robot/          ← independent MQTT robot (ESP32/Pico template)
    mqtt_robot_main.lua       ← main process (tick loop, energy, bitmask)
    mqtt_robot_config.lua     ← config loader, MQTT status publisher
    remote_mqtt_ct.lua        ← test robot (ChainTree, no config/status)
  mqtt_bridge/                ← multi-robot NATS↔MQTT bridge
    mqtt_bridge.lua           ← single process, all robots, JSON+CBOR
  runtime/                    ← vmrt, nats_transport, mqtt_transport, queue_monitor
  tests/                      ← all test suites

knowledge_base/mqtt/          ← MQTT C libraries + Lua bindings
  libmqtt_pubsub.so          ← streaming pub/sub (ring buffer + mosquitto)
  liblua_cbor.so             ← JSON↔CBOR codec
  libmqtt_luajit_adapter.so  ← KV store + queue (batch reader)
  lib/mqtt_pubsub.lua        ← PubSub FFI binding
  lib/lua_cbor.lua           ← CBOR FFI binding
  lib/mqtt_kv_store.lua      ← KV store FFI binding
  lib/mqtt_queue.lua          ← Queue FFI binding
  test/                       ← unit tests

thread_bridge/                ← Thread mesh bridge (TODO)
  fnv1a/                      ← libfnv1a.so (built, verified)
  lib/                        ← fnv1a.lua FFI wrapper

c_programs_and_containers/build_blocks/
  ros_planner_ii_robot/       ← C robot (MQTT/CBOR, standalone binary)
    libs/mqtt_pubsub.c/.h     ← streaming ring buffer pub/sub
    libs/cbor_codec.c/.h      ← JSON↔CBOR codec
    src/main.c                ← entry point + tick loop
    src/robot_protocol.h      ← packet types, energy costs, durations
    src/robot_state.c/.h      ← energy, pose, worker lifecycle
    src/robot_mqtt.c/.h       ← MQTT transport, wire format
    src/robot_config.c/.h     ← JSON config loader
    src/json_extract.c/.h     ← field extraction from payloads
    test_c_robot.sh           ← end-to-end test (CBOR/JSON)
  chain_tree_c/               ← ChainTree C runtime (separate project)
  knowledge_base/nats/        ← C NATS/JetStream library
```

## Robot Config JSON Format

### NATS robot (robots/robot_process/)
```json
{
    "robot_id": "rover_1",
    "site": "moonbase.alpha.surface_ops",
    "nats_server": "nats://127.0.0.1:4222",
    "robot_class": "lunar_rover",
    "remote_json": "remote.json",
    "energy_infinite": false
}
```

### MQTT robot (robots/mqtt_robot/)
```json
{
    "robot_id": "rover_1",
    "site": "moonbase.alpha.surface_ops",
    "mqtt_host": "localhost",
    "mqtt_port": 1883,
    "robot_class": "lunar_rover",
    "remote_json": "remote.json",
    "energy_max": 10000,
    "energy_infinite": false,
    "wire_format": "cbor",
    "capabilities": ["init_check", "path_spline", ...]
}
```

## Required Infrastructure

- NATS server: `nats://127.0.0.1:4222` (Docker: nanodatacenter/nats-js-ram)
- MQTT broker: `localhost:1883` (Docker: nanodatacenter/mosquitto-ram-ws)
