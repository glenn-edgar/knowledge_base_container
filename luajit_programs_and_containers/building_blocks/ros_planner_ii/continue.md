# ROS Planner II — Continuation Plan

## Session Summary (2026-04-02)

### Unified namespace refactoring
All KB paths, NATS topics, and bucket names now follow lowercase convention rooted at DSL site prefix (`moonbase.alpha.surface_ops`). Same keys in SQLite, NATS, and future Postgres. Cookie-cutterable by changing the DSL root.

### Energy budget system
- `energy_max` per robot class in DSL (rovers=10000, arms=5000)
- Edge weights = energy tokens (Dijkstra cost = energy cost)
- Action server rejects missions if total_cost > energy_remaining
- Robot owns energy state, publishes to NATS status
- `recharge` VN (packet_type_id=12) restores energy to max
- Energy persists across restarts via NATS KV (saved every 30s)

### Bitmask status board
- Hub publishes decoded bitmask to NATS every 10 ticks or on change
- Bit 0 = heartbeat (robot sets to 1, external watchdog clears to 0)
- Published as: { kb_name, raw, fields, robot_id, timestamp }

### Capability validation
- mission_builder.build() validates stop actions AND nav VNs against capabilities
- Rejects at plan time with unsupported_capabilities error
- Published to status board with specific unsupported action list

### Mission lifecycle guards
- submit() rejects if robot already has active mission
- cancel(robot_id) explicit cancel with reason="cancelled"
- site removed from mission_cmd

### Independent robot process
- `robots/robot_process/robot_main.lua` — standalone process, Pi Zero 2 template
- `robots/robot_process/robot_config.lua` — config loader, inventory validator, slot claim
- Reads config JSON → validates against NATS KV inventory → claims slot
- Energy recovery on restart (reads last saved from NATS KV)
- Heartbeat bit + bitmask + energy published to NATS status board

### Bug fixes
- hub_control instance/module method dispatch (module wrappers shadowed instance methods via __index)
- Shared KB construct library labels: KB_STATUS_FIELD→status, KB_STREAM_FIELD→stream

---

## Tests: 293 passing across 7 suites

```bash
cd ros_planner_ii/tests
./run_tests.sh planner     # 60/60 — Dijkstra, route builder, VN KB, exporter
./run_tests.sh action      # 14/14 — coroutine scheduler + direct execution
./run_tests.sh sequencer   # 16/16 — KB-driven route execution
./run_tests.sh hub_rt      # 87/87 — hub runtime + mission telemetry
./run_tests.sh nats_ct     # 72/72 — original full stack regression
./run_tests.sh nats        # 32/32 — NATS loopback
./run_tests.sh robot       # 12/12 — independent robot process
./run_tests.sh all         # everything
```

---

## Next Steps

### 1. Remote test mode (this subsystem)
Add `./run_tests.sh remote` that assumes the robot is already running externally (on a Pi or other machine). No spawn, no cleanup — just runs the planner test against a robot that's already on NATS.

### 2. ChainTree C robot (separate project)
Develop in `/home/gedgar/knowledge_base_assembly/c_programs_and_containers/build_blocks/chain_tree_c/`:
- New ChainTree C nodes/leaves for JSON↔CBOR protocol handling
- C robot process: ChainTree C runtime + C NATS client + CBOR codec
- Uses FNV-1a integer hash keys (same as Thread bridge)
- Speaks JSON on NATS (same protocol as LuaJIT robot)
- Pi Zero 2 does JSON→CBOR directly (no dongle needed)
- Fold back into ChainTree C test suite

### 3. Thread bridge (this subsystem)
Continue from thread_bridge/continue.md plan:
- Simulated Thread robot (NATS loopback, no CBOR/SLIP)
- Bridge process (coroutine-based, routes planner↔robot)
- End-to-end loopback test
- Then CBOR codec, SLIP framing, real serial

### 4. Two deployment paths (same planner protocol)
```
Thread:  Planner → NATS(JSON) → Bridge → Dongle(JSON→CBOR) → Mesh → Robot(CBOR)
Pi:      Planner → NATS(JSON) → Robot(JSON→CBOR internally)
```
Both look identical to the planner. Translation boundary moves based on robot capability.

### 5. Production hardening
- Multi-robot test (2+ robots with separate remotes)
- Recharge scheduler (external app, monitors energy, submits recharge missions)
- Dashboard/monitoring app (reads NATS status board)
- Error recovery, watchdog, graceful shutdown

---

## NATS Topic Layout

```
moonbase.alpha.surface_ops.
  robots.rover_1.rpc                    ← hub→robot commands (JobQueue)
  robots.rover_1.stream_bus             ← robot→hub events (JobQueue)
  robots.rover_1.status.state           ← connected, active_kb, pose
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

## File Locations

```
ros_planner_ii/
  action_server/lib/          ← action_server.lua, mission_builder.lua
  global_planner/lib/         ← dijkstra.lua, route_builder.lua, global_planner.lua
  local_planner/lib/          ← hub_runtime.lua, mission.lua, sequencer.lua
  hub_dsl/                    ← KB plugins, protocol, hub_functions, kb_construct
  robots/test_robot/          ← test robot (spawned by test scripts)
  robots/robot_process/       ← independent robot (standalone, Pi template)
  runtime/                    ← vmrt, nats_transport, queue_monitor, ks_blackboard
  tests/                      ← all test suites

thread_bridge/                ← Thread mesh bridge (separate process)
  fnv1a/                      ← libfnv1a.so (built, verified)
  lib/                        ← fnv1a.lua FFI wrapper
  sim_robot/                  ← simulated Thread robot (TODO)
  bridge/                     ← bridge process (TODO)

c_programs_and_containers/build_blocks/
  chain_tree_c/               ← ChainTree C runtime (separate project)
  knowledge_base/nats/        ← C NATS/JetStream library
```
