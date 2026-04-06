# ROS Planner II — Continuation Plan

## Session Summary (2026-04-06)

Major cleanup and architecture session covering seven areas:

### 1. Planner Container (complete)
- Image: `nanodatacenter/ros-planner:latest` (150MB, Ubuntu 24.04 aarch64)
- Location: `third_party_containers/ros_planner/`
- Prebuilt .so files from host (no in-container compilation, fast build ~5s)
- `bootstrap_container.lua` queries extracted SQLite for NATS/MQTT/site from KB
- `docker_build.sh` stages Lua modules, `docker_run.sh` runs with domain arg
- Tested and running

### 2. Link Protocol — URLP v1 (complete)
- Split `link_announce` from `link_heartbeat` (keepalive only, no state transitions)
- Robot side: `link_client.lua` in `runtime/` — announce → wait_ack → confirm → live
- Planner side: `link_manager.lua` — re-announce from live robot fires `on_link_exception`
- Planner heartbeat seq for restart detection (robot detects planner reboot)
- Mission cancelled on link exception (robot reboot, disconnect, stale timeout)
- Wired into `action_server.serve()` loop via `mqtt_hub_transport` link handler
- Both `remote_mqtt_ct.lua` and `mqtt_robot_main.lua` updated with link_client
- Unit tests: 38 (link_manager) + 41 (link_client)

### 3. MQTT Robot Self-Sufficient (complete)
- `ros_planner_ii_mqtt_robot/` now contains everything: remote_dsl.lua, remote_user_functions.lua, capabilities.lua, build.sh
- All 12 VN workers including `recharge` (matches lunar_rover KB definition)
- `robots/test_robot/` deleted — MQTT robot is the canonical robot
- All scripts updated to point at `ros_planner_ii_mqtt_robot/`

### 4. Hub DSL Eliminated (complete)
- `hub_runtime.lua` rewritten as state machine: idle → wait_ack → active → done/error
- VN definitions loaded from SQLite KB at startup via `kb_query:get_all_virtual_nodes()`
- Same API: activate_kb, tick, kb_is_complete, deactivate_kb
- No more hub.json, no more build step, no ChainTree dependency on hub side
- Adding a new VN to the KB requires zero hub-side code changes
- Removed from: run_tests.sh build step, Dockerfile, docker_build.sh, bootstrap

### 5. Planner-Generated Test Routes (complete)
- All integration tests now use global_planner + mission_builder against landing_zone board
- No hardcoded 18-action routes — Dijkstra generates routes from moon map
- test_hub_runtime.lua removed (redundant with sequencer test)
- 5 test suites: planner, mqtt_direct, mqtt_cbor, sequencer, action

### 6. Robot Capabilities from Link Protocol (complete)
- Action server uses robot's announced capabilities from link_confirm
- Falls back to KB only in direct execution mode (tests without link protocol)
- Robot's actual capabilities are authoritative over KB's static definition

### 7. No Hardcoded Defaults (complete)
- Removed all fallback defaults for site, NATS, MQTT from runtime modules
- Each module requires values from constructor opts (errors if missing)
- Values originate from KB query → planner_server → action_server → sequencer → hub_runtime
- Test harness (run_tests.sh) provides dev defaults via env vars

---

## Tests: Current State

```bash
cd ros_planner_ii/tests

bash ./run_tests.sh all         # 123 assertions, 0 failures, 5 suites

# Individual suites:
bash ./run_tests.sh planner     # 60 — graph, routing, KB schema
bash ./run_tests.sh mqtt_direct # 23 — JSON wire, 5-stop mission from moon map
bash ./run_tests.sh mqtt_cbor   # 11 — CBOR wire, same mission
bash ./run_tests.sh sequencer   # 15 — sequencer API, 3-stop mission with recharge
bash ./run_tests.sh action      # 14 — action server, coroutine + direct execution

# Unit tests (run separately):
bash ./run_tests.sh link_manager  # 38 — planner-side link protocol
bash ./run_tests.sh link_client   # 41 — robot-side link protocol
bash ./run_tests.sh kv_writer     # 16 — coalescing queue
```

Total: 202 assertions across 7 suites + kv_writer

---

## Next Session: Turn MQTT Robot into Real Robot

The `ros_planner_ii_mqtt_robot/` is currently a simulation — workers count down ticks
and report success. The robot needs to become a real robot that:

### What "real" means:
1. **Workers execute actual hardware commands** — not tick countdowns
   - `path_spline` → motor control, odometry, PID
   - `deliver_part` → arm servos, gripper
   - `inspection_scan` → sensor reads, data collection
   - `recharge` → dock with charger, monitor battery

2. **Sensor integration** — workers read actual sensors and report real bitmask/pose data
   - Distance sensor → obstacle detection in path workers
   - IMU → heading tracking in path_rotate
   - Color sensor → line following in path_line
   - Force sensor → gripper feedback in deliver_part

3. **Hardware abstraction** — separate hardware-specific code from protocol code
   - `remote_user_functions.lua` contains both protocol (ACK, heartbeat, completion)
     and hardware simulation (tick countdowns). Split these.
   - Controller functions (dispatch, watchdog, heartbeat, completion) are robot-independent
   - Worker functions are hardware-specific — different per robot class

4. **Target platform** — Pi Zero 2 W or ESP32
   - MQTT transport works on both
   - ChainTree DSL compiles to JSON, runs on LuaJIT
   - C robot alternative: `ros_planner_ii_c_cbor_robot/` (separate plan)

### Approach:
- Create a hardware abstraction layer in the MQTT robot
- Controller stays generic, workers become pluggable per robot class
- Start with simulated hardware (current behavior) as the "sim" driver
- Add real hardware drivers one VN at a time

---

## File Locations

```
ros_planner_ii/
  runtime/
    hub_runtime.lua         — state machine (no ChainTree), VN defs from KB
    link_manager.lua        — planner-side link protocol (URLP v1)
    link_client.lua         — robot-side link protocol
    mqtt_hub_transport.lua  — shared MQTT client, link message routing
    mqtt_transport.lua      — per-robot MQTT (JSON or CBOR), link send/recv
    kv_writer.lua           — coalescing KV queue
    queue_monitor.lua       — transport ↔ event bridge
  local_planner/lib/
    hub_runtime.lua         — state machine hub
    sequencer.lua           — route execution
    mission.lua             — telemetry
  action_server/lib/
    action_server.lua       — coroutine scheduler, link_manager, mission cancel
    mission_builder.lua     — route builder with capability validation
  global_planner/lib/
    global_planner.lua      — Dijkstra path planner
    route_builder.lua       — edge→VN translation, auto path_rotate
  hub_dsl/
    protocol/               — command_packets, event_ids, stream_packets, packet_mapper
    hub_functions/          — hub_control, event_handlers, error_recovery
    kb_construct/           — construct_surface_ops.lua, kb_query, kb_exporter, kb_runtime
  tests/                    — 5 integration suites + 2 link unit tests

ros_planner_ii_mqtt_robot/  — self-contained robot (base for containerization)
  mqtt_robot_main.lua       — production entry point with link_client
  remote_mqtt_ct.lua        — test harness with link_client
  mqtt_robot_config.lua     — config loader, MQTT status publisher
  remote_dsl.lua            — ChainTree DSL (controller + 12 workers)
  remote_user_functions.lua — all VN workers (simulation)
  capabilities.lua          — lunar_rover VN set (12 capabilities)
  build.sh                  — compile remote.json

ros_scripts/
  planner_server.lua        — persistent server, KB infra discovery
  bootstrap_container.lua   — container entrypoint
  start_server.sh           — host dev launcher
  start_robot.sh            — robot launcher
  submit_mission.sh         — mission client

third_party_containers/
  ros_planner/              — planner container (built, tested)
  kv_bridge/                — MQTT→NATS KV bridge (Go)
```

## Required Infrastructure
- NATS: `nats-js-ram` container, port from KB
- MQTT: `mosquitto-ram-ws_main` container, port from KB
- KV bridge: `kv-bridge` container
- SQLite DBs: built by `kb_dsl/scripts/master_build.lua`
