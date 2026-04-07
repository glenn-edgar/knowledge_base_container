# ROS Planner II — Continuation Plan

## Session Summary (2026-04-06/07)

### 1. Robot Controller Refactor (complete)
- Collapsed 4 controller DSL columns (dispatch, watchdog, heartbeat, completion) into single `robot_controller.lua`
- Controller runs directly from tick loop — no ChainTree dependency
- Watchdog changed from tick-counting to liveness-based (workers set `bb.worker_alive = true`)
- Controller KB removed from DSL entirely (137 → 125 nodes)
- `queue_monitor` no longer used on robot side
- `remote_user_functions.lua` is workers-only, each with individual DSL definition matching KB VN schema
- Each worker prints incoming command JSON on init

### 2. Unified KB Build (complete)
- `master_build.lua` builds planner data (boards, VNs, robot classes) into Postgres via `planner_tree.lua`
- `surface_ops_planner_data.lua` is single source of truth for all planner data
- `sqlite_extract.lua` builds planner data into domain SQLite via CDT (creates status/stream/bitmask tables)
- One DB per domain: system KB + subsystems KB + planner KB in one file
- `site_config.lua` `planner_data` field drives which domains get planner data

### 3. Container Networking (complete)
- `planner-net` Docker network for container-to-container communication
- MQTT/NATS reached by container name (Docker DNS): `mosquitto-ram-ws_main`, `nats-js-ram`
- Fixes WSL2 `--network host` limitation
- `site_config.lua` uses `127.0.0.1` instead of `0.0.0.0`
- `mqtt_hub_transport.lua` handles async MQTT connect (pcall + is_connected poll)
- SQLite DB mounted read-write (planner writes status updates)

### 4. Production Hardening (complete)
- `start_planner_system.sh` orchestrator: start/stop/restart/status/build
- kv-bridge on `planner-net` (was `--network host`, couldn't reach other containers)
- NATS export retry (3 attempts, 2s delay)
- Debug prints removed from mqtt_hub_transport connect flow
- Energy: action server reads from link_manager (live robot state), not stale NATS KV
- `docker_build.sh` rebuilds runtime KB during staging, verifies boards exist

---

## Tests: Current State

```bash
cd ros_planner_ii/tests
bash ./run_tests.sh mqtt_direct    # 23 assertions, 0 failures
```

## How to Run

### Full System (Container)
```bash
cd ~/knowledge_base_assembly/luajit_programs_and_containers/building_blocks

# Start everything (network, KB build, kv-bridge, planner container)
bash start_planner_system.sh start

# Check status
bash start_planner_system.sh status

# Start robot (Terminal 2)
bash ros_scripts/start_robot.sh ros_planner_ii_mqtt_robot/rover_1_config.json

# Submit mission (Terminal 3, after robot shows "live")
bash ros_scripts/submit_mission.sh ros_scripts/moonbase_mission.json

# Stop everything
bash start_planner_system.sh stop
```

### Non-Container (Dev Mode)
```bash
# Terminal 1: Planner server
bash ros_scripts/start_server.sh

# Terminal 2: Robot
bash ros_scripts/start_robot.sh ros_planner_ii_mqtt_robot/rover_1_config.json

# Terminal 3: Mission
bash ros_scripts/submit_mission.sh ros_scripts/moonbase_mission.json
```

---

## Next Session: Two-Prong Approach

### Prong 1: Web UI via OpenResty

Connect the existing OpenResty web gateway to the planner's NATS API.
Display real-time data from the external interface (`docs/api/README.md`):

1. **Fleet Summary** — watch `{site}.action_server.summary` in NATS KV
   - Active missions, registered robots, per-robot state
2. **Mission Status** — poll `{site}.action_server.{robot_id}.status`
   - Planning → executing → completed/failed
3. **Mission Result** — read `{site}.action_server.{robot_id}.result`
   - Success, completed/total, elapsed, pose, fault
4. **Robot Status** — watch `{site}.robots.{robot_id}.status.link`
   - Link state, wire format, energy, heartbeat
5. **Telemetry Stream** — subscribe `{site}.robots.{robot_id}.stream.telemetry`
   - action_start, heartbeat, action_complete, mission_complete
6. **Board Viewer** — read `{site}.boards.{name}` from `kb_export`
   - Node positions, edges, current robot positions
7. **Mission Launcher** — submit to `{site}.action_server.missions` job queue

OpenResty connects to NATS via lua-resty-nats or a NATS WebSocket bridge.
Frontend: minimal HTML/JS using Server-Sent Events or WebSocket for live updates.

### Prong 2: LuaJIT Robot Transform

Turn the simulated MQTT robot into a real robot:

1. **Worker driver interface** — each worker is a pluggable module
   - `sim` driver: current tick-countdown behavior (test/dev)
   - `hw` driver: real hardware (motor control, sensors, arm servos)
   - Selected by robot config: `"driver": "sim"` or `"driver": "hw"`

2. **Split remote_user_functions.lua**
   - `workers/sim/` — simulated workers (current code)
   - `workers/hw/` — real hardware workers (one per VN)
   - Worker loader reads driver from config, requires the right module

3. **Hardware abstraction per VN**
   - `path_spline` → motor control, odometry, PID loop
   - `path_rotate` → IMU heading tracking, turn-in-place
   - `deliver_part` → arm servo, gripper, force sensor
   - `inspection_scan` → sensor read, data collection
   - `recharge` → dock detection, battery monitor

4. **Target platform** — Pi Zero 2 W
   - LuaJIT + MQTT transport (same as simulation)
   - GPIO/I2C/SPI via LuaJIT FFI
   - Worker drivers are the only platform-specific code

---

## File Locations

```
ros_planner_ii/
  runtime/
    mqtt_hub_transport.lua  — async MQTT connect, planner-net compatible
    link_manager.lua        — planner-side link protocol, get_energy()
    link_client.lua         — robot-side link protocol
    kv_writer.lua           — coalescing KV queue
  local_planner/lib/
    hub_runtime.lua         — state machine hub (site param)
    sequencer.lua           — route execution (site param)
  action_server/lib/
    action_server.lua       — energy from link_manager, site param
  global_planner/lib/
    global_planner.lua      — Dijkstra (site param)
  hub_dsl/kb_construct/
    kb_query.lua            — optional site param to constructor

ros_planner_ii_mqtt_robot/
  robot_controller.lua      — robot-independent controller (no ChainTree)
  remote_user_functions.lua — workers only, individual VN definitions
  remote_dsl.lua            — workers only, no controller KB
  rover_1_config.json       — robot config for start_robot.sh

ros_scripts/
  planner_server.lua        — single DB, site param
  bootstrap_container.lua   — NATS export retry, no runtime KB build
  start_server.sh           — non-container planner
  start_robot.sh            — robot launcher
  submit_mission.sh         — mission client
  moonbase_mission.json     — 5-stop moon base mission

kb_dsl/scripts/
  master_build.lua          — Postgres + planner tree + SQLite extract
  planner_tree.lua          — planner data in Postgres (wrapper)
  surface_ops_planner_data.lua — planner data builder (CDT API)
  sqlite_extract.lua        — system/subsystems extract + CDT planner build
  site_config.lua           — 127.0.0.1 hosts, planner_data field

start_planner_system.sh     — system orchestrator (start/stop/restart/status/build)

third_party_containers/
  ros_planner/
    docker_build.sh         — rebuilds runtime KB, verifies boards
    docker_run.sh           — planner-net, container DNS, read-write mount
  kv_bridge/
    docker_run.sh           — planner-net, container DNS
```

## Required Infrastructure
- Postgres: `pg-vector` container (bridge network)
- NATS: `nats-js-ram` container (bridge + planner-net)
- MQTT: `mosquitto-ram-ws_main` container (bridge + planner-net)
- KV bridge: `kv-bridge` container (planner-net)
- Web gateway: `openresty-gateway` container (bridge, port 8080)
