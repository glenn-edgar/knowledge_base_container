# ROS Planner II — Continuation Plan

## Session Summary (2026-04-05)

### What was accomplished

**1. MQTT-only architecture (complete)**
- All robot communication uses MQTT. NATS KV for external consumers only.
- Bridge, NATS robot, socket transport — all removed.

**2. Real-time hub tick path (complete)**
- Hub tick loop has zero NATS calls. All blocking KV writes removed.
- In-memory blackboard replaces ks_blackboard (no cross-process sharing needed).
- Status published via `kv_bridge/write` MQTT topic (fire-and-forget, microseconds).
- Wall-clock timeouts: 5s ack, 10s kb_done (os.time deadlines, not tick counters).

**3. kv-bridge Go container (complete)**
- Located: `third_party_containers/kv_bridge/`
- Go binary in alpine Docker image (~5MB, multi-stage build).
- Subscribes to `kv_bridge/write` MQTT topic, writes to NATS KV async.
- JSON envelope: `{"bucket":"...","key":"...","value":{...}}` (value is raw JSON passthrough).
- Supports delete: `{"bucket":"...","key":"...","op":"delete"}`.
- Bucket handles cached, 30s stats logging.
- `docker start kv-bridge` / `docker stop kv-bridge`

**4. Client/server split (complete)**
- Located: `ros_scripts/`
- `start_server.sh` — builds KB, exports to NATS, runs persistent planner.
- `start_robot.sh` — starts MQTT robot from config JSON.
- `submit_mission.sh` — submits mission JSON, polls NATS KV for result.
- `planner_server.lua` — persistent action_server (drain_nats=true, stays alive when idle).
- `mission_client.lua` — reads mission JSON, submits via NATS JobQueue, polls status/result.
- Example missions: `missions/deliver_sample.json`, `missions/full_circuit.json`.

**5. CBOR wire format end-to-end (complete)**
- Robot: `VMRT_WIRE_FORMAT=cbor` env var on `remote_mqtt_ct.lua` (no separate directory).
- Hub outbound: `send_rpc()` encodes to CBOR per robot's wire_format.
- Hub inbound: `poll()` decodes CBOR per robot's wire_format.
- Pre-set via `mqtt_hub:set_wire_format(robot_id, "cbor")` or learned from link handshake.
- 87/87 tests pass with CBOR, identical route to JSON test.

**6. Persistent action_server**
- `serve()` loop stays alive when `drain_nats=true` (was exiting on 0 missions).
- 50ms idle poll when no active missions.
- Results published to NATS KV by coroutine on completion.

---

## Tests: Current State

```bash
cd ros_planner_ii/tests

./run_tests.sh all            # 351 assertions, 0 failures

# Individual suites:
./run_tests.sh planner        # 60/60 — graph, routing, KB schema
./run_tests.sh mqtt_direct    # 87/87 — planner → MQTT (JSON) → robot
./run_tests.sh mqtt_cbor      # 87/87 — planner → MQTT (CBOR) → robot
./run_tests.sh hub_rt         # 87/87 — hub_runtime API + mission telemetry
./run_tests.sh sequencer      # 16/16 — route execution + telemetry
./run_tests.sh action         # 14/14 — mission builder + coroutine + direct
./run_tests.sh kv_writer      # 16/16 — coalescing queue (unit)
./run_tests.sh link_manager   # 25/25 — link state machine (unit)
```

---

## Next Session Priorities

### 1. ChainTree C robot (ros_planner_ii_c_cbor_robot/)
Pi Zero 2 target. CBOR wire format, MQTT transport. Directory exists but is empty.

### 2. Thread bridge
Simulated loopback first, then CBOR/SLIP over serial, then real hardware.

### 3. Production hardening
- Recharge scheduler
- Dashboard integration (reads NATS KV via kv-bridge)
- Error recovery refinement

---

## File Locations

```
ros_planner_ii/
  runtime/
    mqtt_hub_transport.lua  — shared MQTT client, poll decodes CBOR, publish_kv for bridge
    mqtt_transport.lua      — per-robot MQTT (used by robot subprocess, CBOR support)
    kv_writer.lua           — coalescing KV queue (used by link_manager, not tick path)
    link_manager.lua        — URLP v1 state machine
    queue_monitor.lua       — bridges transport ↔ ChainTree events
  local_planner/lib/
    hub_runtime.lua         — in-memory blackboard, MQTT auto-poll, zero NATS
    sequencer.lua           — accepts mqtt_hub pass-through
  action_server/lib/
    action_server.lua       — persistent serve(), mqtt_hub pass-through
  hub_dsl/hub_functions/
    event_handlers.lua      — wall-clock timeouts (os.time deadlines)
  robots/test_robot/
    remote_mqtt_ct.lua      — test robot (JSON or CBOR via VMRT_WIRE_FORMAT)
  tests/
    test_mqtt_direct.lua    — JSON integration (87/87)
    test_mqtt_cbor.lua      — CBOR integration (87/87)
    test_hub_runtime.lua    — hub_runtime + mission (87/87)
    test_sequencer.lua      — sequencer route (16/16)
    test_action_server.lua  — action server (14/14)
    test_global_planner.lua — planner (60/60)
    test_kv_writer.lua      — KV writer unit (16/16)
    test_link_manager.lua   — link manager unit (25/25)
    run_tests.sh            — test runner

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

third_party_containers/
  kv_bridge/                — Go MQTT→NATS KV bridge container
    main.go, Dockerfile, docker_build.sh, docker_run.sh
```

## Required Infrastructure
- NATS server: `nats://127.0.0.1:4222` (Docker: nanodatacenter/nats-js-ram)
- MQTT broker: `localhost:1883` (Docker: nanodatacenter/mosquitto-ram-ws)
- KV bridge: (Docker: nanodatacenter/kv-bridge)
