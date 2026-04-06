# Testing

## Prerequisites

- NATS container running (`nats-js-ram`, port 4222)
- MQTT container running (`mosquitto-ram-ws_main`, port 1883)
- KV bridge container running (`kv-bridge`)

## Running Tests

```bash
cd ros_planner_ii/tests

# All integration suites
bash ./run_tests.sh all

# Individual suites
bash ./run_tests.sh planner       # 60 — graph, routing, KB schema (no MQTT)
bash ./run_tests.sh mqtt_direct   # 23 — JSON wire, 5-stop mission from moon map
bash ./run_tests.sh mqtt_cbor     # 11 — CBOR wire, same mission
bash ./run_tests.sh sequencer     # 15 — sequencer API, 3-stop with recharge
bash ./run_tests.sh action        # 14 — action server, coroutine + direct

# Unit tests (run separately, no MQTT needed)
bash ./run_tests.sh link_manager  # 38 — planner-side link protocol
bash ./run_tests.sh link_client   # 41 — robot-side link protocol
bash ./run_tests.sh kv_writer     # 16 — coalescing queue
```

## What Each Suite Tests

### planner
Global planner unit test. Validates Dijkstra pathfinding, route building, mission builder capability checks, energy validation. No external dependencies beyond SQLite KB.

### mqtt_direct
Full end-to-end: plans a 5-stop mission against the landing_zone board, executes via MQTT (JSON), validates pose accumulation, NATS telemetry (KeyStore status, JetStream stream). Robot registers via link protocol.

### mqtt_cbor
Same as mqtt_direct but with CBOR wire format. Validates transparent CBOR encoding/decoding.

### sequencer
Plans a 3-stop mission (habitat, recharge at charging_station, return). Validates sequencer API, route validation, mission telemetry.

### action
Tests mission_builder (route planning), coroutine scheduler (mission via serve()), and direct execution (execute_mission()). Robot registers via link protocol.

### link_manager / link_client
Unit tests with mock transport. No MQTT/NATS needed. Tests: handshake, re-announce exception, heartbeat keepalive, planner restart detection, clean disconnect, shutdown.

## Test Architecture

Integration tests start a real MQTT robot subprocess (`remote_mqtt_ct.lua`). The test acts as the planner. Robot registration happens via link protocol (`test_link_helper.lua`). All missions are planned by the global planner against the actual moon map — no hardcoded routes.
