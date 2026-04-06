# ROS Planner II

LuaJIT robot mission planner with MQTT transport, KB-driven virtual nodes, and coroutine-based multi-robot scheduling.

## What It Does

- Plans multi-stop missions across a waypoint graph (Dijkstra shortest path)
- Executes missions via MQTT to remote robots (JSON or CBOR wire format)
- Manages robot registration and liveness via link protocol (URLP v1)
- Runs multiple concurrent missions using LuaJIT coroutines
- Publishes telemetry to NATS KV and JetStream for external consumers

## Key Design Principles

- **KB is single source of truth** — virtual node definitions, board graphs, robot capabilities, infrastructure connection details all come from the SQLite knowledge base
- **No hardcoded defaults** — NATS/MQTT host:port and site name come from KB infrastructure queries
- **Robot capabilities are authoritative** — the robot announces what it supports via link protocol; the planner validates missions against actual capabilities
- **Adding a new virtual node requires zero planner code changes** — add it to the KB, add a worker to the robot, done

## Architecture Summary

```
Mission Client                    Planner Container
    |                                 |
    |-- submit mission (NATS) ------->|
    |                                 |-- global_planner (Dijkstra)
    |                                 |-- route_builder (edge → VN)
    |                                 |-- mission_builder (capability check)
    |                                 |-- sequencer (execute route)
    |                                 |     |
    |                                 |     |-- hub_runtime (state machine)
    |                                 |     |     |
    |                                 |     |     |-- send command (MQTT)
    |                                 |     |     |-- wait ACK
    |                                 |     |     |-- wait KB_DONE
    |                                 |     |     |-- apply pose delta
    |                                 |     |
    |                                 |     +-- next action...
    |                                 |
    |<-- result (NATS KV) -----------|

MQTT Robot
    |
    |-- link_announce (MQTT) -------> Planner
    |<-- link_bridge_ack ------------|
    |-- link_confirm --------------->|  (registered, live)
    |
    |<-- RPC command (MQTT) ---------|
    |-- ACK ------------------------>|
    |-- heartbeats ----------------->|
    |-- KB_DONE -------------------->|
```

## Quick Start

```bash
# Prerequisites: NATS, MQTT, kv-bridge containers running
# SQLite DBs built: cd kb_dsl/scripts && luajit master_build.lua

# Run tests
cd ros_planner_ii/tests
bash ./run_tests.sh all

# Start planner server (host dev)
cd ros_scripts && ./start_server.sh

# Start planner container
cd third_party_containers/ros_planner
./docker_build.sh
./docker_run.sh surface_ops

# Submit a mission
cd ros_scripts && ./submit_mission.sh missions/deliver_sample.json
```

## Test Suites

| Suite | Assertions | What it tests |
|-------|-----------|---------------|
| planner | 60 | Graph, routing, KB schema |
| mqtt_direct | 23 | JSON wire, 5-stop mission from moon map |
| mqtt_cbor | 11 | CBOR wire, same mission |
| sequencer | 15 | Sequencer API, 3-stop mission with recharge |
| action | 14 | Action server, coroutine + direct execution |
| link_manager | 38 | Planner-side link protocol |
| link_client | 41 | Robot-side link protocol |
