# ROS Planner II — Continuation Plan

## Session Summary (2026-04-01)

Built the complete planner stack bottom-up in one session:

| Layer | Module | Tests |
|-------|--------|-------|
| Protocol | command_packets, stream_packets, event_ids, packet_mapper | via nats_ct 72/72 |
| Hub DSL | 12 KB plugins (slim, code-only), event handlers | via nats_ct 72/72 |
| Remote | Controller + 11 dormant worker KBs | via nats_ct 72/72 |
| Transport | NATS JobQueue (two queues per robot) | 32/32 |
| Hub Runtime | hub_runtime.lua (extracted from test) | 87/87 |
| Mission Telemetry | JetStream KV stream + KeyStore status + SQLite bookends | via hub_rt |
| Sequencer | KB-driven, replan-ready (new/load_route/run) | 16/16 |
| Global Planner | Dijkstra, bidirectional, blocked edges, replan | 60/60 |
| Action Server | Coroutine scheduler, multi-stop missions, replan loop | 14/14 |

**Key architecture decisions made today:**
- KB is single source of truth for VN definitions (hub plugins are code-only)
- Coroutine scheduler for 100 concurrent robots (no threads, no fork)
- Instance-based hub_control (no shared global pose between coroutines)
- NATS subjects = SQLite KB paths (one namespace, two backends)
- KB exporter: SQLite → NATS KeyStore at startup (30 keys)
- comm_type in KB robot connections (nats/thread/wifi/mqtt)
- Replan like car navigation: blocked edges → Dijkstra reroute or full replan
- FNV-1a C library built (thread_bridge/fnv1a/)

---

## Next Session: Thread Bridge + Loopback Robot

### Architecture Principle

**Robots are independent processes.** The planner stack does NOT launch or manage robots. It talks to NATS. Robots connect on their own, register themselves, and wait for commands. The planner stack will eventually be a container — pure logic, no hardware dependencies.

**The NATS bridge is also independent.** Discovers robots, translates protocols, routes messages. Doesn't care if the planner is running.

**Startup order doesn't matter.** Everything discovers each other via NATS KV.

### Build Order (bottom-up, each step testable independently)

#### Step 1: Simulated Thread Robot (standalone process)

Standalone LuaJIT process. Speaks JSON over a NATS "temp channel" that simulates the serial link to the bridge.

```
thread.bridge.serial.tx  → bridge sends to robot
thread.bridge.serial.rx  → robot sends to bridge
```

Messages use the same packet structure as real serial (msg_type, robot_id, payload) — just JSON not CBOR/SLIP.

- Receives commands, sends acks, sends sensor events
- Registers itself in NATS KV with EUI-64 identity
- Lives at: `building_blocks/thread_bridge/sim_robot/`

#### Step 2: Bridge Process (standalone)

Standalone LuaJIT process. Coroutine-based (like action server).

- Reads robot registry from NATS KV (discovers robots by comm_type="thread")
- Routes between planner NATS topics and temp channel NATS topics
- Translates planner protocol (JSON commands/events) ↔ robot protocol (msg_type + robot_id)
- Lives at: `building_blocks/thread_bridge/bridge/`

#### Step 3: End-to-End Loopback Test

All three processes launched independently:
1. Simulated Thread robot connects to NATS, registers
2. Bridge connects to NATS, discovers robot
3. Planner submits mission via action server

Test verifies: mission command → bridge → temp channel → simulated robot → ack/events → bridge → planner → mission complete.

#### Step 4: Add CBOR Codec

Replace JSON in temp channel with CBOR (integer FNV-1a hash keys). Bridge translates JSON↔CBOR using field registry. Robot speaks CBOR.

- Field registry: `thread_bridge/lib/field_registry.lua` (hash↔name from KB)
- CBOR codec: `thread_bridge/lib/cbor_codec.lua` (pure LuaJIT, small subset)
- FNV-1a C library already built: `thread_bridge/fnv1a/libfnv1a.so`

#### Step 5: Add SLIP Framing

Wrap CBOR packets in SLIP frames. Bridge does SLIP encode/decode. Still over NATS temp channel (artificial but tests the codec).

- SLIP codec: `thread_bridge/lib/slip_codec.lua`

#### Step 6: Real Serial

Swap NATS temp channel for serial port FFI (`/dev/ttyUSB0`). Bridge reads/writes SLIP frames over serial. Simulated robot replaced by real Zephyr dongle + Thread mesh nodes.

- Serial port: `thread_bridge/lib/serial_port.lua` (FFI termios)

---

## File Locations

```
ros_planner_ii/               ← planner stack (containerizable)
  action_server/lib/          ← action_server.lua, mission_builder.lua
  global_planner/lib/         ← dijkstra.lua, route_builder.lua, global_planner.lua
  local_planner/lib/          ← hub_runtime.lua, mission.lua, sequencer.lua
  hub_dsl/                    ← KB plugins, protocol, hub_functions, kb_construct
  robots/test_robot/          ← test robot (NATS direct, not Thread)
  runtime/                    ← vmrt, nats_transport, queue_monitor
  tests/                      ← all test suites

thread_bridge/                ← Thread mesh bridge (separate process)
  fnv1a/                      ← libfnv1a.so (built, verified)
  lib/                        ← fnv1a.lua FFI wrapper (done)
                              ← field_registry.lua (TODO)
                              ← cbor_codec.lua (TODO)
                              ← slip_codec.lua (TODO)
                              ← serial_port.lua (TODO)
  sim_robot/                  ← simulated Thread robot (TODO)
  bridge/                     ← bridge process (TODO)
```

## How to Run Tests

```bash
cd ros_planner_ii/tests
./run_tests.sh planner     # 60/60 — Dijkstra, route builder, VN KB, exporter
./run_tests.sh action      # 14/14 — coroutine scheduler + direct execution
./run_tests.sh sequencer   # 16/16 — KB-driven route execution
./run_tests.sh hub_rt      # 87/87 — hub runtime + mission telemetry
./run_tests.sh nats_ct     # 72/72 — original full stack regression
./run_tests.sh nats        # 32/32 — NATS loopback
./run_tests.sh all         # everything
```

## Commits Today

```
dc55ad80 Action server (single-threaded)
2aaa1778 Coroutine scheduler
52280d74 KB capabilities, pose reset, comm_type, cleanup
40e52639 VN KB definitions, FNV-1a, KB exporter
678d3cf7 KB single source of truth, slim plugins
625b2080 hub_control instance bug fix for multi-robot coroutines
```
