# ChainTree C Robot — Continue From Here

## Session Summary (2026-04-04, evening)

### Single CBOR buffer optimization
Replaced `cbor_inject_buf[MAX_POLL_MSGS][CBOR_SLOT_SIZE]` (8×128 = 1KB) with a
single 128-byte buffer. Instead of injecting all MQTT messages then processing,
we now inject one message and drain all ChainTree events before reusing the
buffer. Works because `cfl_send_streaming_data_event` stores a pointer (not a
copy) — the buffer must stay valid until consumed.

### Wall-clock timer
Replaced fixed-step `delta_time = 0.01` with real `clock_gettime(CLOCK_MONOTONIC)`.
- `handle->delta_time` is actual elapsed seconds since last tick
- `handle->future_time_stamp` is monotonic wall-clock time
- Pacing: `nanosleep(10ms)` + non-blocking `mqps_poll(timeout=0)`
- Makefile: `_POSIX_C_SOURCE` bumped to `200809L`, removed per-file defines

### rob_publish one-shots (rob_ prefix pattern established)
Created `src/rob_publish.c/.h` — generic robot publish functions as ChainTree
one-shot nodes. Each reads blackboard state and publishes via `g_robot_ctx`.

**Implemented:**
- `rob_send_ack_fn` — reads `current_seq`/`current_test_id` from blackboard,
  publishes ack on stream_bus. Used as first node in every worker KB.
  Removed inline `robot_mqtt_send_ack` call from `cbor_rpc_dispatch_fn`.
- `rob_publish_state_fn` — publishes status/state retained (connected=true)
- `rob_publish_energy_fn` — publishes status/energy retained
- `rob_publish_bitmask_fn` — publishes status/bitmask retained

**New DSL KB: `robot_init`**
Runs once at startup, activated by main.c alongside controller:
```
ROB_PUBLISH_STATE → ROB_PUBLISH_ENERGY → ROB_PUBLISH_BITMASK → log → terminate
```
Removed the three `robot_mqtt_publish_*` calls from main.c.
Shutdown `publish_state(connected=false)` stays in main.c cleanup (ChainTree
already torn down at that point).

### All tests pass
- `./test_c_robot.sh json` — 12/12
- `./test_c_robot.sh cbor` — 12/12
- Zero warnings, zero new dependencies

### Current DSL structure
```
robot_init  (7 nodes)   — startup status publish, runs once
controller  (4 nodes)   — CBOR sink dispatch + completion monitor
worker_*    (9 nodes ea) — ROB_SEND_ACK → log → wkr_init → wait → log → WORKER_TERM → terminate
```
14 KBs total (robot_init + controller + 12 workers), 134 operational nodes.

---

## Next Session: Continue rob_ refactoring

### 1. rob_send_heartbeat one-shot
Move heartbeat publishing from `cbor_rpc_dispatch_fn` (initial heartbeat) and
`ctrl_completion_main_fn` (final heartbeat) into DSL one-shots.

Needs a blackboard field for phase ("initial"/"final") or two separate one-shots:
- `rob_send_heartbeat_initial_fn` — called in dispatch after worker activation
- `rob_send_heartbeat_final_fn` — called in completion

The initial heartbeat also sets `state->worker.*` fields (active, packet_type,
test_id, seq, elapsed, start_pose). Consider whether that state setup belongs
in the one-shot or stays in dispatch.

### 2. rob_send_kb_done one-shot
Move `robot_mqtt_send_kb_done` from `ctrl_completion_main_fn` into a DSL
one-shot. Reads blackboard `worker_success`, `fault_code`. Also publishes
energy after done.

### 3. Periodic bitmask/energy from DSL
Currently in main.c tick counter (`tick_count % BITMASK_TICKS`). Could move
to a dedicated `robot_periodic` KB with a timer-driven main function that
publishes on interval. Lower priority — the main.c approach works fine.

### 4. user_energy_deduct
Move `robot_state_deduct_energy` from `ctrl_completion_main_fn` into a
`user_energy_deduct_fn` one-shot. This is class-specific (lunar_rover energy
cost table) — uses `user_` prefix, not `rob_`.

### 5. Simplify main.c
After steps 1–4, `ctrl_completion_main_fn` becomes just: check `worker_done`
blackboard flag, deactivate worker KB, reset blackboard. All protocol messages
are DSL one-shots. `cbor_rpc_dispatch_fn` becomes: parse packet_type, set
blackboard, activate worker KB. No direct MQTT calls remain in user functions.

main.c becomes:
- Load config, init MQTT, init ChainTree
- Activate robot_init + controller KBs
- Loop: nanosleep → poll → inject → tick → check shutdown
- Cleanup (publish disconnected state)

---

## Build commands
```bash
# Recompile DSL (from ros_planner_ii_robot directory)
make dsl

# Build robot
make clean && make

# Run tests
./test_c_robot.sh json    # 12/12
./test_c_robot.sh cbor    # 12/12
```

## File inventory
```
src/
  main.c                ← generic loop, no protocol knowledge except shutdown publish
  rob_publish.c/.h      ← rob_ one-shots (ack, state, energy, bitmask)
  ct_user_functions.c/.h ← dispatch boolean, completion main, worker inits
  robot_mqtt.c/.h       ← MQTT transport + protocol message builders
  robot_state.c/.h      ← energy, pose, worker lifecycle
  robot_config.c/.h     ← JSON config loader
  robot_context.h       ← global context (mqtt + state)
  robot_protocol.h      ← packet types, energy costs, durations
  json_extract.c/.h     ← field extraction from payloads
dsl/
  robot_mqtt_dsl.lua    ← 14 KBs (robot_init + controller + 12 workers)
  continue.md           ← this file
libs/
  mqtt_pubsub.c/.h      ← ring buffer pub/sub
  cbor_codec.c/.h       ← JSON↔CBOR codec
```
