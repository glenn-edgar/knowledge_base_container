# continue.md — 2026-05-01 session handoff (L1+L2 landed)

## What landed this session

### Track A — portability refactor — DONE

Committed in `24f19a5c` (commit message names nano_data_center work due
to a parallel-terminal commit-a mixup — the actual contents are Track A
plus two unrelated nano_data_center file edits; not destructive, just
untidy in the log).

- `libcomm/bus_config.h` — central #define site for tunable sizes;
  per-target overrides via `-D`. Forward-looking entries (LOGICAL_ROBOT_MAX,
  MSGQ_DEPTH_DEFAULT, BUS_THREAD_STACK_BYTES) included.
- `libcomm/bus_kernel.h` — portable thread/msgq/timer/mutex interface;
  storage-opaque blobs sized for pthread + Zephyr worst case.
- `libcomm/bus_kernel_linux.c` — pthreads backend, all four primitives.
- `libcomm/bus_kernel_zephyr.c` — concrete skeleton (no TODOs); guarded
  by `#ifndef __ZEPHYR__ #error` so the Linux Makefile cannot
  accidentally build it.
- `libcomm/ext_bus.h` + `libcomm/ext_bus_linux_pty.c` — 3-fn driver
  contract (tx/rx/rx_wait); per-silicon swap point. transport_uart.c
  now sits *above* `ext_bus_t` and uses `ext_bus_tx/rx`.
- `libcomm/test_bus_kernel.c` (29 checks) + `libcomm/test_ext_bus_contract.c`
  (14 checks) wired into `run_tests.sh`.
- Bonus fix: pre-existing Makefile `$(ROBOT_SIM_TARGET)` forward-reference
  bug — `make clean && make` no longer skips robot_sim.

**Validation:** `bash run_tests.sh --skip-e2e` → 227 unit checks green
(was 184; +29 bus_kernel, +14 ext_bus contract). 10/10 N=100
multi-dongle stress runs, no flakes.

### Track C — design contracts — LOCKED

All five questions answered. Memory: `project_dongle_track_c.md`.

| # | Decision | Lock |
|---|---|---|
| Q1 | Internal-bus message format | 40-B `bus_msg_t` envelope, 32-B inline payload, no internal acks, no robot-to-robot peer queues. Manager owns 2 ~10-LoC translators (`frame_meta_t ↔ bus_msg_t`) at the ext-bus boundary. |
| Q2 | Logical-robot lifecycle | Long-lived `bus_thread` per robot, vtable + tick sentinel via `bus_timer`. Sentinels reserved at `dst_robot=0xFF`. |
| Q3 | Identity storage | Zephyr settings + NVS for identity AND per-robot tunables (PID, calibration). Schema-versioned. Robot_sim mirrors via JSON file. |
| Q4 | External-bus speed | 921.6 kbps default / 100 msg/s sustained / 250 burst / 10 ms e2e. RX 4 KB / TX 2 KB DMA rings. 400 kbps documented override band. |
| Q5 | Logical-robot count v1 | `LOGICAL_ROBOT_MAX = 8`, 2 primary + 6 aux split. ~25 KB worst-case RAM per dongle. |

## What this enables

Tracks A+C let us build the **four-thread dongle decomposition on Linux**
inside `robot_sim` BEFORE touching real hardware. Once that works,
Track B (Pico bring-up) becomes mechanical because the architecture is
already validated.

## Plan of action — next sessions

Pivot from "Track B next" to **Linux logical_robot slices L1-L5**:
build the four-thread dongle shape on Linux, run the existing mission
planner against it, prove the architecture e2e on Linux. Track B (real
Pico hardware) is **deferred** until L1-L5 are green.

### Slice L1 — bus_msg.h + sentinels — DONE

- bus_config.h gained BUS_MSG_INLINE_PAYLOAD_MAX=32,
  BUS_MSGQ_DEFAULT_DEPTH=16, LOGICAL_ROBOT_PRIMARY_MAX=2,
  LOGICAL_ROBOT_AUX_MAX=6, sentinel constants.
- libcomm/bus_msg.h: bus_msg_t (40 B locked) + sentinel helpers,
  static-asserted invariants (size, payload, header offset,
  sentinel-outside-robot-space).
- libcomm/test_bus_msg.c: 23 checks (size invariants, sentinel
  helpers, real-msg-not-sentinel, msgq round-trip with 0/5/32-byte
  payloads).

### Slice L2 — logical_robot lifecycle — DONE

- libcomm/logical_robot.h: vtable (init/on_msg/tick/shutdown/
  tick_period_ms) + handle struct + 4-fn API.
- libcomm/logical_robot.c: generic logical_robot_entry — calls init,
  starts tick timer if tick_period_ms>0, blocks on
  bus_msgq_get(FOREVER), dispatches sentinels (tick / shutdown) and
  real messages, drains + calls shutdown on exit.
- libcomm/test_logical_robot.c: 16 checks (basic lifecycle,
  on_msg ordering, sentinel rejection, tick fires, inbox-full
  backpressure).
- ships in libcomm.so; ready for L3+.

### Slice L3 — drive_base logical_robot wired to physics_core

physics_core.c already has a clean transport-portable C API
(`physics_pipe.h`): 64-byte cmd blocks (CMD_PUSH_LINE, CMD_PUSH_SPLINE,
CMD_PUSH_ROTATE, CMD_BEGIN_GRIP, CMD_BEGIN_DOCK, CMD_SET_PARAM, …)
flowing chain_tree→physics, 256-byte up blocks (events + telemetry)
flowing physics→chain_tree, two SPSC ring buffers. The header
explicitly anticipates "in-process today, shared-memory next,
SLIP-framed serial later."

CBOR/JSON wire-format flag in `rover_1_*config.json` is about the
MQTT bridge between ros_planner_ii and the robot Lua process; it's
above libcomm and never touches drive_base. Earlier draft of this
plan mistakenly called physics_core JSON-shaped — corrected
2026-05-01.

- New `robot_sim/drive_base_robot.c`:
  - `drive_base_init`: phys_create() (or whatever physics_core calls
    its constructor), load tunables, push CMD_SET_PARAM blocks.
  - `drive_base_on_msg`: translate bus_msg_t (40 B) → physics_pipe
    cmd block (64 B). ~10 LoC adapter; both shapes are already
    designed for this. Uses drive_base catalogue from
    project_drive_base_catalogue (line + B-spline subset; streaming
    deferred).
  - `drive_base_tick`: drain up-FIFO of physics_core, emit each
    event/telemetry as a bus_msg_t upward to the manager's outbound.
  - `drive_base_shutdown`: phys_destroy().
- Tunables: PID constants + max velocity + max acceleration. JSON
  file in Linux mode (mirrors Zephyr settings/NVS on embedded per Q3).
- **Done when:** unit tests verify a "MOVE_LINE" command via
  bus_msg_t produces a CMD_PUSH_LINE in physics_pipe and the physics
  state updates.
- **Estimated scope:** ~250-400 LoC drive_base_robot.c + ~100 LoC test.
  ~1 session, not 1-2.

### Slice L4 — robot_sim restructure: four threads + manager wiring

- `robot_sim/main.c` shrinks to argv parsing + spawning four threads.
- `robot_sim/external_bus.c` — high-prio thread reading pty master,
  feeding frame decoder, posting decoded frames to manager's inbox.
- `robot_sim/dongle_manager.c` — HELLO/IDENT (existing) + commissioning
  state machine (Q3) + frame→bus_msg translation + routing + reverse
  path (events → s2m frames → ext_bus tx).
- `robot_sim/internal_bus.c` — fan-out worker: reads single fan-out
  queue, dispatches to per-robot inboxes by `dst_robot`.
- drive_base logical_robot from L3 starts inside this shape.
- **Done when:** existing pty multi-dongle tests still green at N=100;
  new e2e test issues a drive_base command via libcomm and observes
  physics state change.

### Slice L5 — wire ros_planner_ii through to drive_base

- Replace `mqtt_robot_main.lua`'s direct `physics_core` calls with
  `comm_submit(drive_base_cmd)` through libcomm: pty → robot_sim →
  drive_base → physics_core.
- Existing `test_random_paths.lua` MQTT scenarios become the e2e
  validator for the whole stack.
- **Done when:** `bash run_tests.sh` (no --skip-e2e) runs full MQTT
  scenarios and they pass through the new architecture.

## How to resume

```bash
cd /home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/ros_planner_ii_mqtt_robot
make
bash run_tests.sh --skip-e2e   # 266 checks should be green (227 prior + 23 L1 + 16 L2)
```

Quick stress sanity:

```bash
for i in 1 2 3 4 5; do luajit test_comm_pty_multi_dongle.lua 2>/dev/null | grep summary; done
# expect: 5x "[summary] 18 passed, 0 failed"
```

Open with **Slice L3** — drive_base logical_robot wired to
physics_core via physics_pipe.h. The physics_core C API is already
transport-portable (64-byte cmd blocks + 256-byte up blocks), so no
JSON/CBOR parsing in libcomm. ~1 session.

## Track B (real Pico hardware) — DEFERRED

continue.md's prior Track B plan (Zephyr workspace, RP2040 UART driver,
two-Pico interop) is correct but waits until L1-L5 prove the
architecture on Linux. Reasons:

- L1-L5 use the exact same `bus_kernel.h` / `bus_msg_t` / vtable
  contracts that Track B will use → all the design risk is taken
  before any hardware time.
- Track B needs ordered hardware (Pico + Pi Debug Probe) and a Zephyr
  workspace setup (~2-3 GB toolchain).
- USB passthrough on WSL2 is flakier than native; user may want to
  run Track B from a different machine.

Track B is unblocked the moment L1-L5 are green.

## Track C follow-ups deferred to Track B

- Q3 commissioning protocol (which catalogue cmd, exact framing of
  identity/tunables write).
- Q4 motor/sensor bus contract (per-logical_robot, per-silicon).

Both surface naturally at Track B time. Not blockers for L1-L5.
