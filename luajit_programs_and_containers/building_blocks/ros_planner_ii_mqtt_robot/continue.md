# continue.md — 2026-05-01 end-of-session handoff (L5 closed)

**Open here next session.** L5 paths_only e2e milestone is achieved;
next is L6 (drive_base tool catalogue) to make mixed scenarios pass,
or Track B (Pico hardware) which is now unblocked.

## L5 milestone — Linux mission planner driving the dongle architecture

The full chain works end-to-end on Linux:

```
ros_planner_ii → MQTT → mqtt_robot_main.lua → robot_hal (dongle mode)
  → dongle_hal.lua → libcomm → SLIP-framed pty
  → robot_sim ext_bus_thread → dongle_manager → internal_bus
  → drive_base.on_msg → libphysics
  → drive_base.tick → polled GET_TELEMETRY response → libcomm
  → master HAL cache → controller observes completion
```

Every layer of Tracks A, C, and L1-L5 is exercised. Tracks A+C
contracts proved correct under realistic workload.

## How to verify on resume

```bash
cd /home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/ros_planner_ii_mqtt_robot
make
bash run_tests.sh --skip-e2e
# → 306 unit checks across 14 groups, all green
```

E2E with the dongle architecture (paths_only):

```bash
HAL_MODE=dongle bash run_tests.sh --speed 10 --count 5
# → paths_only_seed42, paths_only_seed1 both green (cmds=6 ack=6 done=6)
# → mixed_seed17, mixed_seed99 FAIL (expected — L6 boundary)
```

N=100 multi-dongle stress sanity:

```bash
for i in 1 2 3 4 5; do luajit test_comm_pty_multi_dongle.lua 2>/dev/null | grep summary; done
# → 5x "[summary] 18 passed, 0 failed"
```

## What's done — every slice

- **Track A** — portability refactor: bus_config.h, bus_kernel.h
  (pthreads + Zephyr concrete skeleton), ext_bus.h 3-fn driver
  contract, transport_uart layered above ext_bus.
- **Track C** — five contract locks (Q1 message format, Q2 lifecycle,
  Q3 identity storage, Q4 ext-bus speed, Q5 robot count).
- **L1** — bus_msg.h (40-byte envelope) + sentinel helpers.
- **L2** — generic logical_robot_entry (vtable + tick sentinel).
- **L3** — drive_base_robot.{h,c} translating bus_msg_t to libphysics.
- **L4a/b** — robot_sim restructured into four bus_threads + catalogue
  routing. Manager handles request-response (GET_*) vs fire-and-
  forget (PUSH_*, STOP/RESUME/ABORT, TELEMETRY_ON/OFF) split.
- **L5.1b** — DRV_CMD_TELEMETRY_ON/_OFF + master_seq attribution.
- **L5.2** — drive_base_ffi.lua (Lua catalogue mirror).
- **L5.3** — robot_sim --tunables blob.bin (Q3 NVS mirror).
- **L5.5** — dongle_hal.lua + robot_hal mode == "dongle" branch.
- **L5.6** — start_robot.sh orchestration (HAL_MODE=dongle spawns
  robot_sim, captures pty, exports ROBOT_SIM_PTY).
- **L5.7 (paths_only)** — DRV_CMD_GET_TELEMETRY closes the libcomm
  gap discovered in the prior session. paths_only MQTT scenarios
  green.

## What L5 left for L6

Mixed e2e scenarios (mixed_seed17 / mixed_seed99) FAIL because
drive_base's catalogue has only motion commands — no
grip/release/dock/charge. dongle_hal raises on every tool method
EXCEPT read_tool_status (which returns a benign stub).

## Next session — pick one

### Option A: L6 — drive_base tool catalogue (~1 session)

Goal: get mixed_* MQTT scenarios green so all 4 e2e scenarios
pass under HAL_MODE=dongle.

1. Extend libcomm/drive_base_robot.h:
   ```c
   #define DRV_CMD_BEGIN_TOOL_MOVE  0x1040u
   #define DRV_CMD_BEGIN_GRIP       0x1041u
   #define DRV_CMD_BEGIN_RELEASE    0x1042u
   #define DRV_CMD_BEGIN_DOCK       0x1043u
   #define DRV_CMD_BEGIN_CHARGE     0x1044u
   ```
   Plus payload structs (slot int32 + optional target + speed).

2. drive_base_robot.c on_msg handlers wire each to the matching
   `phys_begin_*` call. Like PUSH_*, push to seg_track if the
   call returned a non-zero seg_id (for tools, libphysics may or
   may not return one — check the API).

3. Either extend the GET_TELEMETRY response with tool-status fields
   (per-slot 16 bytes × 8 slots = 128 B doesn't fit in 32; need a
   separate DRV_CMD_GET_TOOL_STATUS that takes a slot index and
   returns one tool's status). Or fold the most-needed tool fields
   into the existing GET_TELEMETRY (e.g. battery_j only — that's
   what the controller polls per tick).

4. drive_base_ffi.lua: add cmd codes + payload builders for the
   new commands.

5. dongle_hal.lua: replace the begin_* error stubs with real
   comm_submit calls. read_tool_status: replace the constant stub
   with a real polled GET_TOOL_STATUS (or whatever the design
   in step 3 picks).

6. Run e2e with all 4 scenarios. mixed_* should now pass.

7. Commit. Update memory + this continue.md.

### Option B: Track B — Pico hardware port (multi-session)

Architecture is proven on Linux; same code ports to embedded.

Hardware needed: Raspberry Pi Pico (~$5) + Pi Debug Probe (~$12).
WSL2 USB passthrough via usbipd-win works but is flakier than
native Linux for board flashing.

Plan (per the original Track B in continue.md):

1. Set up Zephyr workspace (west init for rpi_pico target).
   Confirm `samples/hello_world` blinks an LED.
2. bus_kernel_zephyr.c: it's a concrete skeleton in the repo
   already; expect linker errors on first compile that surface
   any remaining Linux-isms above the contract.
3. ext_bus_rp2040_uart_dma.c: per-silicon driver. UART + DMA RX
   double-buffer + idle-line ISR + semaphore.
4. Loopback contract test on hardware (TX→RX jumper). Same source
   as test_ext_bus_contract on Linux.
5. Two-Pico interop. PING test mirroring the Linux pty path.
6. Eventually bring up drive_base inside the Pico and run
   test_dongle_hal.lua against a real Pico over USB-CDC-ACM.

### Option C: design-only — Q3 commissioning protocol, motor bus, …

continue.md's earlier "Track C follow-ups" remain (Q3 commissioning
catalogue command; Q4 motor/sensor bus). These were deferred to
Track B time. If Track B is blocked on hardware availability, this
is design work that can land any time.

## Recommendation

L6 is the smallest meaningful next step that finishes the Linux
e2e story (all 4 scenarios green). After L6, Track B is unambiguous
porting work without remaining Linux-side TODOs.

## Memory pointers

- `project_dongle_architecture.md` — locked architecture.
- `project_dongle_port_track_a.md` — Track A summary.
- `project_dongle_track_c.md` — Track C contract decisions.
- `project_dongle_l1_l5_progress.md` — L1-L5 closure (this work).
- `feedback_chain_tree_no_blocking_io.md`,
  `feedback_no_soft_faults.md`,
  `feedback_phase6_handler_budget.md`,
  `feedback_verify_handoff_hypothesis.md` — discipline carryover.

## Working tree state

Clean. 6 commits ahead → all pushed to origin/master at end of
2026-05-01 session. Next session opens cleanly.

```
532c16ae  L5: GET_TELEMETRY polled path closes libcomm gap   ← top
f6b9a578  L5.5: dongle_hal.lua skeleton + libcomm gap
41247f23  L5.2 + L5.3: drive_base FFI and tunables blob
0babc8f5  drive_base: L5.1b — TELEMETRY_ON/OFF + master-seq
d2c289ac  robot_sim: L4 — four-thread dongle decomposition
a376ebdf  libcomm: L3 — drive_base wired to libphysics
c0342bc2  libcomm: Track A + Track C + L1/L2
```
