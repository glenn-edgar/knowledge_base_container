# continue.md — 2026-05-01 end-of-session handoff

**Open here next session.** This file is the single source of truth for
where the dongle port stands and what to do next. Memory has the
locked decisions (`project_dongle_track_*`); this doc has the
imperative work plan.

## Current state — clean checkpoint

Working tree is clean (verify with `git status`). Five-commit stack on
master ahead of origin:

```
f6b9a578  L5.5: dongle_hal.lua skeleton + libcomm gap discovered  ← top
41247f23  L5.2 + L5.3: drive_base FFI and tunables blob loader
0babc8f5  drive_base: L5.1b — TELEMETRY_ON/OFF + master-seq
d2c289ac  robot_sim: L4 — four-thread dongle decomposition
a376ebdf  libcomm: L3 — drive_base logical_robot wired to libphysics
c0342bc2  libcomm: Track A portability + Track C contracts + L1/L2
```

(Two earlier commits — `e73e88a5`, `b13e2b5a` — are unrelated
nano_data_center work that landed in parallel terminals.)

## How to verify on resume

```bash
cd /home/gedgar/knowledge_base_assembly/luajit_programs_and_containers/building_blocks/ros_planner_ii_mqtt_robot
make
bash run_tests.sh --skip-e2e
# → 300 unit checks green across 12 test groups
```

N=100 multi-dongle stress sanity:

```bash
for i in 1 2 3 4 5; do luajit test_comm_pty_multi_dongle.lua 2>/dev/null | grep summary; done
# → 5x "[summary] 18 passed, 0 failed"
```

## What's done (all slices)

- **Track A** — portability refactor (bus_config.h, bus_kernel.h
  pthreads + Zephyr skeleton, ext_bus.h 3-fn driver contract,
  transport_uart layered above ext_bus). 43 new test checks.
- **Track C** — five Q&A locks (Q1 message format, Q2 lifecycle,
  Q3 identity storage, Q4 ext-bus speed, Q5 robot count). Memory:
  `project_dongle_track_c.md`.
- **L1** — `bus_msg.h` (40-byte envelope) + sentinel helpers + 23
  unit checks.
- **L2** — generic `logical_robot_entry` (vtable + tick sentinel)
  + 16 unit checks.
- **L3** — `drive_base_robot.{h,c}` translating bus_msg_t to
  libphysics. 15 unit checks.
- **L4a + L4b** — robot_sim restructured into four `bus_thread`s
  (ext_bus, manager, internal_bus, drive_base). Catalogue routes
  through to drive_base; SEG_DONE flows back through ext_tx_q.
  19 catalogue routing checks. Bug caught + fixed: drive_base's
  unconditional 100Hz telemetry was overflowing ext_tx_q and
  flaking PING ACKs.
- **L5.1b** — `DRV_CMD_TELEMETRY_ON / _OFF` + master-seq
  attribution in seg_track FIFO.
- **L5.2** — `drive_base_ffi.lua` (Lua catalogue + payload builders).
- **L5.3** — robot_sim `--tunables blob.bin` (Q3 NVS mirror) +
  `build_drive_base_tunables.lua` JSON→bin tool.
- **L5.5 (partial)** — `dongle_hal.lua` skeleton + `robot_hal.lua`
  mode == "dongle" branch.

## **🛑 Gap blocking L5 e2e — open here next session**

**The problem:**

`libcomm`'s master-side `comm_poll` only surfaces s2m frames whose
`ack_seq` matches an outstanding request slot:

```c
// libcomm/comm.c:725 (in master_handle_frame)
if (s->seq != in->ack_seq) return;     // stale or unmatched
```

`drive_base`'s `DRV_EVT_TELEMETRY` and `DRV_EVT_SEG_DONE` are emitted
**unsolicited** — they reach the wire fine but get filtered on the
master side as "stale or unmatched." `dongle_hal.lua`'s cache never
updates; `read_pose()` / `read_path_status()` return stale zeros.

`test_dongle_hal.lua` reproduces this:

```
PASS  robot_sim opened a pty
PASS  HAL initialised in dongle mode
PASS  initial pose at origin
PASS  push_line returned seg_id=1
FAIL  SEG_DONE arrived for our seg_id   ← unsolicited frame dropped
FAIL  rover advanced past x=0.5m        ← cache never updated
PASS  rover stayed near y axis
```

**Recommended fix — polled GET_TELEMETRY (smaller change):**

1. Add `DRV_CMD_GET_TELEMETRY` (0x1031) to `libcomm/drive_base_robot.h`.
   Empty request payload. Response = 32-byte struct (x, y, h, v, ω,
   energy_used_total, last_done_master_seq, queue_depth, flags).

2. Refactor `robot_sim/dongle_threads.c` manager: classify catalogue
   commands into "fire-and-forget" (auto-ACK as today) vs
   "request-response" (route to int_bus_q, do NOT auto-ACK; the
   target robot produces the response itself). Use a simple range or
   bitmap. GET_TELEMETRY = request-response.

3. `libcomm/drive_base_robot.c` on_msg adds GET_TELEMETRY handler:
   read phys state, build 32-byte payload, emit bus_msg_t to
   outbound with `seq = request.seq` so libcomm correlates the
   response. drive_base's `s->last_done_master_seq` is set on each
   SEG_DONE edge inside tick (separate from the now-vestigial
   unsolicited DRV_EVT_SEG_DONE emission, which we keep for the
   in-process tests).

4. `dongle_hal.lua`: replace the broken "TELEMETRY_ON + drain
   unsolicited" approach with periodic GET_TELEMETRY polls. Cache
   structure already exists (`self._pose`, `self._path`); just pump
   it from the polled response payload. Suggested tick rate: 50 ms.

5. Update existing in-process tests if needed (they should still
   pass since they use ext_tx_q directly, not over-the-wire).

**Why polling is fine for our workload:**
- Controller tick (CT_TICK_SIM_S = 100 ms) is slower than 50 ms poll.
- 921.6 kbps wire trivially handles 20 polls/s × 80-byte round trips.
- Matches how physics_pipe.h already works (cmd block down,
  telemetry up — but with explicit request-response on each side).

**Alternative (bigger):** extend libcomm with an unsolicited s2m
path (callback or queue on the master side). More natural for embedded
RTOS but a real libcomm extension. Skipping for now; revisit if
polling proves too chatty in practice.

## Plan of action — next session

In order:

### Step 1 — manager refactor + GET_TELEMETRY catalogue (~30-60 min)

- Add `DRV_CMD_GET_TELEMETRY = 0x1031` in `libcomm/drive_base_robot.h`.
- Add `s->last_done_master_seq` field to drive_base_t.
- drive_base tick: on SEG_DONE edge, update `last_done_master_seq`
  (in addition to emitting the unsolicited DRV_EVT_SEG_DONE for
  in-process tests).
- drive_base on_msg: handle GET_TELEMETRY → build 32-byte response
  (replace `active_seg_id` field with `last_done_master_seq` since
  that's what the master cares about), emit bus_msg with
  `seq = request.seq`, post to outbound.
- Manager (`robot_sim/dongle_threads.c`): split catalogue handling
  into two cases. Suggested:
  ```c
  if (cmd == DRV_CMD_GET_TELEMETRY) {
      // request-response: route, no auto-ACK
      bus_msgq_put(&ctx->int_bus_q, &in);
  } else if (cmd >= 0x0100u) {
      // fire-and-forget: route + auto-ACK
      bus_msgq_put(&ctx->int_bus_q, &in);
      manager_push_simple_ack(...ACK_BARE...);
  }
  ```
- Extend `test_dongle_catalogue.c` with a GET_TELEMETRY round-trip
  check.

### Step 2 — dongle_hal polled drain (~30 min)

- `dongle_hal.lua`: drop `TELEMETRY_ON` from init.
- Replace `_drain` body with: submit GET_TELEMETRY (using a cached
  reusable handle or fresh per call), wait for response, decode
  payload, update `self._pose` / `self._path`.
- Throttle: only re-submit if a previous GET_TELEMETRY isn't
  outstanding, OR if last update is > 50 ms old.
- Re-run `test_dongle_hal.lua` — should now show pose advancing,
  SEG_DONE matching.

### Step 3 — orchestration (L5.6, ~30 min)

- `start_robot.sh` (in `building_blocks/ros_scripts/`): spawn
  `robot_sim --type 1 --instance 1 --addr 1 --tunables /tmp/dbt.bin`
  before launching `mqtt_robot_main.lua`. Capture its `PTY=…` line
  via stdout pipe. Export `ROBOT_SIM_PTY=<path>` and `HAL_MODE=dongle`.
  Also build the tunables blob via
  `luajit build_drive_base_tunables.lua physics_config.json /tmp/dbt.bin`.
- `mqtt_robot_main.lua` doesn't change — `robot_hal.new()` already
  routes to `dongle_hal` when `HAL_MODE=dongle`.

### Step 4 — e2e MQTT scenarios (L5.7)

```bash
bash run_tests.sh   # without --skip-e2e
```

Should run `test_random_paths.lua` paths_only scenarios end-to-end
through the new architecture. Mixed scenarios will fail (tools not
in drive_base catalogue yet — deferred to L6+).

Expect to debug timing / orchestration issues on first run.

### Step 5 — commit + memory update

Once paths_only e2e green: commit, update
`project_dongle_l1_l5_progress.md` memory file, update this
continue.md to mark L5 closed and Track B (Pico hardware) as the
next milestone.

## Track B (Pico hardware) — still deferred

`bus_kernel_zephyr.c` is a concrete skeleton ready to compile under
a Zephyr workspace. `ext_bus_rp2040_uart_dma.c` is the per-silicon
file to write next. Hardware needs: Raspberry Pi Pico (~$5) + Pi
Debug Probe (~$12). USB passthrough via WSL2's `usbipd-win` is
viable but flakier than native Linux.

Track B is unblocked the moment L5 is green.

## Memory pointers

Read these for context:

- `project_dongle_architecture.md` — locked architecture
  (4-role decomposition, 3 boundaries, 3 Zephyr targets).
- `project_dongle_port_track_a.md` — Track A summary.
- `project_dongle_track_c.md` — Track C contract decisions (Q1-Q5).
- `feedback_chain_tree_no_blocking_io.md` — tick handler discipline.
- `feedback_no_soft_faults.md` — fail-stop everywhere.
- `feedback_phase6_handler_budget.md` — ≤50 ms handler cap.
- `feedback_verify_handoff_hypothesis.md` — caught the L4b telemetry
  saturation bug and the L5 unsolicited gap; verify before fixing.
