# Pipe implementation roadmap

Sequenced TODO list for landing the [control / physics
pipe](../architecture/pipe.md). Each step is small enough that
`./run_tests.sh` should stay green before and after.

The hard rule: **don't break the existing tests during migration.**
Every step preserves `phys_step` semantics; the pipe is added behind
the scenes and the existing FFI calls become wrappers around it.

## Phase 1 — header + scaffolding

### 1.1 Land `physics_pipe.h`

Already done. `physics_pipe.h` declares block layouts, FIFOs, fault
codes, telemetry field bits, and the SPSC primitives.

### 1.2 Add `physics_pipe.c`

- Implement `pipe_cmd_push / pop`, `pipe_up_push / pop`,
  `pipe_*_depth`, `pipe_*_clear`. Plain head/tail indexing, no
  atomics needed for the in-process build. Add memory barriers when
  we move to shared-memory (phase 5).
- Implement the `cmd_build_*` host-side helpers.
- Add to the Makefile: `physics_pipe.c` in the SRC list, `libphysics.so`
  rebuilds with both objects.

### 1.3 Embed FIFOs in `phys_t`

In `physics_core.c`:

```c
struct phys_s {
    /* ... existing fields ... */
    cmd_fifo_t cmd_fifo;
    up_fifo_t  up_fifo;

    /* telemetry control */
    uint32_t telem_period;       /* inner steps; 0 = disabled */
    uint64_t telem_mask;         /* field-group mask */
    uint32_t inner_step_counter;

    /* watchdog state */
    uint32_t cmd_silent_us;      /* sim-time us since last cmd */
    uint32_t fault_word;         /* sticky; zeroed by CMD_RESET */
};
```

`phys_create` initialises both FIFOs to empty, telemetry to
`TF_DEFAULT_MASK` at 40 Hz.

**Validation gate**: `make` builds clean, `./run_tests.sh` still
green. The pipe is wired in but nothing reads or writes it yet.

## Phase 2 — physics-side wiring

### 2.1 Drain `cmd_fifo` at the top of `phys_step`

```c
void phys_step(phys_t *p, double dt_sim) {
    if (!p) return;
    /* Drain pending cmds first; each may modify queue / tools / params */
    cmd_block_t blk;
    while (pipe_cmd_pop(&p->cmd_fifo, &blk)) {
        phys_apply_cmd(p, &blk);    /* dispatch on blk.kind             */
    }
    /* ... existing inner-loop logic ... */
}
```

`phys_apply_cmd` is a switch over `cmd_kind`. Each case calls the
existing static helpers (`q_push` for path pushes,
`phys_begin_tool_move` for tool ops, etc.). For unknown kinds, raise
`FAULT_INVALID_CMD`.

Each successful cmd: emit `EVT_CMD_ACK` on `up_fifo`. Update
`p->cmd_silent_us = 0`.

### 2.2 Emit telemetry every N inner steps

In `inner_step`, after the chassis update:

```c
p->inner_step_counter++;
if (p->telem_period > 0 &&
    p->inner_step_counter >= p->telem_period) {
    p->inner_step_counter = 0;
    phys_emit_telem(p);
}
```

`phys_emit_telem` builds an `up_block_t` of kind `UP_TELEM`, fills
only the field groups in `p->telem_mask`, and pushes to `up_fifo`. On
push failure, raise `FAULT_UP_OVERFLOW`.

### 2.3 Convert `last_done_seg_id` latch to `EVT_SEG_DONE`

Today the segment-done path latches `PATH_F_SEG_DONE` and the latched
flag is read by `phys_read_path_status` (which clears it). Add an
event push at the same point:

```c
/* in follower_step, on segment completion: */
p->last_done_seg_id = cur->seg_id;
p->flags |= PATH_F_SEG_DONE;
emit_event_seg_done(p, cur->seg_id, p->energy_used_total);
q_pop(p);
```

`emit_event_seg_done` builds `EVT_SEG_DONE` and pushes to `up_fifo`.
Keep the latch and the existing API working — the new event channel is
additive.

### 2.4 Convert tool completion to `EVT_TOOL_DONE`

When a `tool_t::op` returns to 0 with `TOOL_F_AT_TARGET`, emit
`EVT_TOOL_DONE { slot, flags_final, value_final }`. Keep the existing
flag-reads working.

### 2.5 Watchdogs

- Increment `cmd_silent_us` by `dt_sim * 1e6` each inner step.
  When > 200_000 → `FAULT_CONTROLLER_SILENT`.
- Track `cycle_us` (host monotonic delta around the inner step). On
  > 2× `inner_dt_us` → `FAULT_CYCLE_OVERRUN`.

### 2.6 Fault path

When any `FAULT_*` is raised:

```c
static void phys_raise_fault(phys_t *p, uint32_t code,
                             uint32_t ctx0, uint32_t ctx1) {
    if (p->fault_word != 0) return;     /* already faulted */
    p->fault_word = code;
    /* Clear motion + tools */
    q_clear(p);
    p->v_cmd = p->v_l = p->v_r = 0;
    for (int i = 0; i < p->tool_count; i++) p->tools[i].op = 0;
    /* Emit EVT_FAULT */
    emit_event_fault(p, code, ctx0, ctx1);
}
```

After this point, `phys_apply_cmd` accepts only `CMD_RESET`. All
other cmds raise `FAULT_INVALID_CMD` (which is a no-op while already
faulted thanks to the `fault_word != 0` check).

`CMD_RESET` clears `fault_word`, drains both FIFOs, returns to
normal. Mission state is **not** restored.

**Validation gate**: telemetry flowing, events flowing, faults raise
correctly. The existing `phys_read_*` API still works because we
preserved the latches. `./run_tests.sh` still green.

## Phase 3 — Lua-side adapter

### 3.1 Add `physics_pipe.lua`

Same dir as `physics_ffi.lua`. Wraps the FFI types and the SPSC
primitives. Adds:

```lua
local pipe = require("physics_pipe").wrap(hal_handle)

pipe:push_line(fx, fy, tx, ty, h_from, h_to, speed)   -- returns seg_id
pipe:request_stop() / pipe:release_stop() / pipe:abort_path()
pipe:begin_tool_move(slot_or_name, target, speed)
pipe:begin_grip(slot) / :begin_release / :begin_dock / :begin_charge
pipe:set_param(name, value)
pipe:set_telem_rate(period_inner_steps)
pipe:set_telem_fields(mask)        -- accepts a table {"TF_PATH","TF_FAULT"}
pipe:heartbeat()                   -- emit one CMD_HEARTBEAT
pipe:drain(callback)               -- callback(blk) per up_block
```

Internally it builds `cmd_block_t` cdata with the host-side helpers
and calls `pipe_cmd_push`. Sequence numbers minted in Lua.

### 3.2 Re-base `physics_ffi`'s OO methods on the pipe

`hal:push_line(...)` becomes:

```lua
function H:push_line(fx, fy, tx, ty, h_from, h_to, speed)
    return self.pipe:push_line(fx, fy, tx, ty, h_from, h_to, speed)
end
```

Existing callers are unchanged. The pipe is now load-bearing.

### 3.3 Adapt `robot_controller`'s polling

Today the controller polls `hal:read_path_status().last_done_seg_id`.
Change to:

```lua
function M:tick()
    self:drain_pipe()        -- new: handle UP_TELEM / EVT_*
    self:drain_commands()
    self:timer_tick()
end

function M:drain_pipe()
    self.hal.pipe:drain(function(blk)
        if blk.kind == "UP_TELEM" then
            self._latest_telem = blk
        elseif blk.kind == "EVT_SEG_DONE" then
            self:_on_seg_done(blk.seg_id, blk.energy_at_complete)
        elseif blk.kind == "EVT_TOOL_DONE" then
            self:_on_tool_done(blk.slot, blk.flags_final, blk.value_final)
        elseif blk.kind == "EVT_FAULT" then
            self:_on_pipe_fault(blk)
        end
    end)
end
```

Path workers' completion check moves from polling to
`_on_seg_done(seg_id)` setting `bb.worker_success = true` when
`seg_id == bb._seg_id`.

### 3.4 Heartbeat + watchdog plumbing

- `mqtt_robot_main.lua`'s tick loop calls `pipe:heartbeat()` once per
  tick (after `ctrl:tick()` if no other commands flowed). This
  satisfies `cmd_silent`.
- Controller tracks last-up-block sim_t. If no block in 100 ms wall
  →  abort mission with `FAULT_PHYSICS_SILENT` semantics.

### 3.5 Fault path

`EVT_FAULT` from physics:

1. Controller calls `ctrl:abort_all()` (clears worker queue).
2. Controller emits a final `kb_done` with
   `success = false, fault_reason = "pipe_fault:<code>"` for each
   active worker.
3. Mission terminates. The supervisor (track 1) decides what to do.
4. Recovery is a manual `CMD_RESET` followed by re-establishing
   mission state.

**Validation gate**: e2e tests still pass. Path workers complete on
events instead of polling. Telemetry rate / mask defaults match
today's implicit behaviour. Faults end the mission.

## Phase 4 — telemetry exercise

### 4.1 Test: dynamic telemetry rate

Add a unit test in `test_physics.lua`:

```
SET_TELEM_RATE 1   -- 200 Hz
run 1 sec sim, count UP_TELEM frames received -> ~200
SET_TELEM_RATE 0   -- disabled
run 1 sec sim, count -> 0
SET_TELEM_RATE 5   -- 40 Hz
run 1 sec, count -> ~40
```

### 4.2 Test: field mask

- Set mask to just `TF_PATH`. Read a frame; verify `x == 0`,
  `cross_track_err` matches the active segment.
- Set mask to `TF_TOOLS | TF_FAULT`. Verify pose is zero.

### 4.3 Test: fault path

- Push a path that aborts on cross-track.
- Confirm one `EVT_FAULT(FAULT_PATH_CROSS_TRACK)` is emitted.
- Confirm subsequent `CMD_PUSH_*` raise `FAULT_INVALID_CMD` (no-op
  due to `fault_word != 0`).
- Send `CMD_RESET`, push a fresh segment, confirm normal operation.

### 4.4 Test: heartbeat-silent fault

- Stop sending heartbeats for 250 ms sim.
- Confirm `EVT_FAULT(FAULT_CONTROLLER_SILENT)` lands.

**Validation gate**: 4 new tests green; physics passes 30/30.

## Phase 5 — process split (later)

When phases 1–4 are stable:

### 5.1 Move physics into a separate process

- `phys_create` allocates the FIFOs in a `mmap`'d region with a
  well-known name.
- The Lua side opens the same region read/write.
- `pipe_*_push/pop` add `__atomic` ops or `volatile` + memory barriers
  for cross-process safety.
- physics is launched as a sibling process by `start_robot.sh`; both
  exit on the other's death (PID watch).

### 5.2 SLIP serial transport (TBD)

- Block format unchanged.
- Wire layer: COBS framing + CRC32.
- Two separate UART channels (or one with a multiplexer byte).
- This is the MCU-port path. Spec is still TBD; the in-process pipe
  needs to settle first.

## What we're not doing

- **No soft faults.** Already settled.
- **No mission resumption after fault.** RESET is a hard reboot.
- **No replay buffer.** Telemetry is fire-and-forget; events are
  reliable through the FIFO but not durable across a fault.
- **No version negotiation.** Both sides ship together; block format
  is hard-coded.

## Definition of done

- All 26 physics tests + 4 e2e scenarios green using the pipe.
- 4 new pipe-specific tests green.
- The existing `physics_ffi` API is untouched at the surface; the
  guts route through the pipe.
- `docs/architecture/pipe.md` matches the implementation.
- `physics_core.c` no longer reads the physics state directly from
  Lua's perspective — only via `up_fifo` blocks.

## Risk register

| Risk | Mitigation |
|---|---|
| Break test suite during phase 2 wiring | Land each step behind a feature flag; toggle on after the controller adapts in phase 3 |
| FIFO depth too small for path bursts | `CMD_FIFO_DEPTH = 32` matches `QUEUE_CAP`; if real missions burst harder, raise both together |
| Telemetry block too small if we add fields | `UP_BLOCK_BYTES = 256` has ~80 bytes spare today; growth is one bump |
| `CMD_HEARTBEAT` cadence wrong → bogus fault | Make `cmd_silent` window configurable via `CMD_SET_PARAM` for headless test runs |
