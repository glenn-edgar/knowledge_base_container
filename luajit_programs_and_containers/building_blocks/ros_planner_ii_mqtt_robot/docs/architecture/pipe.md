# Control / physics pipe

The boundary between the soft real-time ChainTree (10 Hz, may stall) and
the hard real-time physics inner loop (200 Hz today, eventually MCU
firmware). Designed so the same contract carries us from in-process
shared memory to a SLIP-framed serial link without code changes above
the transport.

## Why a pipe at all

Linux user-space cannot reliably hold a tighter cadence than ~10 Hz
without PREEMPT_RT, and even with it the inner motor loop belongs in
firmware. So the production-shaped split is:

- **ChainTree side** decides what to do, hands segments and tool ops
  *ahead of where the robot is*, and never blocks the inner loop.
- **Physics side** runs the segment queue, motor PID, sensor sampling,
  and autonomous safety. It must never stall waiting for the controller.

The pipe is what makes the boundary explicit. Today both sides share an
FFI handle; the pipe replaces those direct calls with two FIFOs of fixed
memory blocks.

## Topology

```
                  +----------------------+
   ChainTree ---> |   cmd_fifo            | ---> physics
   (10 Hz)        |   N × cmd_block (64B) |       (200 Hz)
                  +----------------------+

                  +----------------------+
   ChainTree <--- |   up_fifo             | <--- physics
                  |   M × up_block (256B) |
                  +----------------------+
```

- Two SPSC ring buffers, lock-free with head/tail counters.
- One block size per direction, padded to the largest payload.
- Storage lives **inside `phys_t`** (allocated by `phys_create`); no
  singletons. Multi-instance future-friendly.
- Telemetry rides the up FIFO as ordinary blocks — not a side-channel
  shared struct. Unifies the wire and ports to SLIP unchanged.

## Block sizes (current)

```c
#define CMD_BLOCK_BYTES  64
#define UP_BLOCK_BYTES   256
#define CMD_FIFO_DEPTH   32      /* segment-ahead buffer */
#define UP_FIFO_DEPTH    64      /* events + telemetry frames */
```

## Cmd block

```c
typedef struct {
    uint16_t kind;          /* CMD_*                                */
    uint16_t reserved;
    uint32_t seq;           /* monotonic, ChainTree-assigned        */
    uint8_t  payload[56];   /* kind-specific                        */
} cmd_block_t;              /* 64 bytes */
```

| Tag | Name                  | Payload |
|----:|---|---|
| 1   | `CMD_PUSH_LINE`       | `{ fx, fy, tx, ty, h_from, h_to, speed, seg_id }` |
| 2   | `CMD_PUSH_SPLINE`     | same |
| 3   | `CMD_PUSH_ROTATE`     | `{ from_h, to_h, rate, seg_id }` |
| 10  | `CMD_REQUEST_STOP`    | empty |
| 11  | `CMD_RELEASE_STOP`    | empty |
| 12  | `CMD_ABORT_PATH`      | empty (clears segment queue, no fault) |
| 20  | `CMD_BEGIN_TOOL_MOVE` | `{ slot, target, speed }` |
| 21  | `CMD_BEGIN_GRIP`      | `{ slot }` |
| 22  | `CMD_BEGIN_RELEASE`   | `{ slot }` |
| 23  | `CMD_BEGIN_DOCK`      | `{ slot }` |
| 24  | `CMD_BEGIN_CHARGE`    | `{ slot, target_j }` |
| 30  | `CMD_SET_PARAM`       | `{ param_id, value }` |
| 31  | `CMD_SET_TELEM_RATE`  | `{ telem_period_inner_steps }` |
| 32  | `CMD_SET_TELEM_FIELDS`| `{ field_mask }` |
| 99  | `CMD_HEARTBEAT`       | empty (resets `cmd_silent` watchdog) |
| 200 | `CMD_RESET`           | empty (only valid after a fault) |

`seg_id` is ChainTree-assigned. Physics echoes it back in
`EVT_SEG_DONE`. Single producer, no collision possible.

## Up block

```c
typedef struct {
    uint16_t kind;          /* UP_TELEM | EVT_*                     */
    uint16_t reserved;
    uint32_t sim_t_us;      /* every up frame is timestamped        */
    uint8_t  payload[248];  /* kind-specific                        */
} up_block_t;               /* 256 bytes */
```

| Tag | Name              | Notes |
|----:|---|---|
| 1   | `UP_TELEM`        | telemetry frame; rate + fields are runtime-selectable |
| 100 | `EVT_CMD_ACK`     | `{ seq }` — binary ack |
| 101 | `EVT_SEG_DONE`    | `{ seg_id, energy_at_complete }` |
| 102 | `EVT_TOOL_DONE`   | `{ slot, flags_final, value_final }` |
| 200 | `EVT_FAULT`       | `{ fault_code, sim_t_us, ctx0, ctx1 }` — system halted |

## Telemetry control (runtime)

Two cmds set the cadence and content. Both are fault-stop on bad input.

### `CMD_SET_TELEM_RATE`

```
payload: { uint32_t telem_period_inner_steps; }
```

Physics emits one `UP_TELEM` block every `N` inner steps.

| `N` | Effective rate (default `inner_dt = 5 ms`) |
|---|---|
| 0 | disabled — no telemetry |
| 1 | 200 Hz |
| 5 | 40 Hz (boot default) |
| 20 | 10 Hz |
| 200 | 1 Hz |

Out-of-range `N` is a fault. "Inner steps" is used (not Hz) so physics
needs no float math at runtime.

### `CMD_SET_TELEM_FIELDS`

```
payload: { uint64_t field_mask; }
```

Each bit selects a field group. Physics fills only selected groups;
unselected groups are sent as zeros — the block stays fixed-size,
which keeps the SLIP framer trivial.

| Bit | Group | Fields |
|---:|---|---|
| 0 | `TF_TIME`        | `cycle_us, cycles_since` |
| 1 | `TF_POSE_TRUTH`  | `x, y, heading` |
| 2 | `TF_POSE_NOISY`  | `x_n, y_n, heading_n` |
| 3 | `TF_VELOCITY`    | `v, omega` |
| 4 | `TF_WHEELS`      | `v_l, v_r, v_l_target, v_r_target` |
| 5 | `TF_ACTUATORS`   | `torque_l, torque_r, current_l, current_r` |
| 6 | `TF_PATH`        | `flags, active_seg_id, last_done_seg_id, queue_depth, active_progress, cross_track_err, heading_err, v_cmd` |
| 7 | `TF_ENERGY`      | `energy_used_total, battery_j, battery_v` |
| 8 | `TF_TOOLS`       | `tools[8]` |
| 9 | `TF_FAULT`       | `fault_word` |
| 10–63 | reserved   | |

**Boot default mask:**
`TF_TIME | TF_POSE_NOISY | TF_VELOCITY | TF_PATH | TF_ENERGY | TF_TOOLS | TF_FAULT`.

## Telemetry payload

Fits in 248 bytes. The full layout (all groups present at zero cost
when masked out):

```c
typedef struct {
    uint32_t cycle_us;
    uint32_t cycles_since;

    float    x, y, heading;
    float    x_n, y_n, heading_n;
    float    v, omega;

    float    v_l, v_r, v_l_target, v_r_target;
    float    torque_l, torque_r;
    float    current_l, current_r;

    uint32_t flags;
    uint32_t active_seg_id;
    uint32_t last_done_seg_id;
    int32_t  queue_depth;
    float    active_progress;
    float    cross_track_err;
    float    heading_err;
    float    energy_used_total;
    float    v_cmd;

    float    battery_j;
    float    battery_v;

    struct { uint32_t flags; int32_t kind; float value, target; } tools[8];

    uint32_t fault_word;
} telem_payload_t;
```

Telemetry blocks include the active `field_mask` in the payload header
so the consumer knows which fields are live.

## Fault model — fail-stop only

There are **no soft faults**. Any of these is a hard fault that halts
the system:

| Fault code | Source | Trigger |
|---|---|---|
| `FAULT_PATH_CROSS_TRACK` | physics | follower drifted past `cross_track_abort_m` |
| `FAULT_TOOL`            | physics | tool reported `TOOL_F_FAULT` |
| `FAULT_CYCLE_OVERRUN`   | physics | inner step took > 2× `inner_dt` |
| `FAULT_CONTROLLER_SILENT` | physics | no cmd in 200 ms |
| `FAULT_UP_OVERFLOW`     | physics | up_fifo full when emitting |
| `FAULT_CMD_OVERFLOW`    | ChainTree | cmd_fifo full when pushing |
| `FAULT_PHYSICS_SILENT`  | ChainTree | no up_block in 100 ms |
| `FAULT_INVALID_CMD`     | physics | malformed or out-of-range payload |

On any fault:

1. Physics zeros torques, clears segment queue, idles all tool ops,
   latches `fault_word`.
2. Physics ignores every cmd kind except `CMD_RESET`.
3. ChainTree surfaces the fault to the supervisor. Recovery policy is
   the supervisor's problem, not the pipe's.
4. On `CMD_RESET`, physics clears `fault_word`, drains both FIFOs,
   returns to normal operation. **No mission resumption.**

## Watchdogs

| Watchdog | Owner | Window | Fault on trip |
|---|---|---|---|
| `cmd_silent`        | physics  | 200 ms | `FAULT_CONTROLLER_SILENT` |
| `up_silent`         | ChainTree | 100 ms | `FAULT_PHYSICS_SILENT` |
| `cycle_overrun`     | physics  | 2 × `inner_dt` | `FAULT_CYCLE_OVERRUN` |
| `cmd_fifo_overflow` | ChainTree (writer) | n/a | `FAULT_CMD_OVERFLOW` |
| `up_fifo_overflow`  | physics (writer)   | n/a | `FAULT_UP_OVERFLOW` |

`CMD_HEARTBEAT` is what keeps `cmd_silent` from tripping when there's
no real command to send. ChainTree emits one per tick if its outbound
queue is otherwise empty.

## API (physics side)

```c
/* In physics_core.c, per-tick: */
void phys_step(phys_t *p, double dt_sim);   /* unchanged externally */

/* Internally: */
void phys_drain_cmd_fifo(phys_t *p);        /* called at start of phys_step */
void phys_emit_telem    (phys_t *p);        /* called every N inner steps  */
void phys_emit_event    (phys_t *p, ...);   /* called on seg_done / fault  */
```

## API (Lua / future controller side)

```lua
local pipe = require("physics_pipe").new(hal)

-- Outgoing
local seg_id = pipe:push_line(fx, fy, tx, ty, h_from, h_to, speed)
pipe:request_stop()
pipe:set_telem_rate(5)                         -- 40 Hz
pipe:set_telem_fields("TF_PATH", "TF_FAULT")   -- mask helper
pipe:heartbeat()                               -- once per tick

-- Incoming (drain everything queued)
pipe:drain(function(blk)
    if blk.kind == "UP_TELEM"     then ... end
    if blk.kind == "EVT_SEG_DONE" then ... end
    if blk.kind == "EVT_FAULT"    then abort_mission(blk) end
end)
```

The current `hal:push_line(...)` becomes a wrapper around
`pipe:push_line(...)` for backwards-compat during migration.

## Transport portability

| Transport | When | What changes |
|---|---|---|
| In-process (today) | LuaJIT FFI to `libphysics.so` | FIFO arrays inside `phys_t`; FFI calls drive head/tail |
| Shared memory | physics in its own process | Same structs in mmap'd region; no API change |
| SLIP serial | Physics on an MCU | Block format unchanged; add COBS framing + CRC at the wire layer |
| CAN / CAN-FD | Real drive on a CAN bus | Multi-frame for cmd_block; PDO mapping for telemetry |

The C side never knows which transport carries the bytes. Dispatch
lives in `physics_pipe.lua` and (later) the wire-encoder shim.

## Push-ahead invariant

The pipe's reason to exist:

> **ChainTree must keep `queue_depth ≥ 2` whenever the mission is
> active. Physics must never stall waiting for a command — if the
> queue empties, the follower decelerates the robot to a stop and
> reports `queue_depth == 0` in telemetry. That is not a fault, but
> it is mission-relevant; the supervisor decides what to do.**

This is exactly the contract a CAN-bus controller implements. Designing
to it now means the eventual MCU port is mechanical, not architectural.
