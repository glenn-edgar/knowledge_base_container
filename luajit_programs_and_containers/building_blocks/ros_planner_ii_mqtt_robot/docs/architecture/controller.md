# Continuous-motion dispatcher — `robot_controller.lua`

Owns three things:

1. **RPC drain** — pulls JSON command packets off the transport, ACKs them,
   classifies them as path or non-motion.
2. **Path queue routing** — eagerly pushes path packets to the C segment
   queue up to the next non-motion boundary; gates non-motion packets in
   `worker_queue` until the robot is stopped and the C queue is empty.
3. **Worker lifecycle** — activates one ChainTree worker at a time, runs
   watchdog + heartbeat + completion, sends `kb_done`.

## Packet routing

```
on RPC packet → ack → classify
  if path packet (line / spline / rotate):
      if not _blocked_by_non_motion:
          seg_id = hal:push_<kind>(...)             ← C queue
          enqueue { kind="path", pushed=true, seg_id, cmd }
      else:
          enqueue { kind="path", pushed=false,  cmd }   ← deferred
  else (non-motion):
      _blocked_by_non_motion = true
      enqueue { kind="non_motion", cmd }
```

`_blocked_by_non_motion` is the gate: as soon as one non-motion packet
arrives, every later path packet is held in the queue until the non-motion
worker completes. After it does, `_after_non_motion_done` walks the queue
and pushes deferred path packets to C up to the next non-motion boundary.

## Worker activation

`_advance_worker_queue` runs each tick when no worker is active.

| Head packet kind | Activation condition |
|---|---|
| `path`       | always (already in C queue, just start the worker so it polls) |
| `non_motion` | `hal:queue_depth() == 0` AND `hal:is_stopped()` |

A worker's KB is activated by enabling its nodes, calling
`engine.init_test`, and adding it to `handle.active_tests`. The blackboard
is reset (deltas, watchdog, heartbeat counter), and `bb._seg_start` snaps
the truth pose so the worker can compute per-packet position deltas.

## Watchdog + heartbeat

Each tick of an active worker:

| Counter | Resets when | Trips at | Action |
|---|---|---|---|
| `watchdog_silence` | worker sets `bb.worker_alive = true` | 50 ticks (5 s sim) | mark `worker_done`, `fault_reason = "watchdog_timeout"` |
| `heartbeat_counter` | every 10 ticks | 10 ticks (1 s sim) | `_send_heartbeat("periodic")` over MQTT stream |

## Completion

When `bb.worker_done` flips:

1. Snapshot the truth pose, fold it into `global_pos`.
2. Compute energy deduction: `energy_used_total - _energy_at_worker_start`.
   For `recharge` packets, snap `energy.remaining` to the current battery
   reading instead.
3. Emit a `final` heartbeat, then a `kb_done` stream event with success,
   per-packet deltas, energy fields, and optional `fault_reason`.
4. If the completed worker was non-motion, call `_after_non_motion_done`
   to flush deferred path packets and `hal:release_stop()`.

## Abort path

`ctrl:abort_all()` clears the Lua queue, drops the non-motion gate, and
calls `hal:abort_path()` (which clears the C queue). Used by
`on_planner_lost`.
