# Continuous-motion design

The reason the C side owns a path queue rather than a single setpoint:
**the robot has to stay in motion across virtual-node boundaries**.

## Invariants

1. **C is the path of trust.** Anything in the segment queue *will* be
   followed in order; pure-pursuit chases the head, pops on completion,
   and never inserts a stop unless `request_stop()` was called.
2. **Path workers are thin.** They do not steer. They poll
   `hal:read_path_status().last_done_seg_id` and complete when their
   `bb._seg_id` matches.
3. **Non-motion workers assume stationary.** The controller will not
   activate them until `queue_depth() == 0` AND `is_stopped()`.
4. **One worker active at a time.** `bb.active_worker` is the lock.
   `_advance_worker_queue` is a no-op while it's set.

## Motion / non-motion gating

The controller maintains `_blocked_by_non_motion`. Once a non-motion
packet enters the worker queue, every later path packet enters with
`pushed = false` — i.e. it is *not* immediately handed to C, and has to
wait until the non-motion worker completes.

After the non-motion worker finishes, `_after_non_motion_done` walks the
worker_queue head-to-tail and pushes deferred path packets to C until it
hits another non-motion packet (which becomes the new gate).

This is what makes a stream of mixed packets — line, spline, scan, line,
line — drive smoothly to the scan, dwell, then drive smoothly off again.

## Three queues

| Queue | Where | Holds | Pop |
|---|---|---|---|
| C segment queue | `phys_t::queue[QUEUE_CAP=32]` | active line/spline/rotate segments | follower on arrival |
| Lua worker queue | `ctrl.worker_queue` | every command in arrival order, pushed-or-deferred flag | `_advance_worker_queue` when not busy |
| ChainTree event queue | `handle.event_queue` | one `CFL_TIMER_EVENT` per active KB per tick | engine drains every tick |

## Why pure-pursuit + arc-length LUT

Bezier splines need an arc-length parameterisation so the follower can
hold a constant linear speed even when the curve has uneven `t` density.
`build_arc_lut` samples 33 points along the segment at construction; the
follower projects the robot onto the curve, looks up the closest
`arc_length`, and then does a forward `path_point_at(s + L)` to get the
look-ahead goal. The lookahead is `min + k_v * |v_cmd|` so it lengthens
at speed.

A line is just a degenerate spline (control points at thirds along the
chord) — same code path, same LUT, no special-case follower.

A rotate segment is its own follower branch: zero linear, signed angular
rate proportional to heading error. `arc_len` is `|wrap_angle(to-from)|`
just so progress reads sensibly.

## Cross-track abort

If the robot drifts more than `cross_track_abort_m` away from the
projected segment point, the follower zeros velocity and sets
`PATH_F_FAULT`. Path workers translate this into a `path_fault`
`kb_done` event. The default was 5.0 m; raised to 20.0 m in
`physics_config.json` because hard 90° corners on `path_line` segments
can transiently push the robot outside a tight band, and the random-path
harness's pose model can drift from the C truth pose enough to trigger
spurious aborts. (Real planners that read deltas from `kb_done` won't
have this problem; they'll re-plan from the C-reported pose.)
