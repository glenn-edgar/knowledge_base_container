# Control logic

The C plant has four control loops, all running inside `inner_step(dt)`
(default 200 Hz, `dt = inner_dt = 0.005 s`). This page is the
mathematical detail behind them.

```
follower_step  →  (v_want, w_want)         pure-pursuit, picks a goal
        |
        v
compute_setpoints(v_want, w_want, dt)      ramps, translates to wheels
        |
        v
motor_pid_step(dt)                         per-wheel PID + dynamics
        |
        v
chassis_step(dt)                           integrate v, ω → x, y, heading
```

## 1. Pure-pursuit follower (line / spline)

Given the head segment and current truth pose `(x, y, heading)`:

1. **Project** the robot onto the segment by sampling the 33-point arc-length
   LUT and picking the nearest point. Returns `s_closest` (arc-length) and
   `dist_closest` (cross-track error).
2. **Cross-track abort.** If `dist_closest > cross_track_abort` →
   `PATH_F_FAULT`, zero output, return.
3. **Arrival check.** If `s_closest ≥ arc_len - arrival_tol` OR
   `dist_to_endpoint < arrival_tol` → pop segment, set
   `last_done_seg_id`, latch `PATH_F_SEG_DONE`, recurse into the next
   segment.
4. **Lookahead distance.**
   ```
   L = max(lookahead_min, lookahead_min + lookahead_k_v · |v_cmd|)
   ```
   Defaults: 0.20 m + 0.40 · |v_cmd|, so 0.5 m/s gives `L = 0.40` m.
5. **Lookahead point.** `path_point_at(s_closest + L)` — walks forward
   into the next segment(s) if `s + L > arc_len`.
6. **Curvature.** With `α = wrap(target_heading - heading)`,
   `ld = ‖(goal - pose)‖`:
   ```
   κ = 2 · sin(α) / ld          (classical pure-pursuit)
   ```
7. **Curvature speed limit.**
   ```
   v_limit_curve = 1.5 / |κ|     when |κ| > 1e-3
   v_want        = min(speed_eff, v_limit_curve)
   w_want        = v_want · κ
   ```
   The `1.5` is a hard-coded centripetal-acceleration cap (m/s²). Tight
   curves slow the robot down so the chassis doesn't slide off the
   segment.

`speed_eff = 0` if `stop_requested`, otherwise the segment's commanded
speed.

## 2. Pure-pursuit follower (rotate)

`SEG_ROTATE` is its own branch. Heading-only:

```
err = wrap(rot_to_h - heading)
if |err| < heading_tol  → segment complete
v_want = 0
w_want = sign(err) · rate         where rate = segment.speed
```

`active_progress` is computed off `|err| / |total swing|` for monitoring.

## 3. Setpoint translation

`compute_setpoints(v_want, w_want, dt)` ramps both targets and converts
to per-wheel speeds:

```
# Linear: limit |dv/dt|
v_diff   = clip(v_want - v_cmd, -max_lin_accel·dt, max_lin_accel·dt)
v_cmd   += v_diff

# Angular: limit |dω/dt|, but compare against current chassis ω
w_cur    = (v_r - v_l) / wheelbase
w_diff   = clip(w_want - w_cur, -max_ang_accel·dt, max_ang_accel·dt)
w_use    = w_cur + w_diff

# Diff-drive inverse
v_l_target = v_cmd - 0.5 · w_use · wheelbase
v_r_target = v_cmd + 0.5 · w_use · wheelbase
```

Defaults: `max_lin_accel = 1.0 m/s²`, `max_ang_accel = 3.0 rad/s²`.

The `v_cmd` ramp is what makes the robot accelerate smoothly off a
standing start — pure-pursuit only knows the *direction* of motion; the
ramp shapes the *magnitude*.

## 4. Per-wheel PID + dynamics

```
e_l = v_l_target - v_l         # error in m/s
int_l += e_l · dt              # integrate
int_l = clip(int_l, ±imax)     # anti-windup; imax = max_torque / ki
de_l = (e_l - prev_l) / dt
prev_l = e_l

τ_l = clip(kp·e_l + ki·int_l + kd·de_l, ±max_torque)
```

Same for the right wheel. Torque becomes wheel force via
`F = τ / wheel_radius`, minus a viscous loss `lin_friction · v`:

```
m_total      = mass_kg + payload_mass
m_eff_wheel  = 0.5 · m_total                     # half-vehicle inertia
F_l          = τ_l / wheel_radius - lin_friction · v_l
v_l         += (F_l / m_eff_wheel) · dt
v_l          = clip(v_l, ±max_wheel_rad_s · wheel_radius)
```

Defaults: `kp = 8`, `ki = 4`, `kd = 0.05`, `max_torque = 1.5 N·m`,
`max_wheel_rad_s = 30`, `lin_friction = 0.8`.

**Energy** is integrated as the absolute mechanical power summed over
both wheels:
```
P    = |τ_l · ω_l| + |τ_r · ω_r|        where ω_w = v_w / r
energy_used_total += energy_k · P · dt
battery_j         -= energy_k · P · dt    (clamped to ≥ 0)
```

This is what `read_path_status().energy_used_total` returns; the
controller debits the planner-side energy budget from the delta of this
field across a worker.

## 5. Chassis integration

Standard diff-drive kinematics:

```
v = ½ (v_l + v_r)                       # body linear speed
ω = (v_r - v_l) / wheelbase              # body angular speed
ω -= ang_friction · ω · dt               # angular viscous loss

x       += v · cos(heading) · dt
y       += v · sin(heading) · dt
heading  = wrap(heading + ω · dt)
```

`ang_friction = 0.4` is the only "fake" damper — the lin/ang friction
together provide enough loss that the robot doesn't drift on a noisy
PID for long after `request_stop`.

## 6. Stopped detection

After integration:

```
v_mag = ½ |v_l + v_r|
w_mag = |v_r - v_l| / wheelbase
PATH_F_STOPPED = (v_mag < stopped_eps_v) AND (w_mag < stopped_eps_omega)
```

Defaults `0.01 m/s` and `0.02 rad/s`. Used by `phys_is_stopped` (which
the controller's non-motion gate consults).

## 7. Tool sub-loop

Independent of the motion control. `tool_step(t, dt)` runs after the
chassis update for each in-use tool:

| Tool kind | While `op = move(1)` | While `op = grip/release(2/3)` | While `op = dock(4)` | While `op = charge_hold(5)` |
|---|---|---|---|---|
| `revolute_1dof` | step `value` toward `target` at `speed`, latch `AT_TARGET` when within `step` | n/a | n/a | n/a |
| `binary` | n/a | dwell `transition_s`, set `value = target`, latch `AT_TARGET \| GRASPED` (or `RELEASED`); on grip near `load_dock` snap `payload_mass = station.param1`; on release drop payload | n/a | n/a |
| `passive_dock` | n/a | n/a | check `station_at_pose(charger)`; latch `DOCKED \| AT_TARGET` or `FAULT` | integrate `battery_j += rate_w · dt` until `≥ charge_target_j`, then `AT_TARGET \| DOCKED`; otherwise `DOCKED \| CHARGING` |

Everything tool-side is event-driven: the worker calls `phys_begin_*`,
the inner loop runs the op forward, and the worker polls
`phys_read_tool_status` for the relevant flag bits.

## Tuning surface

If the robot feels sluggish, tune in this order:

1. **Speed limit on curves** — the hard-coded `1.5` in
   `follower_step`. Higher = faster through tight Beziers but more
   slip.
2. **Lookahead** — `lookahead_min` too small → oscillation; too large →
   corner cutting. `lookahead_k_v` is the speed-dependent term.
3. **PID gains** — `kp` for snappiness, `ki` to cancel drag, `kd`
   sparingly. The integrator is anti-windup-clamped to
   `max_torque / ki`.
4. **Friction** — lower `lin_friction` to coast, raise to brake harder
   on `request_stop`. `ang_friction` rarely needs tuning.
5. **Acceleration caps** — `max_linear_accel`, `max_angular_accel`. The
   ramps in `compute_setpoints` enforce these.

If the robot drifts off path on long straights, the issue is almost
always the PID — the follower will hold zero cross-track if the wheel
loops can hit their setpoints.
