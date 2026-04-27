# `physics_config.json`

Physics model loaded by the HAL. Default values given are the ones in
the file checked into this directory; missing fields fall back to
`physics_ffi.new`'s defaults.

## Top-level

```json
{
  "kinematics": "diff",
  "chassis":  { ... },
  "motors":   { ... },
  "path_follower": { ... },
  "sensors":  { ... },
  "tools":    [ ... ],
  "initial_pose": { "x": 0, "y": 0, "heading": 0 },
  "battery": { "capacity_j": 100000.0, "initial_j": 100000.0 }
}
```

`kinematics` is informational; only `diff` is implemented.

## `chassis`

| Field | Default | Effect |
|---|---|---|
| `wheelbase_m`     | 0.30 | Distance between left/right wheels. Used in body→wheel and wheel→body. |
| `wheel_radius_m`  | 0.04 | Converts torque ↔ force (`F = τ/r`). |
| `mass_kg`         | 8.0  | Plus payload, splits 50/50 across wheels for inertia. |
| `inertia_kg_m2`   | 0.12 | Reserved (currently unused beyond capture). |
| `lin_friction`    | 0.8  | Viscous loss `F_loss = lin_friction * v`. |
| `ang_friction`    | 0.4  | `ω -= ang_friction * ω * dt` per chassis step. |

## `motors`

| Field | Default | Effect |
|---|---|---|
| `max_torque_nm` | 1.5 | Torque cap; integrator anti-windup uses it. |
| `max_wheel_rad_s` | 30.0 | Wheel-speed cap. |
| `pid.kp / ki / kd` | 8 / 4 / 0.05 | Per-wheel speed loop. |
| `energy_k` | 1.0 | Multiplier on `Σ |τ·ω| dt` into joules. |

## `path_follower`

| Field | Default | Notes |
|---|---|---|
| `lookahead_min_m`     | 0.20 | Minimum lookahead distance. |
| `lookahead_k_v`       | 0.40 | `lookahead = min + k_v * |v_cmd|`. |
| `arrival_tol_m`       | 0.05 | When `dist_to_endpoint < tol` or `s ≥ arc_len - tol`, segment pops. |
| `heading_tol_rad`     | 0.05 | Rotate completes when `|err| < tol`. |
| `max_linear_accel`    | 1.0  | `v_cmd` ramp limit. |
| `max_angular_accel`   | 3.0  | `ω` ramp limit. |
| `cross_track_abort_m` | 20.0 | Drift > tol → `PATH_F_FAULT`. Raised from 5.0; see [continuous-motion](../architecture/continuous-motion.md#cross-track-abort). |
| `queue_capacity`      | 32   | Hard upper bound = `QUEUE_CAP` in C. |
| `stopped_eps_v`       | 0.01 | Below this lin speed counts as stopped. |
| `stopped_eps_omega`   | 0.02 | Below this ang speed counts as stopped. |
| `inner_dt_s`          | 0.005 | Inner-loop step (200 Hz). |

## `sensors`

| Block | Field | Default | Effect |
|---|---|---|---|
| `gps`     | `pose_sigma_xy` | 0 | Gaussian noise on `read_pose().x/y`. |
| `gps`     | `pose_sigma_h`  | 0 | Gaussian noise on `read_pose().heading`. |
| `gps`     | `rate_hz`       | 20 | Reserved (not enforced today). |
| `imu`     | `yaw_rate_sigma` | 0 | Reserved. |
| `imu`     | `bias_drift`    | 0 | Reserved. |
| `battery` | `noise_pct`     | 0 | Reserved. |

`read_pose_truth()` always returns ground truth regardless of these.

## `tools`

Array. Slots 0..7. Each entry needs at minimum `slot`, `name`, `kind`.
Recognised `kind` values: `revolute_1dof`, `binary`, `passive_dock`.

```json
{ "slot": 0, "name": "arm",
  "kind": "revolute_1dof",
  "limits_deg": [-30, 180],
  "max_speed_dps": 60,
  "max_torque": 1.5 }
```

| Kind | Required fields | Notes |
|---|---|---|
| `revolute_1dof` | `limits_deg [min, max]`, `max_speed_dps` | Lua converts both to radians for C. `transition_ms` ignored. |
| `binary`        | `transition_ms` | `states` array is informational. |
| `passive_dock`  | `offset_xyz`, `tolerance_m`, `tolerance_h_deg` | Used for `begin_dock` and `begin_charge` with charger stations. |

The names are the keys workers / tests pass instead of slot numbers
(`hal:begin_grip("gripper")`).

## `initial_pose`, `battery`

Both straight pass-through to `phys_set_initial_pose` and
`phys_set_battery`.
