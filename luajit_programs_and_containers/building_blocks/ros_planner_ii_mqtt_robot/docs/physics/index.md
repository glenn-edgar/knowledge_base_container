# Physics layer

The C plant + its FFI wrapper. Everything that runs at >10 Hz lives here;
Lua only talks to it through the OO handle returned by
`physics_ffi.new(physics_config, sim_map, opts)`.

## Files

- [`physics_core.c`](core.md) — the C plant. Built into `libphysics.so`.
- [Control logic](control.md) — pure-pursuit, PID, chassis, ramps, tool
  sub-loop. Read this if you're tuning the model.
- [`physics_ffi.lua`](ffi.md) — `ffi.cdef` of the export surface, the OO
  handle, and the flag-constant tables (`PATH_F`, `TOOL_F`, `TOOL_KIND`,
  `STATION_KIND`).
- [`physics_config.json`](config.md) — chassis, motors, follower, sensors,
  tools, initial pose, battery.
- [`sim_map.json`](sim-map.md) — passive fixtures (charger, load_dock,
  paint, assembly).
- [`Makefile`](../operations/build.md#libphysics) — `make` builds
  `libphysics.so` next to the source.

## Coordinate / unit conventions

| Quantity | Unit | Frame |
|---|---|---|
| Position `x, y` | metres | world |
| Heading | radians | world; 0 along +x, CCW positive |
| Linear speed | m/s | body |
| Angular speed | rad/s | body, CCW positive |
| Mass | kg |  |
| Energy | joules |  |
| Battery rate | watts |  |
| Tolerance angles in JSON | degrees | converted to rad in `physics_ffi.new` |
| Tool angles in JSON / packets | degrees | converted to rad |

Time inside the C plant is sim-clock seconds. Cap'd between calls by
`phys_step(dt_sim)` which inner-loops in steps of `inner_dt`.

## Subsystems inside `physics_core.c`

1. **RNG + noise** — xorshift64; Box-Muller Gaussian for sensor noise.
2. **Bezier helpers** — `bezier_point`, `bezier_tangent`, `build_arc_lut`,
   `arc_to_param`, `project_onto_segment`. 33-sample LUT per segment.
3. **Segment queue** — ring buffer, `q_push / q_pop / q_front / q_peek`.
   Capacity `QUEUE_CAP = 32`.
4. **Pure-pursuit follower** — `follower_step`, with separate handling
   for `SEG_ROTATE` (heading-only) and line/spline. Cross-track abort.
5. **Motor PID + chassis** — per-wheel PID with anti-windup integrator
   clamp; chassis integrates `v = ½(v_l + v_r)`,
   `ω = (v_r - v_l) / wheelbase`.
6. **Setpoint computation** — speed-ramp `v_cmd` toward follower's
   `v_want`, clamp angular accel, translate to `(v_l_target, v_r_target)`.
7. **Tools** — `revolute_1dof`, `binary` gripper, `passive_dock` for
   charger. Payload mass coupling on grip/release at a `load_dock`.
8. **Stations** — pose + tolerance window. `station_at_pose(kind)` returns
   the index of the first station whose pose is within tolerance.
9. **Battery** — joule integrator; `phys_begin_charge` flips a charger
   tool into the charge_hold state, which integrates rate_w * dt.

## Inner step

```
inner_step(dt):
    follower_step → (v_want, w_want)
    if stop_requested: v_want = 0
    compute_setpoints → ramps and translates to wheel targets
    motor_pid_step    → updates v_l, v_r, integrates energy
    chassis_step      → updates x, y, heading
    update STOPPED / TRACKING flags
    for each tool: tool_step
```

`phys_step(dt_sim)` calls `inner_step(inner_dt)` for `ceil(dt_sim/inner_dt)`
iterations. With defaults `dt_sim=0.10` and `inner_dt=0.005`, that's 20
inner steps per outer tick.
