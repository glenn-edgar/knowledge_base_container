# HAL — `robot_hal.lua`

Hardware-abstraction selector. Today only `mode = "sim"` is implemented;
the file is structured so a real-hardware backend can be slotted in
without changes elsewhere.

## Surface

The HAL surface is intentionally a 1:1 mirror of `physics_ffi`'s OO handle
plus the flag-constant tables. A real backend has to implement the same
methods — anything that consumes the HAL must only depend on this set:

- `step(dt)` — sim only; real HAL ignores
- `sim_time()` → seconds
- `read_pose()` → `{ x, y, heading, v, omega, sim_t }` (with sensor noise)
- `read_pose_truth()` → same, ground truth, no noise
- `read_path_status()` → `{ flags, active_seg_id, last_done_seg_id,
  queue_depth, active_progress, cross_track_err, heading_err,
  energy_used_total, v_cmd }`
- `read_tool_status(slot)` → `{ flags, kind, value, target, payload_mass,
  battery_j, battery_capacity_j }`
- Path queue ops: `push_line / push_spline / push_rotate`,
  `request_stop / release_stop / is_stopped`, `abort_path`,
  `queue_depth / active_seg_id / last_done_seg_id`
- Tool ops: `begin_tool_move / begin_grip / begin_release / begin_dock /
  begin_charge`
- Lookups: `station_at_pose / battery_j / payload_mass`
- Flag tables: `PATH_F`, `TOOL_F`, `TOOL_KIND`, `STATION_KIND`

## Mode selection

```lua
local mode = opts.mode or os.getenv("HAL_MODE") or "sim"
```

`"sim"` loads `physics_ffi`, reads `physics_config.json` and `sim_map.json`
(from `opts.dir` if provided, else CWD), constructs a handle, and sets
`hal.mode = "sim"`. Anything else errors today.

## File layout

The HAL expects two JSON files in the same directory as the robot's main
config:

- `physics_config.json` — chassis, motors, follower, sensors, tools,
  initial pose, battery
- `sim_map.json` — passive fixtures (stations) the robot can dock at

Both are passed to `physics_ffi.new` to populate the C plant. See
[Physics config](../physics/config.md) for the field-by-field map.
