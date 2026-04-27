# `physics_ffi.lua`

LuaJIT FFI wrapper. Loads `libphysics.so` (looking next to the script
first), runs `ffi.cdef` for every export listed in
[physics_core.md](core.md#exported-functions-ffi-surface), then exposes:

- module `M.PATH_F`, `M.TOOL_F`, `M.TOOL_KIND`, `M.STATION_KIND` —
  flag-name → bit / enum-int tables matching the C `#define`s.
- `M.new(physics_config, sim_map, opts)` — constructs an OO handle.

## Constructor

```lua
local hal = physics_ffi.new(physics_config, sim_map, { seed = 42 })
```

What it does:

1. `phys_create()` and `ffi.gc(p, phys_destroy)`.
2. Reads the `chassis`, `motors`, `path_follower`, `sensors`, `battery`,
   `initial_pose` blocks of `physics_config` and calls the matching
   `phys_set_*` functions with sensible defaults for missing fields.
3. For each entry in `physics_config.tools`, calls `phys_add_tool` with
   degree → radian conversion of `limits_deg` and `max_speed_dps`.
   Records `tools_slot_by_name[name] = slot`.
4. For each entry in `sim_map.stations`, calls `phys_add_station` with
   the right `param1 / param2` semantics for the kind:
   - charger: `param1 = charge_rate_w`
   - load_dock: `param1 = payload.mass_kg`, `param2 = pickup_arm_angle_deg`
   - paint_fixture: `param1 = paint_arm_angle_deg`
   - assembly_fixture: `param1 = deliver_arm_angle_deg`
5. Pre-allocates `phys_pose_t`, `phys_path_status_t`, `phys_tool_status_t`
   `cdata` boxes so reads don't allocate per-call.

## OO methods

The handle's methods all just plumb FFI calls + extract scalars. Notable:

- `step(dt_sim)` — advance physics
- `read_pose() / read_pose_truth()` → plain Lua tables (with `tonumber`
  coercion)
- `read_path_status()` — note: reading clears `PATH_F_SEG_DONE` on the C
  side, by design
- Tool methods (`begin_tool_move`, `begin_grip`, …) accept a slot **or a
  tool name**:
  ```lua
  hal:begin_grip("gripper")           -- or hal:begin_grip(1)
  ```
  Resolution goes through `tools_slot_by_name`.
- `station_at_pose("charger")` — accepts a `STATION_KIND` name string.

## Library loading

The loader tries, in order:

1. `<dir of physics_ffi.lua>/libphysics.so`
2. `./libphysics.so`
3. `libphysics` (system search path)

This means tests run from the source directory work without `LD_LIBRARY_PATH`
gymnastics. Containerised builds will need `libphysics.so` placed alongside
the Lua source.
