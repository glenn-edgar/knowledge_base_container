# Sequencer

Stateful route execution with replan support.

## Usage

```lua
local sequencer = require("sequencer")
local seq = sequencer.new({
    robot_id    = "rover_1",
    db_file     = "surface_ops.db",
    site        = site,
    nats_server = nats_server,
    mqtt_hub    = mqtt_hub,
})

seq:load_route(route)

local valid, errors = seq:validate_route()
local result = seq:run()

if result.needs_replan then
    seq:load_route(new_route)
    result = seq:run()
end

seq:finish_mission(result)
seq:shutdown()
```

## Result

```lua
{
    success      = true,
    needs_replan = false,
    final_pose   = { x=0, y=0, heading=270, arm_angle=-135 },
    completed    = 18,
    total        = 18,
    elapsed_ms   = 1234,
    fault        = nil,  -- or { reason="timeout", action_index=5, kb_name="path_wall" }
}
```

## Replan

On fault with `needs_replan = true`, the caller (action_server) can:

1. Get remaining stops from plan_info legs
2. Block the faulted edge on the planner
3. Rebuild route from current position
4. Load new route and run again (up to 3 replans)

Global pose persists across replans. Mission telemetry continues.
