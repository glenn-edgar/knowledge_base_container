# Mission Builder

Combines global planner + route builder to create executable routes from mission commands.

## API

```lua
local mission_builder = require("mission_builder")

local route, plan_info = mission_builder.build(mission_cmd, planner, capabilities)
```

## Capability Validation

Two-phase validation:

1. **Stop actions** — checks mission stop VNs against robot capabilities before planning
2. **Full route** — checks all VNs (including navigation) after route building

Returns `nil, { error = "unsupported_capabilities", unsupported = [...] }` on failure.

## Plan Info

```lua
plan_info = {
    legs = {
        { index=1, from="lander_pad", to="mining_zone_a", route_start=2, route_end=4 },
        { index=2, from="mining_zone_a", to="lander_pad", route_start=5, route_end=8 },
    },
    total_cost = 3200,
}
```

Used by sequencer for replan: identifies which leg faulted, which stops remain.

## Replan

```lua
local new_route, new_info = mission_builder.rebuild(
    remaining_stops, planner, current_node, current_heading)
```

Called after fault. Planner has the failed edge blocked. Rebuilds route from current position to remaining stops.
