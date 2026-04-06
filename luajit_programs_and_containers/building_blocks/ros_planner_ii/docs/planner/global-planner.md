# Global Planner

Dijkstra shortest-path planner over the board waypoint graph.

## Usage

```lua
local global_planner = require("global_planner")
local planner = global_planner.new({
    db_file    = "surface_ops.db",
    board_name = "landing_zone",
})

local path = planner:find_path("lander_pad", "mining_zone_a")
-- path = { "lander_pad", "habitat_site", "mining_zone_a" }
-- cost = 1600

planner:close()
```

## Edge Blocking

On fault, the sequencer can block the edge that caused the failure and replan:

```lua
planner:mark_blocked("habitat_site", "mining_zone_a")
local alt_path = planner:find_path("lander_pad", "mining_zone_a")
-- finds alternative route via charging_station
```
