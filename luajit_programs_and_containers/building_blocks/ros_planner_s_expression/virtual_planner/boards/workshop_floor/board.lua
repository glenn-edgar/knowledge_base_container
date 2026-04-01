-- virtual_node_dsl.lua -- Workshop Floor board definition
--
-- Uses vn_dsl.lua builder API for validated, consistent inputs.

local vn = require("vn_dsl")
vn.reset()

---------------------------------------------------------------------------
-- Board declaration
---------------------------------------------------------------------------
vn.board("workshop_floor", 2400, 1200)

---------------------------------------------------------------------------
-- Board nodes
---------------------------------------------------------------------------
--   [dock_north]----[inspection]----[painting]
--        |               |              |
--   [corridor_n]----[crossroads]---[corridor_e]
--        |               |              |
--   [warehouse]-----[corridor_s]---[assembly]
--        |               |              |
--   [launch]--------[gate_zone]----[shipping]

vn.node("launch",       0,    0,    "base")
vn.node("gate_zone",    800,  0,    "mission")
vn.node("shipping",     1600, 0,    "mission")
vn.node("warehouse",    0,    400,  "waypoint")
vn.node("corridor_s",   800,  400,  "waypoint")
vn.node("assembly",     1600, 400,  "mission")
vn.node("corridor_n",   0,    800,  "waypoint")
vn.node("crossroads",   800,  800,  "waypoint")
vn.node("corridor_e",   1600, 800,  "waypoint")
vn.node("dock_north",   0,    1200, "waypoint")
vn.node("inspection",   800,  1200, "mission")
vn.node("painting",     1600, 1200, "mission")

---------------------------------------------------------------------------
-- Paths (all bidirectional)
---------------------------------------------------------------------------
-- vn.path(from, to, nav_method, speed, waypoints, weight_override)
-- Weight auto-calculated from Euclidean distance if not provided.

vn.path("launch",     "gate_zone",   "spline_follow", 150,
        { {0,0}, {200,10}, {600,-5}, {800,0} })
vn.path("launch",     "warehouse",   "spline_follow", 120,
        { {0,0}, {-10,100}, {5,300}, {0,400} })
vn.path("gate_zone",  "shipping",    "spline_follow", 150)
vn.path("gate_zone",  "corridor_s",  "spline_follow", 150)
vn.path("warehouse",  "corridor_s",  "spline_follow", 150)
vn.path("warehouse",  "corridor_n",  "line_follow",   120)
vn.path("corridor_s", "assembly",    "spline_follow", 150)
vn.path("corridor_s", "crossroads",  "spline_follow", 150)
vn.path("corridor_n", "crossroads",  "spline_follow", 150)
vn.path("corridor_n", "dock_north",  "line_follow",   120)
vn.path("crossroads", "corridor_e",  "spline_follow", 150)
vn.path("crossroads", "inspection",  "spline_follow", 130)
vn.path("corridor_e", "assembly",    "spline_follow", 130)
vn.path("corridor_e", "painting",    "spline_follow", 130)
vn.path("dock_north", "inspection",  "spline_follow", 130)
vn.path("inspection", "painting",    "spline_follow", 130)
vn.path("shipping",   "assembly",    "wall_ride",     100, nil, nil,
        { wall_standoff = 30 })  -- 30mm from wall

---------------------------------------------------------------------------
-- Mission catalog
---------------------------------------------------------------------------
-- vn.mission(name, board_node, approach_heading, points, params, preconditions)

vn.mission("deliver_part", "assembly", 270, 15, {
  arm_target = -45,
  arm_speed  = 80,
  payload    = "part",
  arm_return = 0,
})

vn.mission("paint_sample", "painting", 270, 20, {
  arm_target = -60,
  arm_speed  = 60,
  hold_time  = 500,
  arm_return = 0,
})

vn.mission("pass_gate", "gate_zone", 0, 25, {
  rpc_open      = "gate.open",
  rpc_close     = "gate.close",
  drive_through = 200,
})

vn.mission("load_shipping", "shipping", 0, 20, {
  arm_target = -30,
  arm_speed  = 80,
  payload    = "container",
  arm_return = 0,
}, { "pass_gate" })

vn.mission("inspection_scan", "inspection", 0, 20, {
  sensor_port = "inspect",
  sensor_type = "color",
})

---------------------------------------------------------------------------
-- Build and return validated descriptor
---------------------------------------------------------------------------
vn.summary()
return vn.build()
