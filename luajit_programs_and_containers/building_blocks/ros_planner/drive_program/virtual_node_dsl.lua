-- virtual_node_dsl.lua -- Global planner virtual node graph
--
-- The global planner operates on virtual nodes. A virtual node is any
-- named state the robot can be in -- a board position OR an arm configuration.
-- Dijkstra routes through this unified graph. Each edge carries the data
-- needed for the local planner to compile a hidden node pattern.
--
-- Virtual node types:
--   "board"  -- physical position on the competition table
--   "arm"    -- named arm/actuator configuration
--
-- Edge types (determined by source/target node types):
--   board -> board : path following (spline, line, wall_ride)
--   arm   -> arm   : arm trajectory (joint interpolation)
--   board -> arm   : at position, move arm
--   arm   -> board : arm done, resume driving
--
-- The global planner only sees nodes and weights.
-- The local planner uses edge data to compile hidden node patterns.
-- The local tracker monitors execution via scan tree bitmaps.

local virtual_nodes = {
  name = "workshop_floor",
  dimensions = { x = 2400, y = 1200 },  -- mm (board only)

  ---------------------------------------------------------------------------
  -- Board position nodes
  ---------------------------------------------------------------------------
  -- Physical locations on the competition table.
  -- x, y in mm from board origin. heading is approach angle in degrees.
  board_nodes = {
    launch       = { x = 0,    y = 0,    kind = "base",     heading = 0   },
    gate_zone    = { x = 800,  y = 0,    kind = "mission",  heading = 0   },
    shipping     = { x = 1600, y = 0,    kind = "mission",  heading = 0   },
    warehouse    = { x = 0,    y = 400,  kind = "waypoint", heading = 90  },
    corridor_s   = { x = 800,  y = 400,  kind = "waypoint", heading = 0   },
    assembly     = { x = 1600, y = 400,  kind = "mission",  heading = 270 },
    corridor_n   = { x = 0,    y = 800,  kind = "waypoint", heading = 90  },
    crossroads   = { x = 800,  y = 800,  kind = "waypoint", heading = 0   },
    corridor_e   = { x = 1600, y = 800,  kind = "waypoint", heading = 0   },
    dock_north   = { x = 0,    y = 1200, kind = "waypoint", heading = 90  },
    inspection   = { x = 800,  y = 1200, kind = "mission",  heading = 0   },
    painting     = { x = 1600, y = 1200, kind = "mission",  heading = 270 },
  },

  ---------------------------------------------------------------------------
  -- Arm configuration nodes
  ---------------------------------------------------------------------------
  -- Named arm positions. angle is degrees from home (0).
  -- speed is degrees/second for the movement.
  -- Each arm node is associated with a board node where it is valid.
  -- The arm can only be commanded when the robot is at the correct
  -- board position (enforced by the graph -- no edge exists otherwise).
  arm_nodes = {
    arm_home        = { angle = 0,   speed = 100, at_board = nil       },
    arm_deliver     = { angle = -45, speed = 80,  at_board = "assembly"  },
    arm_paint_down  = { angle = -60, speed = 60,  at_board = "painting"  },
    arm_paint_up    = { angle = 0,   speed = 60,  at_board = "painting"  },
    arm_load        = { angle = -30, speed = 80,  at_board = "shipping"  },
  },

  ---------------------------------------------------------------------------
  -- Edges
  ---------------------------------------------------------------------------
  -- { from, to, weight, edge_type, data, bidirectional }
  --
  -- weight: cost for Dijkstra. Board edges use distance in mm.
  --         Arm edges use time estimate in ms.
  --         Cross-type edges (board<->arm) use the non-zero cost.
  --
  -- edge_type:
  --   "path"     -- board->board, robot follows path geometry
  --   "arm_move" -- arm->arm, joint interpolation
  --   "at_node"  -- board->arm or arm->board, transition at same position
  --
  -- data: edge-type-specific payload for local planner
  --   path:     { nav, speed, waypoints (optional spline points) }
  --   arm_move: { speed } (angle comes from target arm node)
  --   at_node:  {} (no additional data needed)
  --
  -- bidirectional: true for board paths (robot can traverse both ways).
  --   Arm and at_node edges are listed explicitly per direction.

  edges = {
    -- Board <-> Board (path following, bidirectional)
    { "launch",      "gate_zone",    800,  "path", { nav = "spline_follow", speed = 150, waypoints = { {0,0}, {200,10}, {600,-5}, {800,0} } }, true },
    { "launch",      "warehouse",    400,  "path", { nav = "spline_follow", speed = 120, waypoints = { {0,0}, {-10,100}, {5,300}, {0,400} } }, true },
    { "gate_zone",   "shipping",     800,  "path", { nav = "spline_follow", speed = 150 }, true },
    { "gate_zone",   "corridor_s",   400,  "path", { nav = "spline_follow", speed = 150 }, true },
    { "warehouse",   "corridor_s",   800,  "path", { nav = "spline_follow", speed = 150 }, true },
    { "warehouse",   "corridor_n",   400,  "path", { nav = "line_follow",   speed = 120 }, true },
    { "corridor_s",  "assembly",     800,  "path", { nav = "spline_follow", speed = 150 }, true },
    { "corridor_s",  "crossroads",   400,  "path", { nav = "spline_follow", speed = 150 }, true },
    { "corridor_n",  "crossroads",   800,  "path", { nav = "spline_follow", speed = 150 }, true },
    { "corridor_n",  "dock_north",   400,  "path", { nav = "line_follow",   speed = 120 }, true },
    { "crossroads",  "corridor_e",   800,  "path", { nav = "spline_follow", speed = 150 }, true },
    { "crossroads",  "inspection",   400,  "path", { nav = "spline_follow", speed = 130 }, true },
    { "corridor_e",  "assembly",     400,  "path", { nav = "spline_follow", speed = 130 }, true },
    { "corridor_e",  "painting",     400,  "path", { nav = "spline_follow", speed = 130 }, true },
    { "dock_north",  "inspection",   800,  "path", { nav = "spline_follow", speed = 130 }, true },
    { "inspection",  "painting",     800,  "path", { nav = "spline_follow", speed = 130 }, true },
    { "shipping",    "assembly",     400,  "path", { nav = "wall_ride",     speed = 100 }, true },

    -- Board <-> Arm transitions (at_node, explicit per direction)
    { "assembly",       "arm_deliver",    200,  "at_node",  {} },
    { "arm_deliver",    "assembly",       200,  "at_node",  {} },

    { "painting",       "arm_paint_down", 300,  "at_node",  {} },
    { "arm_paint_up",   "painting",       100,  "at_node",  {} },

    { "shipping",       "arm_load",       200,  "at_node",  {} },
    { "arm_load",       "shipping",       200,  "at_node",  {} },

    -- arm_home <-> board positions where arm ops happen.
    -- arm_home is universal (at_board = nil) so we need explicit
    -- edges to each board node that has arm operations.
    { "arm_home",       "assembly",       100,  "at_node",  {} },
    { "arm_home",       "painting",       100,  "at_node",  {} },
    { "arm_home",       "shipping",       100,  "at_node",  {} },

    -- Arm -> Arm (joint trajectory, explicit per direction)
    { "arm_paint_down", "arm_paint_up",   500,  "arm_move", { speed = 60  } },
    { "arm_deliver",    "arm_home",       300,  "arm_move", { speed = 100 } },
    { "arm_paint_up",   "arm_home",       300,  "arm_move", { speed = 100 } },
    { "arm_load",       "arm_home",       300,  "arm_move", { speed = 100 } },
    { "arm_home",       "arm_deliver",    300,  "arm_move", { speed = 80  } },
    { "arm_home",       "arm_paint_down", 400,  "arm_move", { speed = 60  } },
    { "arm_home",       "arm_load",       200,  "arm_move", { speed = 80  } },
  },
}

return virtual_nodes
