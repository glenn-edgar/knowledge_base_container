-- board_dsl.lua — Workshop Floor topological map
--
-- Defines the competition board as a topological graph for the drive program.
-- Three levels of data, one structure:
--   1. Nodes   — Dijkstra graph vertices (named places with coordinates)
--   2. Connections — Dijkstra graph edges (weighted, with nav method)
--   3. Splines — Catmull-Rom control points per connection (execution geometry)
--
-- Dijkstra sees only nodes and weights.
-- The executor sees spline geometry and nav method.
-- Status field is live — updated at runtime when paths fail.

board = {
  name = "workshop_floor",
  dimensions = { x = 2400, y = 1200 },  -- mm

  -- Node kinds:
  --   "base"     — launch/home area (start and end of match)
  --   "mission"  — location where a mission action occurs
  --   "waypoint" — routing-only node (no mission, just connectivity)
  --
  -- Layout:
  --   [dock_north]----[inspection]----[painting]
  --        |               |              |
  --   [corridor_n]----[crossroads]---[corridor_e]
  --        |               |              |
  --   [warehouse]-----[corridor_s]---[assembly]
  --        |               |              |
  --   [launch]--------[gate_zone]----[shipping]
  --                        ^
  --                   (hub controls gate)

  nodes = {
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

  -- Connections: Dijkstra graph edges
  -- Bidirectional by default — one DSL entry generates two DB rows.
  -- nav_method: how the robot traverses this edge
  --   "spline_follow" — follow Catmull-Rom spline (default)
  --   "line_follow"   — follow a line on the board surface
  --   "wall_ride"     — follow a wall edge
  -- status: "clear" or "blocked" (updated at runtime by drive program)
  --
  -- { from, to, weight_mm, nav_method, status }
  connections = {
    { "launch",      "gate_zone",    800,  "spline_follow", "clear" },
    { "launch",      "warehouse",    400,  "spline_follow", "clear" },
    { "gate_zone",   "shipping",     800,  "spline_follow", "clear" },
    { "gate_zone",   "corridor_s",   400,  "spline_follow", "clear" },
    { "warehouse",   "corridor_s",   800,  "spline_follow", "clear" },
    { "warehouse",   "corridor_n",   400,  "line_follow",   "clear" },
    { "corridor_s",  "assembly",     800,  "spline_follow", "clear" },
    { "corridor_s",  "crossroads",   400,  "spline_follow", "clear" },
    { "corridor_n",  "crossroads",   800,  "spline_follow", "clear" },
    { "corridor_n",  "dock_north",   400,  "line_follow",   "clear" },
    { "crossroads",  "corridor_e",   800,  "spline_follow", "clear" },
    { "crossroads",  "inspection",   400,  "spline_follow", "clear" },
    { "corridor_e",  "assembly",     400,  "spline_follow", "clear" },
    { "corridor_e",  "painting",     400,  "spline_follow", "clear" },
    { "dock_north",  "inspection",   800,  "spline_follow", "clear" },
    { "inspection",  "painting",     800,  "spline_follow", "clear" },
    { "shipping",    "assembly",     400,  "wall_ride",     "clear" },
  },

  -- Spline control points per connection.
  -- Catmull-Rom waypoints — the compiler generates Bezier control points
  -- via the spline.lua module.
  -- Connections without an explicit spline entry get an auto-generated
  -- straight-line spline from source (x,y) to target (x,y).
  splines = {
    { connection = { "launch", "gate_zone" },
      speed = 150,
      waypoints = { {0, 0}, {200, 10}, {600, -5}, {800, 0} },
    },
    { connection = { "launch", "warehouse" },
      speed = 120,
      waypoints = { {0, 0}, {-10, 100}, {5, 300}, {0, 400} },
    },
    { connection = { "gate_zone", "corridor_s" },
      speed = 150,
      waypoints = { {800, 0}, {810, 100}, {795, 300}, {800, 400} },
    },
    { connection = { "gate_zone", "shipping" },
      speed = 150,
      waypoints = { {800, 0}, {1000, 5}, {1400, -5}, {1600, 0} },
    },
    { connection = { "corridor_s", "crossroads" },
      speed = 150,
      waypoints = { {800, 400}, {805, 500}, {795, 700}, {800, 800} },
    },
    { connection = { "crossroads", "inspection" },
      speed = 130,
      waypoints = { {800, 800}, {805, 900}, {795, 1100}, {800, 1200} },
    },
    { connection = { "corridor_e", "painting" },
      speed = 130,
      waypoints = { {1600, 800}, {1595, 900}, {1605, 1100}, {1600, 1200} },
    },
    { connection = { "corridor_e", "assembly" },
      speed = 130,
      waypoints = { {1600, 800}, {1605, 700}, {1595, 500}, {1600, 400} },
    },
  },
}

return board
