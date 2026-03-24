------------------------------------------------------------------------
-- Map DSL — FIRST LEGO League Competition Course
--
-- Defines: waypoints, navigation lines, topology edges, zones,
--          mission model positions, and delivery targets.
-- Changes when: the competition course layout changes.
--
-- Coordinate system:
--   Origin (0,0) = bottom-left corner of field interior
--   X axis       = long side (0 .. 2362 mm)
--   Y axis       = short side (0 .. 1143 mm)
--   Heading 0    = positive X (east), 90 = positive Y (north)
--
-- This file compiles into:
--   1. KB waypoint rows (ltree path: map.waypoint.<name>)
--   2. KB edge rows    (ltree path: map.edge.<from>_<to>)
--   3. KB zone rows    (ltree path: map.zone.<name>)
--   4. KB mission rows (ltree path: map.mission.<name>)
------------------------------------------------------------------------

local Map = {}

------------------------------------------------------------------------
-- 1. WAYPOINTS
--    Named positions on the field the robot can navigate to.
--    x,y in mm from origin. heading = expected robot facing (degrees).
--    nav_method: how the robot reaches this point from an adjacent edge.
------------------------------------------------------------------------

Map.waypoints = {

    -- Launch area
    { name = "home",            x =  200, y =  200, heading =  90,
      nav_method = "direct",    zone = "launch" },
    { name = "launch_exit",     x =  200, y =  450, heading =  90,
      nav_method = "direct",    zone = "launch" },

    -- Main navigation spine (black line running left-to-right)
    { name = "spine_west",      x =  300, y =  571, heading =   0,
      nav_method = "line_follow" },
    { name = "spine_center",    x = 1181, y =  571, heading =   0,
      nav_method = "line_follow" },
    { name = "spine_east",      x = 2060, y =  571, heading =   0,
      nav_method = "line_follow" },

    -- North branch line (perpendicular, runs from spine to north wall)
    { name = "branch_n_base",   x =  700, y =  571, heading =  90,
      nav_method = "line_follow" },
    { name = "branch_n_mid",    x =  700, y =  800, heading =  90,
      nav_method = "line_follow" },
    { name = "branch_n_tip",    x =  700, y = 1000, heading =  90,
      nav_method = "line_follow" },

    -- South branch line (perpendicular, runs from spine to south wall)
    { name = "branch_s_base",   x = 1500, y =  571, heading = 270,
      nav_method = "line_follow" },
    { name = "branch_s_mid",    x = 1500, y =  350, heading = 270,
      nav_method = "line_follow" },
    { name = "branch_s_tip",    x = 1500, y =  150, heading = 270,
      nav_method = "line_follow" },

    -- East loop (line loop in the east quadrant)
    { name = "loop_e_entry",    x = 1800, y =  571, heading =   0,
      nav_method = "line_follow" },
    { name = "loop_e_north",    x = 2100, y =  850, heading =  90,
      nav_method = "line_follow" },
    { name = "loop_e_far",      x = 2200, y =  571, heading = 180,
      nav_method = "line_follow" },
    { name = "loop_e_south",    x = 2100, y =  300, heading = 270,
      nav_method = "line_follow" },

    -- Wall-ride reference points (no line, use wall contact)
    { name = "wall_south_mid",  x = 1181, y =   50, heading =   0,
      nav_method = "wall_ride", wall_side = "right" },
    { name = "wall_north_mid",  x = 1181, y = 1093, heading = 180,
      nav_method = "wall_ride", wall_side = "right" },

    -- Mission approach points (close to mission models)
    { name = "m1_approach",     x =  500, y =  900, heading =  90,
      nav_method = "direct" },
    { name = "m2_approach",     x = 1181, y =  850, heading =   0,
      nav_method = "direct" },
    { name = "m3_approach",     x = 1800, y =  200, heading = 270,
      nav_method = "direct" },
    { name = "m4_approach",     x =  900, y =  300, heading = 180,
      nav_method = "direct" },
    { name = "m5_approach",     x = 2100, y =  571, heading =   0,
      nav_method = "direct" },
    { name = "m6_approach",     x =  400, y =  571, heading =  90,
      nav_method = "direct" },
}

------------------------------------------------------------------------
-- 2. EDGES
--    Connections between waypoints. Defines the navigation graph.
--    distance_mm: travel distance (may differ from Euclidean due to
--                 curves). method: how to traverse this edge.
--    bidirectional: true = robot can travel both ways.
------------------------------------------------------------------------

Map.edges = {

    -- Launch -> spine
    { from = "home",          to = "launch_exit",    distance_mm =  250,
      method = "direct",      bidirectional = true },
    { from = "launch_exit",   to = "spine_west",     distance_mm =  150,
      method = "direct",      bidirectional = true },

    -- Main spine (line-follow corridor, west to east)
    { from = "spine_west",    to = "branch_n_base",  distance_mm =  400,
      method = "line_follow", bidirectional = true },
    { from = "branch_n_base", to = "spine_center",   distance_mm =  481,
      method = "line_follow", bidirectional = true },
    { from = "spine_center",  to = "branch_s_base",  distance_mm =  319,
      method = "line_follow", bidirectional = true },
    { from = "branch_s_base", to = "loop_e_entry",   distance_mm =  300,
      method = "line_follow", bidirectional = true },
    { from = "loop_e_entry",  to = "spine_east",     distance_mm =  260,
      method = "line_follow", bidirectional = true },

    -- North branch (perpendicular line)
    { from = "branch_n_base", to = "branch_n_mid",   distance_mm =  229,
      method = "line_follow", bidirectional = true },
    { from = "branch_n_mid",  to = "branch_n_tip",   distance_mm =  200,
      method = "line_follow", bidirectional = true },

    -- South branch (perpendicular line)
    { from = "branch_s_base", to = "branch_s_mid",   distance_mm =  221,
      method = "line_follow", bidirectional = true },
    { from = "branch_s_mid",  to = "branch_s_tip",   distance_mm =  200,
      method = "line_follow", bidirectional = true },

    -- East loop (circular line path)
    { from = "loop_e_entry",  to = "loop_e_north",   distance_mm =  400,
      method = "line_follow", bidirectional = true },
    { from = "loop_e_north",  to = "loop_e_far",     distance_mm =  320,
      method = "line_follow", bidirectional = true },
    { from = "loop_e_far",    to = "loop_e_south",   distance_mm =  320,
      method = "line_follow", bidirectional = true },
    { from = "loop_e_south",  to = "loop_e_entry",   distance_mm =  400,
      method = "line_follow", bidirectional = true },

    -- Branch to mission approach (short dead-reckoned hops off the line)
    { from = "branch_n_mid",  to = "m1_approach",    distance_mm =  220,
      method = "direct",      bidirectional = true },
    { from = "spine_center",  to = "m2_approach",    distance_mm =  279,
      method = "direct",      bidirectional = true },
    { from = "branch_s_mid",  to = "m3_approach",    distance_mm =  340,
      method = "direct",      bidirectional = true },
    { from = "branch_n_base", to = "m4_approach",    distance_mm =  330,
      method = "direct",      bidirectional = true },
    { from = "loop_e_far",    to = "m5_approach",    distance_mm =  100,
      method = "direct",      bidirectional = true },
    { from = "spine_west",    to = "m6_approach",    distance_mm =  100,
      method = "direct",      bidirectional = true },

    -- Wall-ride paths (no line, follow table border)
    { from = "branch_s_tip",  to = "wall_south_mid", distance_mm =  330,
      method = "wall_ride",   wall_side = "right",  bidirectional = true },
    { from = "branch_n_tip",  to = "wall_north_mid", distance_mm =  490,
      method = "wall_ride",   wall_side = "right",  bidirectional = true },
}

------------------------------------------------------------------------
-- 3. ZONES
--    Named rectangular regions on the field.
--    Used for: rule enforcement, scoring context, navigation constraints.
------------------------------------------------------------------------

Map.zones = {

    { name = "launch",
      x = 0,    y = 0,    w = 450,  h = 450,
      rules = { "human_touch_ok", "start_programs_here" } },

    { name = "north_half",
      x = 0,    y = 571,  w = 2362, h = 572,
      rules = { "no_human_touch" } },

    { name = "south_half",
      x = 0,    y = 0,    w = 2362, h = 571,
      rules = { "no_human_touch" } },

    { name = "east_quadrant",
      x = 1800, y = 0,    w = 562,  h = 1143,
      rules = { "no_human_touch", "dense_missions" } },

    { name = "center_bridge",
      x = 1000, y = 450,  w = 362,  h = 250,
      rules = { "elevated_structure", "drive_under_or_over" } },
}

------------------------------------------------------------------------
-- 4. MISSION MODELS
--    Physical Lego structures on the mat. Each mission has:
--      position    — center of the model base (mm)
--      approach    — waypoint name to navigate to before engaging
--      interact    — how the robot physically engages the model
--      payload     — Lego piece the robot must carry/deliver (or nil)
--      points      — score value if completed
------------------------------------------------------------------------

Map.missions = {

    { name = "m1_solar_panel",
      x = 500,  y = 950,
      approach    = "m1_approach",
      interact    = "push_lever",       -- push a lever down
      push_dir    = 90,                 -- push direction (heading degrees)
      push_mm     = 40,                 -- how far to push
      payload     = nil,
      points      = 20,
    },

    { name = "m2_bridge_cross",
      x = 1181, y = 900,
      approach    = "m2_approach",
      interact    = "drive_over",       -- drive across elevated structure
      drive_mm    = 350,
      payload     = nil,
      points      = 30,
    },

    { name = "m3_cargo_deliver",
      x = 1800, y = 150,
      approach    = "m3_approach",
      interact    = "deliver_payload",  -- drop piece in target circle
      payload     = "energy_unit",      -- must be loaded at launch
      drop_method = "grip_open",
      points      = 20,
    },

    { name = "m4_wheel_spin",
      x = 900,  y = 250,
      approach    = "m4_approach",
      interact    = "spin_wheel",       -- contact and rotate a wheel
      spin_dir    = "clockwise",
      contact_mm  = 30,
      points      = 15,
    },

    { name = "m5_collect_sample",
      x = 2150, y = 571,
      approach    = "m5_approach",
      interact    = "grip_and_carry",   -- pick up a piece, bring home
      payload     = "water_sample",
      grip_angle  = 60,
      points      = 25,
    },

    { name = "m6_flip_switch",
      x = 400,  y = 600,
      approach    = "m6_approach",
      interact    = "push_lever",
      push_dir    = 0,
      push_mm     = 25,
      payload     = nil,
      points      = 10,
    },
}

------------------------------------------------------------------------
-- 5. NAVIGATION LINES
--    Describes the physical black lines printed on the mat.
--    The robot's color sensor follows these.
--    Each line is a polyline: ordered list of (x,y) control points.
--    width_mm: printed line width (affects sensor tracking).
------------------------------------------------------------------------

Map.lines = {

    { name = "main_spine",
      width_mm = 25,
      points = {
        { x =  200, y = 571 },
        { x = 2200, y = 571 },
      },
    },

    { name = "north_branch",
      width_mm = 25,
      points = {
        { x = 700, y = 571 },
        { x = 700, y = 1050 },
      },
    },

    { name = "south_branch",
      width_mm = 25,
      points = {
        { x = 1500, y = 571 },
        { x = 1500, y = 100 },
      },
    },

    { name = "east_loop",
      width_mm = 20,
      points = {
        { x = 1800, y =  571 },
        { x = 2100, y =  850 },
        { x = 2250, y =  571 },
        { x = 2100, y =  300 },
        { x = 1800, y =  571 },
      },
    },

    { name = "launch_exit_line",
      width_mm = 25,
      points = {
        { x = 200, y = 450 },
        { x = 200, y = 571 },
      },
    },
}

------------------------------------------------------------------------
-- 6. SPLINE ROUTES
--    Named multi-waypoint paths the compiler fits a Catmull-Rom spline
--    through. The robot follows the smooth curve using IMU + odometry
--    instead of stopping at each waypoint to turn.
--
--    Each route lists waypoint names in order. The compiler looks up
--    (x,y) from Map.waypoints and generates cubic Bezier segments.
--
--    nav.spline_route("route_name") in the Mission DSL expands to a
--    single SPLINE_FOLLOW slot call with the computed control points.
------------------------------------------------------------------------

Map.spline_routes = {

    -- Launch to spine west: gentle curve from home through launch_exit
    -- onto the main spine. Avoids the hard 90-degree stop-and-turn.
    launch_to_spine = {
        waypoints = { "home", "launch_exit", "spine_west" },
    },

    -- Spine west sweep: smooth S-curve from spine_west through m6,
    -- up to branch_n_base, across to m4. Covers run 1 west missions.
    west_sweep = {
        waypoints = { "spine_west", "m6_approach", "spine_west",
                      "branch_n_base", "m4_approach" },
    },

    -- East loop: smooth curve around the entire east loop without
    -- stopping at each cardinal point.
    east_loop = {
        waypoints = { "loop_e_entry", "loop_e_north", "loop_e_far",
                      "loop_e_south", "loop_e_entry" },
    },

    -- Spine traverse: smooth run along the full spine from west to
    -- the south branch junction. Used for long cross-field transits.
    spine_to_south = {
        waypoints = { "spine_west", "branch_n_base", "spine_center",
                      "branch_s_base" },
    },

    -- Return home: smooth curve from spine_west back to launch.
    spine_to_home = {
        waypoints = { "spine_west", "launch_exit", "home" },
    },
}

------------------------------------------------------------------------
-- 7. SCORING SUMMARY
--    For planner heuristics: mission value vs. estimated time cost.
--    match_time_s from Equipment.field.match_duration_s (150s).
------------------------------------------------------------------------

Map.scoring = {
    max_points          = 120,            -- sum of all mission points
    precision_tokens    = 6,              -- lose one per human touch outside launch
    token_penalty       = 5,              -- points lost per token
    partial_credit      = false,          -- FLL: mission either done or not
}

return Map
