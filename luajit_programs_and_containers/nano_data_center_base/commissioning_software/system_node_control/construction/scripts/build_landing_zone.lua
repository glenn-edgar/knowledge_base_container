#!/usr/bin/env luajit
-- =============================================================================
-- build_landing_zone.lua -- Generate landing_zone.json for the moon_base site.
--
-- Hand-authored source-of-truth for the v3 reference board. Emits a JSON
-- artifact ready for `upload_board.lua --name landing_zone --file ...`.
--
-- This is a commissioning-time tool. In production an external authoring
-- tool (GIS / GPS waypoint planner) will emit the same JSON shape; this
-- script is the canonical reference for that shape and is also used to
-- regenerate the fixture if the source tables below change.
--
-- Format: schema_version 1. See construction/subsystems/BOARD_FORMAT.md
-- for the full spec.
--
-- Usage:
--   build_landing_zone.lua [--out <path.json>]  [--pretty]
--
-- Default output path:
--   ../../../../nano_data_center_instance/configurations/moon_base_alpha/
--     file_scripts/boards/landing_zone.json
-- =============================================================================

local dkjson = require("dkjson")

---------------------------------------------------------------------------
-- arg parsing
---------------------------------------------------------------------------

local opts = { pretty = true }
local i = 1
while i <= #arg do
    if     arg[i] == "--out"    then opts.out = arg[i + 1]; i = i + 2
    elseif arg[i] == "--pretty" then opts.pretty = true; i = i + 1
    elseif arg[i] == "--compact" then opts.pretty = false; i = i + 1
    elseif arg[i] == "-h" or arg[i] == "--help" then
        io.stderr:write("Usage: build_landing_zone.lua [--out <path.json>] [--pretty|--compact]\n")
        os.exit(2)
    else
        io.stderr:write("unknown arg: " .. tostring(arg[i]) .. "\n"); os.exit(2)
    end
end

-- Default output path resolves relative to this script.
if not opts.out then
    local here = arg[0]:match("(.*/)") or "./"
    -- scripts/ → construction/ → system_node_control/ → commissioning_software/
    --        → nano_data_center_base/ → luajit_programs_and_containers/
    opts.out = here
        .. "../../../../../nano_data_center_instance/configurations/"
        .. "moon_base_alpha/file_scripts/boards/landing_zone.json"
end

---------------------------------------------------------------------------
-- metadata
---------------------------------------------------------------------------

local metadata = {
    name        = "landing_zone",
    description = "Lunar surface ops board: lander pad, charging stations, "
               .. "construction/paint module-placement stops, mining zones, "
               .. "and inspection points. Mixed open-terrain spline + "
               .. "painted-line follow for the long charging traverses.",
    author      = "system_node_control/construction/scripts/build_landing_zone.lua",
    created_at  = os.date("!%Y-%m-%dT%H:%M:%SZ"),

    -- Local ENU is the only frame the planner's Euclidean math handles
    -- correctly. Offline tools that work in lat/lon convert to local x/y
    -- on emit.
    coordinate_system = {
        frame              = "local_enu",
        units              = "mm",
        origin             = { lat = -89.9000, lon = 0.0000, alt_m = -120 },
        heading_offset_deg = 0,
    },

    bounds = { x_min = -200, x_max = 2400, y_min = -200, y_max = 2200 },
}

---------------------------------------------------------------------------
-- ui registry (consumed by the map web server)
---------------------------------------------------------------------------

local ui = {
    node_styles = {
        base            = { icon = "rocket",     color = "#888", label = "Base" },
        transit         = { icon = "dot",        color = "#aaa", size  = "small" },
        recharge        = { icon = "battery",    color = "#0a0", label = "Charging" },
        deliver_part    = { icon = "package",    color = "#06c", label = "Delivery" },
        load_shipping   = { icon = "crate",      color = "#06c", label = "Shipping" },
        paint_sample    = { icon = "paintbrush", color = "#c0c", label = "Paint" },
        inspection_scan = { icon = "magnifier",  color = "#c60", label = "Inspection" },
        pass_gate       = { icon = "doorway",    color = "#888", label = "Gate" },
    },
    edge_styles = {
        path_spline = { color = "#69f", stroke = "solid",  width = 2 },
        path_line   = { color = "#666", stroke = "dotted", width = 2 },
        path_wall   = { color = "#a00", stroke = "dashed", width = 2 },
    },
}

---------------------------------------------------------------------------
-- nodes (15 total)
--   start         : lander_pad
--   gate          : habitat_site
--   waypoints     : junction_central, junction_north, transit_*
--   charging      : charging_station_a, charging_station_b   (2 per A.4f spec)
--   module-stops  : construction_bay, paint_station, mining_zone_a   (3)
--   inspection    : survey_point_1, inspection_scan
--   target        : mining_zone_b
---------------------------------------------------------------------------

local nodes = {
    -- Start
    { name = "lander_pad",         x =    0, y =    0, type = "base",
      description = "Lander touchdown pad and mission start." },

    -- Gate to the surface ops zone
    { name = "habitat_site",       x =  800, y =    0, type = "pass_gate",
      params = { gate_id = "hab_north", drive_through = 600 },
      description = "Habitat air-lock pass-through gate." },

    -- Transit waypoints (no mission stops here)
    { name = "junction_central",   x =  800, y =  400, type = "transit" },
    { name = "junction_north",     x = 1200, y =    0, type = "transit" },

    -- Two charging stations (A.4f requires at least two)
    { name = "charging_station_a", x =  800, y =  800, type = "recharge",
      params = { contact_target = "ground_pad", hold_time = 30000 },
      description = "Primary charging pad next to the central junction." },

    { name = "charging_station_b", x = 1600, y = 1200, type = "recharge",
      params = { contact_target = "ground_pad", hold_time = 30000 },
      description = "Secondary charging pad on the eastern transit ring; "
                 .. "used when central pad is occupied or out of range." },

    -- Inspection points
    { name = "survey_point_1",     x =    0, y =  800, type = "inspection_scan",
      params = { sensor_port = 0, sensor_type = 1 },
      description = "Surface composition sample point west of the lander." },

    { name = "inspection_scan",    x = 1700, y =    0, type = "inspection_scan",
      params = { sensor_port = 0, sensor_type = 2 },
      description = "Mining-area inspection station." },

    -- Construction zone transit lattice
    { name = "transit_build_n",    x =  800, y = 1400, type = "transit" },
    { name = "transit_build_w",    x =  600, y = 1600, type = "transit" },

    -- Three module-placement stops
    { name = "construction_bay",   x =  800, y = 1600, type = "load_shipping",
      params = { arm_target = 30, payload_type = 2, arm_speed = 90 },
      description = "Module-placement bay; arm extends to 30°, payload type 2." },

    { name = "paint_station",      x = 1000, y = 1800, type = "paint_sample",
      params = { arm_target = 15, hold_time = 500, arm_speed = 60 },
      description = "Paint-sample station; arm holds at 15° for 500 ms." },

    { name = "mining_zone_a",      x = 1550, y =  -80, type = "deliver_part",
      params = { arm_target = -45, payload_type = 1, arm_speed = 80 },
      description = "Mining sample drop-off A; arm sweeps to -45°." },

    -- Mining transit + target
    { name = "transit_mine_w",     x = 1500, y =    0, type = "transit" },

    { name = "mining_zone_b",      x = 1600, y = 2000, type = "deliver_part",
      params = { arm_target = -45, payload_type = 1, arm_speed = 80 },
      description = "Final delivery target; long traverse from the lander." },
}

---------------------------------------------------------------------------
-- edges
--
-- weight scales with terrain difficulty (open spline ≈ 1× distance,
-- painted-line follow ≈ 1.5× to discourage when not needed, etc.).
--
-- path = {} → auto-interpolate two midpoints (smooth open curve).
-- path = { {x=...,y=...}, ... } → hand-authored waypoints (curved
-- approaches into module-placement stops).
---------------------------------------------------------------------------

local edges = {
    -- Lander → habitat / survey (spline; open terrain, equal weight)
    { from = "lander_pad",         to = "habitat_site",
      nav = "path_spline", speed = 150, weight =  800, path = {},
      description = "Lander → habitat via open spline." },

    { from = "lander_pad",         to = "survey_point_1",
      nav = "path_spline", speed = 120, weight =  800, path = {} },

    -- Habitat node spreads
    { from = "habitat_site",       to = "junction_north",
      nav = "path_spline", speed = 150, weight =  400, path = {} },

    { from = "habitat_site",       to = "junction_central",
      nav = "path_spline", speed = 140, weight =  400, path = {} },

    -- Junction → charging A → build transit
    { from = "junction_central",   to = "charging_station_a",
      nav = "path_spline", speed = 130, weight =  400, path = {} },

    { from = "charging_station_a", to = "transit_build_n",
      nav = "path_spline", speed = 130, weight =  600, path = {} },

    -- Survey → charging A (rough, hilly: weight much higher than distance)
    { from = "survey_point_1",     to = "charging_station_a",
      nav = "path_spline", speed = 100, weight = 1131, path = {},
      description = "Cross-traverse over rough terrain; weight intentionally "
                 .. "higher than Euclidean distance to discourage routing." },

    -- Survey loop: painted-line follow up the western edge
    { from = "survey_point_1",     to = "transit_build_w",
      nav = "path_line",   speed =  90, weight = 1200, path = {},
      description = "Painted lane along the western mast; line-follow vehicle "
                 .. "speed limited by paint visibility." },

    -- Construction zone lattice
    { from = "transit_build_n",    to = "transit_build_w",
      nav = "path_spline", speed = 120, weight =  280, path = {} },

    -- Curved docking approaches into the construction bay (hand-authored)
    { from = "transit_build_n",    to = "construction_bay",
      nav = "path_spline", speed =  80, weight =  220,
      path = { {x=780, y=1450}, {x=760, y=1520}, {x=780, y=1570} },
      description = "Curved approach from north; sweeps slightly west to "
                 .. "clear the gantry leg before final straight-in." },

    { from = "transit_build_w",    to = "construction_bay",
      nav = "path_spline", speed =  80, weight =  220,
      path = { {x=650, y=1580}, {x=700, y=1560}, {x=750, y=1580} },
      description = "Curved approach from west." },

    -- Paint station from north transit (curved approach)
    { from = "transit_build_n",    to = "paint_station",
      nav = "path_spline", speed =  80, weight =  280,
      path = { {x=850, y=1520}, {x=950, y=1700}, {x=1000, y=1780} },
      description = "Curved approach to paint station from north transit." },

    -- North → mining transit → mining_zone_a / inspection_scan
    { from = "junction_north",     to = "transit_mine_w",
      nav = "path_spline", speed = 150, weight =  300, path = {} },

    { from = "transit_mine_w",     to = "mining_zone_a",
      nav = "path_spline", speed = 100, weight =  100, path = {} },

    { from = "transit_mine_w",     to = "inspection_scan",
      nav = "path_spline", speed = 100, weight =  220, path = {},
      description = "Spur from mining transit to the inspection station." },

    -- Long traverses to the eastern target
    { from = "junction_central",   to = "mining_zone_b",
      nav = "path_spline", speed = 130, weight =  894, path = {},
      description = "Direct long traverse via central junction." },

    -- Inter-charging painted lane (long; line-follow over an established route)
    { from = "charging_station_a", to = "charging_station_b",
      nav = "path_line",   speed =  90, weight = 1500, path = {},
      description = "Painted-lane shuttle between primary and secondary "
                 .. "charging pads. Used when one pad is unavailable." },

    -- Charging B onward to target
    { from = "charging_station_b", to = "mining_zone_b",
      nav = "path_spline", speed = 130, weight =  700, path = {} },
}

---------------------------------------------------------------------------
-- Build flat-array paths from the {x,y} waypoint form, with from/to
-- coordinates auto-prepended/appended. Empty path → interpolate two
-- midpoints. See BOARD_FORMAT.md "Edge (v1)" for the on-disk contract.
---------------------------------------------------------------------------

local function interpolate(ax, ay, bx, by, n)
    local pts = {}
    for k = 1, n do
        local t = k / (n + 1)
        pts[#pts + 1] = {
            x = math.floor(ax + (bx - ax) * t + 0.5),
            y = math.floor(ay + (by - ay) * t + 0.5),
        }
    end
    return pts
end

local function build_paths(node_list, edge_list, interp_n)
    interp_n = interp_n or 2

    local lookup = {}
    for _, n in ipairs(node_list) do
        assert(n.name, "node missing name")
        assert(n.x and n.y, "node " .. n.name .. " missing x/y")
        assert(not lookup[n.name], "duplicate node " .. n.name)
        lookup[n.name] = n
    end

    local seen = {}
    for _, e in ipairs(edge_list) do
        assert(e.from and e.to, "edge missing from/to")
        local fn = lookup[e.from] or error("unknown from-node: " .. e.from)
        local tn = lookup[e.to]   or error("unknown to-node: "   .. e.to)
        local lo = e.from < e.to and e.from or e.to
        local hi = e.from < e.to and e.to   or e.from
        local key = lo .. "|" .. hi
        assert(not seen[key], "duplicate undirected edge " .. e.from .. " ↔ " .. e.to)
        seen[key] = true

        local waypoints = e.path or {}
        if #waypoints == 0 and interp_n > 0 then
            waypoints = interpolate(fn.x, fn.y, tn.x, tn.y, interp_n)
        end

        local flat = { fn.x, fn.y }
        for _, wp in ipairs(waypoints) do
            assert(wp.x and wp.y,
                "waypoint missing x/y on edge " .. e.from .. "→" .. e.to)
            flat[#flat + 1] = wp.x
            flat[#flat + 1] = wp.y
        end
        flat[#flat + 1] = tn.x
        flat[#flat + 1] = tn.y
        e.path = flat
    end
end

build_paths(nodes, edges, 2)

---------------------------------------------------------------------------
-- assemble + emit
---------------------------------------------------------------------------

local board = {
    schema_version = 1,
    metadata       = metadata,
    ui             = ui,
    nodes          = nodes,
    edges          = edges,
    markers        = {},   -- v2 reserved
}

local encoded
if opts.pretty then
    -- dkjson honors "indent" in encode opts.
    encoded = dkjson.encode(board, { indent = true, keyorder = {
        "schema_version", "metadata", "ui", "nodes", "edges", "markers",
        "name", "x", "y", "type", "params", "description", "ui",
        "from", "to", "nav", "speed", "weight", "path", "subpaths",
    } })
else
    encoded = dkjson.encode(board)
end

-- ensure target directory exists
local out_dir = opts.out:match("(.*/)")
if out_dir and out_dir ~= "" then
    os.execute("mkdir -p " .. ("'" .. out_dir:gsub("'", "'\\''") .. "'"))
end

local f, err = io.open(opts.out, "wb")
if not f then
    io.stderr:write("cannot write " .. opts.out .. ": " .. tostring(err) .. "\n")
    os.exit(1)
end
f:write(encoded)
if opts.pretty and not encoded:match("\n$") then f:write("\n") end
f:close()

io.stdout:write(string.format(
    "wrote %s (nodes=%d, edges=%d, %d bytes)\n",
    opts.out, #nodes, #edges, #encoded))
