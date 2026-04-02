--[[
    test_global_planner.lua -- Unit tests for global planner:
      dijkstra, route builder, blocked edges, replanning.

    No remote process needed — pure planning tests.
]]

local global_planner = require("global_planner")
local route_builder  = require("route_builder")

local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local root_dir   = script_dir .. "../"
local db_file    = root_dir .. "hub_dsl/kb_construct/surface_ops.db"

print("=== Global Planner Test ===\n")

---------------------------------------------------------------------------
-- Test runner
---------------------------------------------------------------------------
local pass_count = 0
local fail_count = 0

local function check(name, condition, msg)
    if condition then
        pass_count = pass_count + 1
    else
        fail_count = fail_count + 1
        print(string.format("  FAIL: %s — %s", name, msg or ""))
    end
end

---------------------------------------------------------------------------
-- Create planner
---------------------------------------------------------------------------
local planner = global_planner.new({
    db_file    = db_file,
    board_name = "landing_zone",
})

local graph = planner:get_graph()

-- Verify graph loaded
check("graph has 8 nodes",
    (function()
        local n = 0; for _ in pairs(graph.nodes) do n = n + 1 end; return n
    end)() == 8,
    "expected 8 nodes")

check("lander_pad exists", graph.nodes["lander_pad"] ~= nil, "missing")
check("lander_pad coords",
    graph.nodes["lander_pad"].x == 0 and graph.nodes["lander_pad"].y == 0,
    "expected (0,0)")

-- Verify bidirectional edges (lander_pad should have 2 neighbors, but also
-- reverse edges from habitat_site and survey_point_1)
local lp_neighbors = #graph.adj["lander_pad"]
check("lander_pad has neighbors", lp_neighbors >= 2,
    "expected >= 2, got " .. lp_neighbors)

print("  Graph: 8 nodes, bidirectional edges loaded from KB\n")

---------------------------------------------------------------------------
-- Test 1: Dijkstra — lander_pad → mining_zone_b
---------------------------------------------------------------------------
print("--- Dijkstra Tests ---")

local route, info = planner:plan("lander_pad", "mining_zone_b")
check("plan lander→mining_b found", route ~= nil, "no route")
check("plan cost = 2400", info.cost == 2400,
    "expected 2400, got " .. tostring(info.cost))
check("plan path length = 4", #info.path == 4,
    "expected 4 nodes, got " .. #info.path)
check("plan path[1] = lander_pad", info.path[1] == "lander_pad", info.path[1])
check("plan path[4] = mining_zone_b", info.path[4] == "mining_zone_b",
    info.path[#info.path])

print(string.format("  Path: %s (cost=%d)", table.concat(info.path, " → "), info.cost))

---------------------------------------------------------------------------
-- Test 2: Dijkstra — lander_pad → survey_point_2
---------------------------------------------------------------------------
local route2, info2 = planner:plan("lander_pad", "survey_point_2")
check("plan lander→survey2 found", route2 ~= nil, "no route")
-- Shortest: lander_pad → survey_point_1 → survey_point_2 (cost 1600)
check("plan2 cost = 1600", info2.cost == 1600,
    "expected 1600, got " .. tostring(info2.cost))
check("plan2 path = 3 nodes", #info2.path == 3,
    "expected 3, got " .. #info2.path)

print(string.format("  Path: %s (cost=%d)", table.concat(info2.path, " → "), info2.cost))

---------------------------------------------------------------------------
-- Test 3: Same start and goal
---------------------------------------------------------------------------
local route3, info3 = planner:plan("lander_pad", "lander_pad")
check("same node route exists", route3 ~= nil, "nil route")
check("same node cost = 0", info3.cost == 0,
    "expected 0, got " .. tostring(info3.cost))
check("same node empty route", #route3 == 0,
    "expected 0 actions, got " .. #route3)

print("  Same node: cost=0, 0 actions")

---------------------------------------------------------------------------
-- Test 4: Invalid node
---------------------------------------------------------------------------
local route4, info4 = planner:plan("nonexistent", "lander_pad")
check("invalid node returns nil", route4 == nil, "expected nil")
check("invalid node has error", info4.error ~= nil, "expected error")

print("  Invalid node: " .. (info4.error or ""))

---------------------------------------------------------------------------
-- Test 5: Route generation — verify format
---------------------------------------------------------------------------
print("\n--- Route Builder Tests ---")

local route5, info5 = planner:plan("lander_pad", "mining_zone_b",
    { bookend = true })

check("bookend route has init_check", route5[1].kb_name == "init_check",
    "expected init_check, got " .. route5[1].kb_name)
check("bookend route has idle", route5[#route5].kb_name == "idle",
    "expected idle, got " .. route5[#route5].kb_name)

-- Find first path action (skip init_check)
local first_path = nil
for _, action in ipairs(route5) do
    if action.kb_name == "path_spline" or action.kb_name == "path_line" then
        first_path = action
        break
    end
end
check("first path action exists", first_path ~= nil, "no path action")
if first_path then
    check("has from_x", first_path.params.from_x ~= nil, "missing from_x")
    check("has to_x", first_path.params.to_x ~= nil, "missing to_x")
    check("has speed", first_path.params.speed ~= nil, "missing speed")
    check("has distance", first_path.params.distance ~= nil, "missing distance")
    check("from = lander_pad",
        first_path.params.from_x == 0 and first_path.params.from_y == 0,
        "expected (0,0)")
    check("to = habitat_site",
        first_path.params.to_x == 800 and first_path.params.to_y == 0,
        "expected (800,0)")
end

-- Print route summary
print("  Route (" .. #route5 .. " actions):")
for i, a in ipairs(route5) do
    if a.params.to_x then
        print(string.format("    %2d. %-14s → (%d,%d)",
            i, a.kb_name, a.params.to_x, a.params.to_y))
    elseif a.params.to_heading then
        print(string.format("    %2d. %-14s → heading %.0f°",
            i, a.kb_name, a.params.to_heading))
    else
        print(string.format("    %2d. %s", i, a.kb_name))
    end
end

---------------------------------------------------------------------------
-- Test 6: Heading rotations
---------------------------------------------------------------------------
print("\n--- Heading Tests ---")

-- lander_pad(0,0) → habitat_site(800,0) heading=0 (east)
-- habitat_site(800,0) → charging_station(800,800) heading=90 (north)
-- Should insert a rotation between these two
local has_rotate = false
for _, a in ipairs(route5) do
    if a.kb_name == "path_rotate" then
        has_rotate = true
        check("rotation from_heading", a.params.from_heading ~= nil, "missing")
        check("rotation to_heading", a.params.to_heading ~= nil, "missing")
        print(string.format("  Rotation: %.0f° → %.0f°",
            a.params.from_heading, a.params.to_heading))
    end
end
check("route has rotations", has_rotate, "no path_rotate found")

---------------------------------------------------------------------------
-- Test 7: Blocked edge — replan
---------------------------------------------------------------------------
print("\n--- Replan Tests ---")

-- Block habitat_site ↔ charging_station
planner:mark_blocked("habitat_site", "charging_station")

local route6, info6 = planner:replan("lander_pad", "mining_zone_b")
check("replan found route", route6 ~= nil, "no route after blocking")
if info6.path then
    -- Path should NOT go through habitat_site → charging_station
    local uses_blocked = false
    for i = 1, #info6.path - 1 do
        if (info6.path[i] == "habitat_site" and info6.path[i+1] == "charging_station") or
           (info6.path[i] == "charging_station" and info6.path[i+1] == "habitat_site") then
            uses_blocked = true
        end
    end
    check("replan avoids blocked edge", not uses_blocked, "still uses blocked edge")
    print(string.format("  Replan path: %s (cost=%d)",
        table.concat(info6.path, " → "), info6.cost))
end

---------------------------------------------------------------------------
-- Test 8: Block all paths to a node
---------------------------------------------------------------------------
-- mining_zone_a is only reachable via habitat_site → mining_zone_a
planner:clear_blocked()
planner:mark_blocked("habitat_site", "mining_zone_a")

local route7, info7 = planner:replan("lander_pad", "mining_zone_a")
check("blocked node unreachable", route7 == nil,
    "expected nil (mining_zone_a only connected via habitat_site)")
if info7.error then
    print("  Fully blocked: " .. info7.error)
end

---------------------------------------------------------------------------
-- Test 9: Clear blocked — original path restored
---------------------------------------------------------------------------
planner:clear_blocked()

local route8, info8 = planner:plan("lander_pad", "mining_zone_b")
check("cleared blocked restores path", route8 ~= nil, "no route after clear")
check("restored cost = 2400", info8.cost == 2400,
    "expected 2400, got " .. tostring(info8.cost))

print("  Cleared: original path restored (cost=" .. info8.cost .. ")")

---------------------------------------------------------------------------
-- Test 10: find_nearest_node
---------------------------------------------------------------------------
print("\n--- Nearest Node Tests ---")

local nearest, dist = planner:find_nearest_node(400, 0)
check("nearest to (400,0) is lander_pad or habitat_site",
    nearest == "lander_pad" or nearest == "habitat_site",
    "got " .. tostring(nearest))
print(string.format("  Nearest to (400,0): %s (dist=%.0f)", nearest, dist))

local nearest2, dist2 = planner:find_nearest_node(800, 800)
check("nearest to (800,800) is charging_station", nearest2 == "charging_station",
    "got " .. tostring(nearest2))

---------------------------------------------------------------------------
-- Test 11: Heading computation
---------------------------------------------------------------------------
print("\n--- Heading Computation ---")

check("heading east",  math.abs(route_builder.compute_heading(0,0, 100,0) - 0) < 0.1,
    "expected 0")
check("heading north", math.abs(route_builder.compute_heading(0,0, 0,100) - 90) < 0.1,
    "expected 90")
check("heading west",  math.abs(route_builder.compute_heading(0,0, -100,0) - 180) < 0.1,
    "expected 180")
check("heading south", math.abs(route_builder.compute_heading(0,0, 0,-100) - 270) < 0.1,
    "expected 270")

planner:close()

---------------------------------------------------------------------------
-- Test 12: Virtual node definitions in KB
---------------------------------------------------------------------------
print("\n--- Virtual Node KB Tests ---")

local kb_query = require("kb_query")
local q = kb_query.new(db_file, "knowledge_base", "/usr/local/lib/ltree")

local vn_names = q:list_virtual_nodes()
check("KB has 12 virtual nodes", #vn_names == 12,
    "expected 11, got " .. #vn_names)
print("  VN types: " .. table.concat(vn_names, ", "))

local vn = q:get_virtual_node("path_spline")
check("path_spline exists", vn ~= nil, "not found")
if vn then
    check("path_spline packet_type_id = 2", vn.packet_type_id == 2,
        "got " .. tostring(vn.packet_type_id))
    check("path_spline has 8 schema fields", #vn.json_schema == 8,
        "got " .. #vn.json_schema)
    check("path_spline has 3 bitmask fields", #vn.bitmask == 3,
        "got " .. #vn.bitmask)
    check("path_spline has 3 pose fields", #vn.pose_fields == 3,
        "got " .. #vn.pose_fields)
end

local all_vn = q:get_all_virtual_nodes()
check("get_all has 11 entries",
    (function() local n=0; for _ in pairs(all_vn) do n=n+1 end; return n end)() == 12,
    "wrong count")

q:close()

---------------------------------------------------------------------------
-- Test 13: KB exporter to NATS KeyStore
---------------------------------------------------------------------------
print("\n--- KB Exporter Tests ---")

local kb_exporter = require("kb_exporter")
local stats = kb_exporter.export({
    db_file     = db_file,
    nats_server = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222",
    bucket      = "kb_export_test",
})

check("exporter wrote keys", stats.keys_written > 0,
    "expected > 0, got " .. stats.keys_written)
check("exporter found robots", stats.robots > 0,
    "expected > 0, got " .. stats.robots)
check("exporter found VN defs", stats.virtual_nodes == 12,
    "expected 11, got " .. stats.virtual_nodes)
check("exporter found boards", stats.boards > 0,
    "expected > 0, got " .. stats.boards)

print(string.format("  Exported: %d keys (%d robots, %d boards, %d VN defs)",
    stats.keys_written, stats.robots, stats.boards, stats.virtual_nodes))

-- Read back via reader
local reader = kb_exporter.reader({
    nats_server = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222",
    bucket      = "kb_export_test",
})

local conn = reader:get_robot_connection(stats.site, "rover_1")
check("reader: rover_1 connection", conn ~= nil, "not found")
if conn then
    check("reader: comm_type = nats", conn.comm_type == "nats",
        "got " .. tostring(conn.comm_type))
end

local caps = reader:get_robot_capabilities(stats.site, "rover_1")
check("reader: rover_1 capabilities", caps ~= nil and #caps == 12,
    "expected 11 caps, got " .. tostring(caps and #caps))

local vn_spline = reader:get_virtual_node(stats.site, "path_spline")
check("reader: path_spline VN", vn_spline ~= nil, "not found")
if vn_spline then
    check("reader: path_spline packet_type_id", vn_spline.packet_type_id == 2,
        "got " .. tostring(vn_spline.packet_type_id))
end

local board = reader:get_board(stats.site, "landing_zone")
check("reader: landing_zone board", board ~= nil, "not found")
if board then
    check("reader: board has nodes", board.nodes ~= nil and #board.nodes == 8,
        "expected 8 nodes")
end

reader:close()

---------------------------------------------------------------------------
-- Results
---------------------------------------------------------------------------
print(string.format("\n--- Results ---"))
print(string.format("Passed: %d", pass_count))
print(string.format("Failed: %d", fail_count))

if fail_count == 0 then
    print("\nPASSED")
else
    print("\nFAILED")
    os.exit(1)
end
