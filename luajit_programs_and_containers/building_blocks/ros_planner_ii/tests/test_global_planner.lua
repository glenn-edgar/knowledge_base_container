--[[
    test_global_planner.lua -- Unit tests for global planner:
      dijkstra, route builder, blocked edges, replanning.

    No remote process needed — pure planning tests.
]]

local global_planner = require("global_planner")
local route_builder  = require("route_builder")
local json_util      = require("json_util")

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
check("graph has 20 nodes",
    (function()
        local n = 0; for _ in pairs(graph.nodes) do n = n + 1 end; return n
    end)() == 20,
    "expected 20 nodes")

check("lander_pad exists", graph.nodes["lander_pad"] ~= nil, "missing")
check("lander_pad coords",
    graph.nodes["lander_pad"].x == 0 and graph.nodes["lander_pad"].y == 0,
    "expected (0,0)")

-- Verify bidirectional edges (lander_pad should have 2 neighbors, but also
-- reverse edges from habitat_site and survey_point_1)
local lp_neighbors = #graph.adj["lander_pad"]
check("lander_pad has neighbors", lp_neighbors >= 2,
    "expected >= 2, got " .. lp_neighbors)

print("  Graph: 9 nodes, bidirectional edges loaded from KB\n")

---------------------------------------------------------------------------
-- Test 1: Dijkstra — lander_pad → mining_zone_b
---------------------------------------------------------------------------
print("--- Dijkstra Tests ---")

local route, info = planner:plan("lander_pad", "mining_zone_b")
check("plan lander→mining_b found", route ~= nil, "no route")
check("plan cost = 2094", info.cost == 2094,
    "expected 2094 (via junction_central), got " .. tostring(info.cost))
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
    check("has speed", first_path.params.speed ~= nil, "missing speed")
    check("has path", first_path.params.path ~= nil, "missing path")
    check("path is flat array", type(first_path.params.path) == "table",
        "expected table")
    check("path has 8 elements (4 points: endpoints + 2 interpolated)",
        #first_path.params.path == 8,
        "got " .. #first_path.params.path)
    -- First path: lander_pad(0,0) → habitat_site(800,0)
    local p = first_path.params.path
    check("path starts at lander_pad",
        p[1] == 0 and p[2] == 0,
        "expected (0,0), got (" .. p[1] .. "," .. p[2] .. ")")
    check("path ends at habitat_site",
        p[#p - 1] == 800 and p[#p] == 0,
        "expected (800,0), got (" .. p[#p-1] .. "," .. p[#p] .. ")")
    check("no from_x (removed)", first_path.params.from_x == nil, "should be nil")
    check("no distance (removed)", first_path.params.distance == nil, "should be nil")
    check("has energy", first_path.energy ~= nil and first_path.energy > 0,
        "expected energy > 0, got " .. tostring(first_path.energy))
end

-- Verify total energy in plan_info
check("plan_info has energy", info5.energy ~= nil and info5.energy > 0,
    "expected total energy > 0, got " .. tostring(info5.energy))

-- Print route summary
local route_energy = 0
print("  Route (" .. #route5 .. " actions, energy=" .. tostring(info5.energy) .. "):")
for i, a in ipairs(route5) do
    route_energy = route_energy + (a.energy or 0)
    if a.params.path then
        local p = a.params.path
        print(string.format("    %2d. %-14s path[%d] (%d,%d)→(%d,%d) speed=%s energy=%d",
            i, a.kb_name, #p/2, p[1], p[2], p[#p-1], p[#p],
            tostring(a.params.speed), a.energy or 0))
    else
        print(string.format("    %2d. %-14s energy=%d", i, a.kb_name, a.energy or 0))
    end
end

---------------------------------------------------------------------------
-- Test 6: No rotations (path_rotate removed)
---------------------------------------------------------------------------
print("\n--- No Rotation Tests ---")

local has_rotate = false
for _, a in ipairs(route5) do
    if a.kb_name == "path_rotate" then has_rotate = true end
end
check("route has no rotations", not has_rotate, "unexpected path_rotate found")

-- All nav actions should be path_spline or path_line (direct kb_name)
local all_nav_direct = true
for _, a in ipairs(route5) do
    if a.kb_name ~= "init_check" and a.kb_name ~= "idle" then
        if a.kb_name ~= "path_spline" and a.kb_name ~= "path_line" then
            all_nav_direct = false
        end
    end
end
check("all nav actions are path_spline or path_line", all_nav_direct,
    "found unexpected nav kb_name")

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
-- mining_zone_a reachable only via transit_mine_w (single spur) — block it
planner:clear_blocked()
planner:mark_blocked("transit_mine_w", "mining_zone_a")

local route7, info7 = planner:replan("lander_pad", "mining_zone_a")
check("blocked node unreachable", route7 == nil,
    "expected nil (all edges to mining_zone_a blocked)")
if info7.error then
    print("  Fully blocked: " .. info7.error)
end

---------------------------------------------------------------------------
-- Test 9: Clear blocked — original path restored
---------------------------------------------------------------------------
planner:clear_blocked()

local route8, info8 = planner:plan("lander_pad", "mining_zone_b")
check("cleared blocked restores path", route8 ~= nil, "no route after clear")
check("restored cost = 2094", info8.cost == 2094,
    "expected 2094, got " .. tostring(info8.cost))

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
-- Test 11: Transit node type (renumbered from 12)
---------------------------------------------------------------------------
print("\n--- Transit Node Tests ---")

-- Verify transit nodes stored in graph
check("junction_north is transit", planner:is_transit("junction_north"),
    "expected transit")
check("junction_central is transit", planner:is_transit("junction_central"),
    "expected transit")
check("lander_pad is NOT transit (type=base)", not planner:is_transit("lander_pad"),
    "should not be transit")
check("mining_zone_a is NOT transit (type=deliver_part)", not planner:is_transit("mining_zone_a"),
    "should not be transit")
check("nonexistent is NOT transit", not planner:is_transit("nonexistent"),
    "should not be transit")

-- Dijkstra routes through transit nodes silently
local route_via_transit, info_via = planner:plan("habitat_site", "mining_zone_a")
check("route through transit found", route_via_transit ~= nil, "no route")
if info_via.path then
    local found_transit = false
    for _, name in ipairs(info_via.path) do
        if name == "transit_mine_w" then found_transit = true; break end
    end
    check("route passes through transit_mine_w", found_transit,
        "path: " .. table.concat(info_via.path, " → "))
    print("  Transit route: " .. table.concat(info_via.path, " → ") ..
        " (cost=" .. info_via.cost .. ")")
end

-- Mission builder rejects transit stops
local mb = require("mission_builder")
local transit_route, transit_info = mb.build({
    start = "lander_pad",
    stops = { { node = "junction_north" } },
}, planner)
check("mission builder rejects transit stop", transit_route == nil,
    "expected nil (transit node)")
check("rejection error is transit_node_stops",
    transit_info and transit_info.error == "transit_node_stops",
    "got: " .. tostring(transit_info and transit_info.error))
if transit_info and transit_info.transit_stops then
    print("  Rejected: " .. transit_info.transit_stops[1])
end

-- Mission builder accepts operation stops
local ok_route, ok_info = mb.build({
    start = "lander_pad",
    stops = { { node = "mining_zone_a" } },
}, planner)
check("mission builder accepts operation stop", ok_route ~= nil,
    "expected route for operation node")

-- Mission builder emits operation VN for stop actions
local op_route, op_info = mb.build({
    start = "lander_pad",
    stops = { { node = "mining_zone_a", action = "deliver_part",
                params = { arm_target = -45 } } },
}, planner)
check("operation route exists", op_route ~= nil, "expected route")
if op_route then
    local found_op = false
    for _, a in ipairs(op_route) do
        if a.kb_name == "operation" then
            found_op = true
            check("operation_type is deliver_part",
                a.params.operation_type == "deliver_part",
                "got: " .. tostring(a.params.operation_type))
            check("operation data has arm_target",
                a.params.data and a.params.data.arm_target == -45,
                "missing arm_target")
            print("  Operation VN: " .. a.params.operation_type ..
                " data=" .. json_util.encode(a.params.data))
        end
    end
    check("route contains operation VN", found_op, "no operation VN in route")
end

-- Mission builder validates operation_types
local bad_route, bad_info = mb.build({
    start = "lander_pad",
    stops = { { node = "mining_zone_a", action = "fly_to_mars" } },
}, planner, { "deliver_part", "inspection_scan" })
check("unsupported operation rejected", bad_route == nil,
    "expected nil for unsupported operation")
check("rejection error is unsupported_operation",
    bad_info and bad_info.error == "unsupported_operation",
    "got: " .. tostring(bad_info and bad_info.error))
if bad_info and bad_info.unsupported then
    print("  Unsupported: " .. bad_info.unsupported[1])
end

planner:close()

---------------------------------------------------------------------------
-- Test 13: Virtual node definitions in KB (renumbered from 12)
---------------------------------------------------------------------------
print("\n--- Virtual Node KB Tests ---")

local kb_query = require("kb_query")
local q = kb_query.new(db_file, "knowledge_base", "/usr/local/lib/ltree")

local vn_names = q:list_virtual_nodes()
check("KB has 5 virtual nodes", #vn_names == 5,
    "expected 5 (init_check, path_spline, path_line, operation, idle), got " .. #vn_names)
print("  VN types: " .. table.concat(vn_names, ", "))

local vn = q:get_virtual_node("path_spline")
check("path_spline exists", vn ~= nil, "not found")
if vn then
    check("path_spline packet_type_id = 2", vn.packet_type_id == 2,
        "got " .. tostring(vn.packet_type_id))
end

local vn_op = q:get_virtual_node("operation")
check("operation VN exists", vn_op ~= nil, "not found")
if vn_op then
    check("operation packet_type_id = 20", vn_op.packet_type_id == 20,
        "got " .. tostring(vn_op.packet_type_id))
    check("operation has 2 schema fields", #vn_op.json_schema == 2,
        "got " .. #vn_op.json_schema)
end

local all_vn = q:get_all_virtual_nodes()
check("get_all has 5 entries",
    (function() local n=0; for _ in pairs(all_vn) do n=n+1 end; return n end)() == 5,
    "wrong count")

q:close()

---------------------------------------------------------------------------
-- Test 14: KB exporter to NATS KeyStore
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
check("exporter found robot classes", stats.robot_classes > 0,
    "expected > 0, got " .. stats.robot_classes)
check("exporter found VN defs", stats.virtual_nodes == 5,
    "expected 5, got " .. stats.virtual_nodes)
check("exporter found boards", stats.boards > 0,
    "expected > 0, got " .. stats.boards)

print(string.format("  Exported: %d keys (%d classes, %d boards, %d VN defs)",
    stats.keys_written, stats.robot_classes, stats.boards, stats.virtual_nodes))

-- Read back via reader
local reader = kb_exporter.reader({
    nats_server = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222",
    bucket      = "kb_export_test",
})

local infra = reader:get_class_infra(stats.site, "lunar_rover")
check("reader: lunar_rover class infra", infra ~= nil, "not found")
if infra then
    check("reader: lunar_rover has 5 VNs", #infra.virtual_nodes == 5,
        "expected 5, got " .. tostring(infra.virtual_nodes and #infra.virtual_nodes))
end

local vn_spline = reader:get_virtual_node(stats.site, "path_spline")
check("reader: path_spline VN", vn_spline ~= nil, "not found")
if vn_spline then
    check("reader: path_spline packet_type_id", vn_spline.packet_type_id == 2,
        "got " .. tostring(vn_spline.packet_type_id))
end

local board = reader:get_board(stats.site, "landing_zone")
check("reader: landing_zone board", board ~= nil, "not found")
if board then
    check("reader: board has nodes", board.nodes ~= nil and #board.nodes == 20,
        "expected 20 nodes")
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
