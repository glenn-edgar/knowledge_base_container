--[[
    test_action_server.lua -- Validate action server:
      mission builder (route generation) + full execution with remote.
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
]]

local json_util      = require("json_util")
local action_server  = require("action_server")
local global_planner = require("global_planner")
local mission_builder = require("mission_builder")

local robot_id = os.getenv("ROBOT_ID") or "rover_1"
local server   = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222"
local site     = os.getenv("VMRT_KB_SITE") or "moonbase.alpha.surface_ops"

local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local root_dir   = script_dir .. "../"
local hub_json   = root_dir .. "hub_dsl/hub.json"
local db_file    = root_dir .. "hub_dsl/kb_construct/surface_ops.db"

print("=== Action Server Test ===\n")
print(string.format("Robot: %s, Server: %s\n", robot_id, server))

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
-- Test 1: Mission builder only (no execution)
---------------------------------------------------------------------------
print("--- Mission Builder Tests ---")

local planner = global_planner.new({
    db_file    = db_file,
    board_name = "landing_zone",
})

-- Simple mission: lander_pad → mining_zone_b (deliver_part) → lander_pad
local mission_cmd = {
    start   = "lander_pad",
    board   = "landing_zone",
    stops = {
        { node = "mining_zone_b", action = "deliver_part",
          params = { arm_target = -45, arm_speed = 80, payload_type = 1 } },
        { node = "lander_pad" },
    },
    bookend = true,
}

local route, plan_info = mission_builder.build(mission_cmd, planner)
check("build route exists", route ~= nil, "nil route")
check("build has legs", #plan_info.legs == 2,
    "expected 2 legs, got " .. #plan_info.legs)

if route then
    -- First action should be init_check (bookend)
    check("first action = init_check", route[1].kb_name == "init_check",
        "got " .. route[1].kb_name)
    -- Last action should be idle (bookend)
    check("last action = idle", route[#route].kb_name == "idle",
        "got " .. route[#route].kb_name)

    -- Should contain deliver_part somewhere in the middle
    local has_deliver = false
    for _, a in ipairs(route) do
        if a.kb_name == "deliver_part" then
            has_deliver = true
            check("deliver_part params", a.params.arm_target == -45,
                "expected -45")
        end
    end
    check("route has deliver_part", has_deliver, "missing deliver_part action")

    -- Print route summary
    print(string.format("  Route: %d actions, cost=%d", #route, plan_info.total_cost))
    for i, a in ipairs(route) do
        if a.params.to_x then
            print(string.format("    %2d. %-14s → (%d,%d)", i, a.kb_name,
                a.params.to_x, a.params.to_y))
        elseif a.params.to_heading then
            print(string.format("    %2d. %-14s → heading %.0f°", i, a.kb_name,
                a.params.to_heading))
        elseif a.params.arm_target then
            print(string.format("    %2d. %-14s arm=%.0f", i, a.kb_name,
                a.params.arm_target))
        else
            print(string.format("    %2d. %s", i, a.kb_name))
        end
    end
end

---------------------------------------------------------------------------
-- Test 2: Multi-stop mission builder
---------------------------------------------------------------------------
print("\n--- Multi-Stop Mission Builder ---")

local multi_cmd = {
    start = "lander_pad",
    stops = {
        { node = "mining_zone_b", action = "deliver_part",
          params = { arm_target = -45, arm_speed = 80, payload_type = 1 } },
        { node = "charging_station", action = "paint_sample",
          params = { arm_target = -60, arm_speed = 60, hold_time = 500 } },
        { node = "construction_bay", action = "load_shipping",
          params = { arm_target = -30, arm_speed = 80, payload_type = 2 } },
        { node = "lander_pad" },
    },
    bookend = true,
}

local route2, info2 = mission_builder.build(multi_cmd, planner)
check("multi route exists", route2 ~= nil, "nil route")
check("multi has 4 legs", #info2.legs == 4,
    "expected 4, got " .. #info2.legs)

if route2 then
    -- Count action types
    local counts = {}
    for _, a in ipairs(route2) do
        counts[a.kb_name] = (counts[a.kb_name] or 0) + 1
    end
    check("has 3 mission actions",
        (counts["deliver_part"] or 0) + (counts["paint_sample"] or 0) +
        (counts["load_shipping"] or 0) == 3,
        "expected 3 mission actions")
    check("has path actions", (counts["path_spline"] or 0) > 0, "no nav actions")

    print(string.format("  Multi-stop: %d actions, %d legs, cost=%d",
        #route2, #info2.legs, info2.total_cost))
end

planner:close()

---------------------------------------------------------------------------
-- Test 3: Full execution with remote
---------------------------------------------------------------------------
print("\n--- Full Execution Test ---")

-- Give remote time to connect
ffi.C.usleep(1000000)

local srv = action_server.new({
    db_file     = db_file,
    hub_json    = hub_json,
    nats_server = server,
})

local result = srv:execute_mission({
    robot_id = robot_id,
    board    = "landing_zone",
    start    = "lander_pad",
    site     = site,
    stops = {
        { node = "mining_zone_b", action = "deliver_part",
          params = { arm_target = -45, arm_speed = 80, arm_return = 0, payload_type = 1 } },
        { node = "lander_pad" },
    },
    bookend = true,
})

check("execution success", result.success == true,
    "expected true, got " .. tostring(result.success))
check("no fault", result.fault == nil,
    "expected nil, got " .. tostring(result.fault and result.fault.reason))
check("no replans", result.replans == 0,
    "expected 0, got " .. tostring(result.replans))
check("has final_pose", result.final_pose ~= nil, "missing")

if result.final_pose then
    print(string.format("  Final pose: x=%.0f y=%.0f heading=%.0f arm=%.0f",
        result.final_pose.x, result.final_pose.y,
        result.final_pose.heading, result.final_pose.arm_angle))
end

print(string.format("  Completed: %d/%d, elapsed: %dms, replans: %d",
    result.completed or 0, result.total or 0,
    result.elapsed_ms or 0, result.replans or 0))

srv:close()

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
