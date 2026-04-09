--[[
    test_mqtt_direct.lua -- End-to-end: mission → planner → MQTT (JSON) → robot.

    Submits a real mission against the landing_zone board.
    Global planner builds the route (Dijkstra + route_builder).
    Sequencer executes via MQTT transport. Validates:
      - Route generation from moon map
      - Per-action pose accumulation
      - Mission telemetry (NATS KV, JetStream, SQLite bookends)
      - JSON wire format

    Two processes (started by run_tests.sh):
      1. This test (planner side)
      2. remote_mqtt_ct.lua (MQTT robot, JSON wire)
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
    typedef void (*sighandler_t)(int);
    sighandler_t signal(int signum, sighandler_t handler);
]]
ffi.C.signal(13, ffi.cast("sighandler_t", 1))  -- ignore SIGPIPE

local json_util        = require("json_util")
local sequencer_mod    = require("sequencer")
local mission_builder  = require("mission_builder")
local global_planner   = require("global_planner")
local mission_mod      = require("mission")
local mqtt_hub_tx      = require("mqtt_hub_transport")
local link_helper      = require("test_link_helper")

local robot_id  = os.getenv("ROBOT_ID") or "rover_1"
local server    = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222"
local mqtt_host = os.getenv("MQTT_HOST") or "localhost"
local mqtt_port = tonumber(os.getenv("MQTT_PORT") or "1883")
local site      = os.getenv("VMRT_KB_SITE") or "moonbase.alpha.surface_ops"

local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local root_dir   = script_dir .. "../"
local db_file    = root_dir .. "hub_dsl/kb_construct/surface_ops.db"

print("=== MQTT Direct Integration Test ===\n")
print(string.format("Robot: %s, MQTT: %s:%d, NATS: %s\n",
    robot_id, mqtt_host, mqtt_port, server))

---------------------------------------------------------------------------
-- Create MQTT hub transport and wait for robot registration
---------------------------------------------------------------------------
local mqtt_hub = mqtt_hub_tx.new(mqtt_host, mqtt_port, site)
mqtt_hub:connect()
local lm = link_helper.setup(mqtt_hub, site, robot_id, 10)

print("Hub connected, robot registered.")

---------------------------------------------------------------------------
-- Test runner
---------------------------------------------------------------------------
local pass_count = 0
local fail_count = 0

local function check(name, condition, msg)
    if condition then
        print("  PASS: " .. name)
        pass_count = pass_count + 1
    else
        print("  FAIL: " .. name .. (msg and (" — " .. msg) or ""))
        fail_count = fail_count + 1
    end
end

---------------------------------------------------------------------------
-- Test 1: Plan a multi-stop mission against the moon map
---------------------------------------------------------------------------
print("\n--- Route Planning ---")

local planner = global_planner.new({
    db_file    = db_file,
    board_name = "landing_zone",
})

local mission_cmd = {
    start   = "lander_pad",
    board   = "landing_zone",
    stops = {
        { node = "mining_zone_a", action = "deliver_part",
          params = { arm_target = -45, arm_speed = 80, arm_return = 0, payload_type = 1 } },
        { node = "charging_station", action = "paint_sample",
          params = { arm_target = -60, arm_speed = 60, arm_return = 0, hold_time = 500 } },
        { node = "construction_bay", action = "load_shipping",
          params = { arm_target = -30, arm_speed = 80, arm_return = 0, payload_type = 2 } },
        { node = "survey_point_1", action = "inspection_scan",
          params = { sensor_port = 0, sensor_type = 0 } },
        { node = "lander_pad" },
    },
    bookend = true,
}

local route, plan_info = mission_builder.build(mission_cmd, planner)
planner:close()

check("route built", route ~= nil, "nil route")
check("has 5 legs", plan_info and #plan_info.legs == 5,
    "got " .. tostring(plan_info and #plan_info.legs))

if route then
    check("starts with init_check", route[1].kb_name == "init_check",
        "got " .. route[1].kb_name)
    check("ends with idle", route[#route].kb_name == "idle",
        "got " .. route[#route].kb_name)

    -- Count VN types
    local counts = {}
    for _, a in ipairs(route) do
        counts[a.kb_name] = (counts[a.kb_name] or 0) + 1
    end

    check("has deliver_part", (counts.deliver_part or 0) == 1)
    check("has paint_sample", (counts.paint_sample or 0) == 1)
    check("has load_shipping", (counts.load_shipping or 0) == 1)
    check("has inspection_scan", (counts.inspection_scan or 0) == 1)
    check("has path_spline", (counts.path_spline or 0) > 0,
        "no spline navigation")
    check("has path_rotate", (counts.path_rotate or 0) > 0,
        "no rotations (heading changes expected)")

    print(string.format("  Route: %d actions, %d legs, cost=%d",
        #route, #plan_info.legs, plan_info.total_cost))
    print("  Actions:")
    for i, a in ipairs(route) do
        print(string.format("    %2d. %s", i, a.kb_name))
    end
end

---------------------------------------------------------------------------
-- Test 2: Execute mission via sequencer
---------------------------------------------------------------------------
print("\n--- Mission Execution ---")

local seq = sequencer_mod.new({
    robot_id     = robot_id,
    db_file      = db_file,
    site         = site,
    nats_server  = server,
    mqtt_hub     = mqtt_hub,
    capabilities = {
        "init_check", "path_spline", "path_line", "path_wall",
        "path_rotate", "deliver_part", "paint_sample", "load_shipping",
        "pass_gate", "inspection_scan", "recharge", "idle",
    },
})

seq:load_route(route)

local valid, errors = seq:validate_route()
check("route valid for robot", valid,
    errors and #errors > 0 and errors[1] or "")

local result = seq:run()

check("mission success", result.success == true,
    "got " .. tostring(result.success) ..
    (result.fault and (": " .. (result.fault.reason or "")) or ""))
check("all actions completed", result.completed == #route,
    string.format("completed %d/%d", result.completed or 0, #route))
check("no fault", result.fault == nil,
    result.fault and result.fault.reason or "")

-- Validate final pose moved from origin
if result.final_pose then
    local p = result.final_pose
    print(string.format("  Final pose: x=%.0f y=%.0f heading=%.0f arm=%.0f",
        p.x, p.y, p.heading, p.arm_angle))
    -- Mission ends at lander_pad (0,0) but heading/arm may have changed
    check("returned to lander_pad x", math.abs(p.x) < 1,
        "expected ~0, got " .. tostring(p.x))
    check("returned to lander_pad y", math.abs(p.y) < 1,
        "expected ~0, got " .. tostring(p.y))
end

print(string.format("  Completed: %d/%d, elapsed: %dms",
    result.completed or 0, result.total or 0, result.elapsed_ms or 0))

---------------------------------------------------------------------------
-- Test 3: Verify NATS telemetry
---------------------------------------------------------------------------
print("\n--- NATS Telemetry ---")

local m = seq:get_mission()

-- KeyStore status
local ks_status = m:read_status()
if ks_status then
    check("NATS status type", ks_status.type == "mission_complete",
        "got " .. tostring(ks_status.type))
    check("NATS status success", ks_status.success == true,
        "got " .. tostring(ks_status.success))
    print(string.format("  NATS KeyStore status: type=%s, success=%s, completed=%s",
        tostring(ks_status.type), tostring(ks_status.success),
        tostring(ks_status.completed)))
else
    check("NATS status exists", false, "no status in KeyStore")
end

-- JetStream entries
local stream_entries = m:read_stream()
if stream_entries then
    check("stream has entries", #stream_entries > 0,
        "empty stream")
    local type_counts = {}
    for _, e in ipairs(stream_entries) do
        type_counts[e.type] = (type_counts[e.type] or 0) + 1
    end
    check("stream has action_start", (type_counts.action_start or 0) > 0)
    check("stream has action_complete", (type_counts.action_complete or 0) > 0)
    check("stream has heartbeat", (type_counts.heartbeat or 0) > 0)
    check("stream has mission_complete", (type_counts.mission_complete or 0) == 1)

    local parts = {}
    for t, c in pairs(type_counts) do parts[#parts + 1] = t .. "=" .. c end
    print(string.format("  NATS JetStream: %d entries", #stream_entries))
    print(string.format("  Stream types: %s", table.concat(parts, ", ")))
else
    check("stream exists", false, "no JetStream entries")
end

---------------------------------------------------------------------------
-- Cleanup
---------------------------------------------------------------------------
seq:shutdown()
mqtt_hub:close()

---------------------------------------------------------------------------
-- Results
---------------------------------------------------------------------------
print(string.format("\n--- Results ---"))
print(string.format("Passed: %d", pass_count))
print(string.format("Failed: %d", fail_count))
if result and result.final_pose then
    print(string.format("Final global pose: x=%.0f y=%.0f heading=%.0f arm=%.0f",
        result.final_pose.x, result.final_pose.y,
        result.final_pose.heading, result.final_pose.arm_angle))
end

if fail_count == 0 then
    print("\nPASSED")
else
    print("\nFAILED")
    os.exit(1)
end
