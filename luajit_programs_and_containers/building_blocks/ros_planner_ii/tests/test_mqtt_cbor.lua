--[[
    test_mqtt_cbor.lua -- End-to-end: mission → planner → MQTT (CBOR) → robot.

    Same mission as test_mqtt_direct.lua but with CBOR wire format.
    Validates CBOR encoding/decoding works transparently.
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
    typedef void (*sighandler_t)(int);
    sighandler_t signal(int signum, sighandler_t handler);
]]
ffi.C.signal(13, ffi.cast("sighandler_t", 1))

local json_util        = require("json_util")
local sequencer_mod    = require("sequencer")
local mission_builder  = require("mission_builder")
local global_planner   = require("global_planner")
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

print("=== MQTT CBOR Integration Test ===\n")
print(string.format("Robot: %s, MQTT: %s:%d, wire: cbor\n",
    robot_id, mqtt_host, mqtt_port))

---------------------------------------------------------------------------
local mqtt_hub = mqtt_hub_tx.new(mqtt_host, mqtt_port, site)
mqtt_hub:connect()
mqtt_hub:set_wire_format(robot_id, "cbor")

local lm = link_helper.setup(mqtt_hub, site, robot_id, 10)

print("Hub connected (cbor), robot registered.")

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
-- Plan and execute the same multi-stop mission
---------------------------------------------------------------------------
print("\n--- Route Planning (CBOR) ---")

local planner = global_planner.new({
    db_file    = db_file,
    board_name = "landing_zone",
})

local route, plan_info = mission_builder.build({
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
}, planner)
planner:close()

check("route built", route ~= nil)

if route then
    print(string.format("  Route: %d actions, cost=%d", #route, plan_info.total_cost))
end

---------------------------------------------------------------------------
print("\n--- Mission Execution (CBOR) ---")

local seq = sequencer_mod.new({
    robot_id    = robot_id,
    db_file     = db_file,
    site        = site,
    nats_server = server,
    mqtt_hub    = mqtt_hub,
})

seq:load_route(route)
local result = seq:run()

check("mission success", result.success == true,
    "got " .. tostring(result.success) ..
    (result.fault and (": " .. (result.fault.reason or "")) or ""))
check("all actions completed", result.completed == #route,
    string.format("completed %d/%d", result.completed or 0, #route))
check("no fault", result.fault == nil)

if result.final_pose then
    local p = result.final_pose
    print(string.format("  Final pose: x=%.0f y=%.0f heading=%.0f arm=%.0f",
        p.x, p.y, p.heading, p.arm_angle))
    check("returned to lander_pad x", math.abs(p.x) < 1)
    check("returned to lander_pad y", math.abs(p.y) < 1)
end

print(string.format("  Completed: %d/%d, elapsed: %dms",
    result.completed or 0, result.total or 0, result.elapsed_ms or 0))

---------------------------------------------------------------------------
-- NATS telemetry
---------------------------------------------------------------------------
print("\n--- NATS Telemetry (CBOR) ---")

local m = seq:get_mission()
local ks_status = m:read_status()
if ks_status then
    check("NATS status success", ks_status.success == true)
end

local stream_entries = m:read_stream()
if stream_entries then
    check("stream has entries", #stream_entries > 0)
    local type_counts = {}
    for _, e in ipairs(stream_entries) do
        type_counts[e.type] = (type_counts[e.type] or 0) + 1
    end
    check("stream has action_start", (type_counts.action_start or 0) > 0)
    check("stream has action_complete", (type_counts.action_complete or 0) > 0)
    check("stream has mission_complete", (type_counts.mission_complete or 0) == 1)
end

---------------------------------------------------------------------------
seq:shutdown()
mqtt_hub:close()

print(string.format("\n--- Results ---\nPassed: %d\nFailed: %d\n", pass_count, fail_count))
if fail_count == 0 then print("PASSED") else print("FAILED"); os.exit(1) end
