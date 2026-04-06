--[[
    test_sequencer.lua -- Validate sequencer with planner-generated route.

    Uses sequencer.new() / load_route() / run() / shutdown() API.
    Verifies: result table, NATS telemetry, route validation.
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
]]

local json_util       = require("json_util")
local sequencer       = require("sequencer")
local mission_builder = require("mission_builder")
local global_planner  = require("global_planner")
local mqtt_hub_tx     = require("mqtt_hub_transport")
local link_helper     = require("test_link_helper")

local robot_id  = os.getenv("ROBOT_ID") or "rover_1"
local server    = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222"
local mqtt_host = os.getenv("MQTT_HOST") or "localhost"
local mqtt_port = tonumber(os.getenv("MQTT_PORT") or "1883")
local site      = os.getenv("VMRT_KB_SITE") or "moonbase.alpha.surface_ops"

local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local root_dir   = script_dir .. "../"
local db_file    = root_dir .. "hub_dsl/kb_construct/surface_ops.db"

print("=== Sequencer Test (MQTT) ===\n")
print(string.format("Robot: %s, MQTT: %s:%d, NATS: %s\n",
    robot_id, mqtt_host, mqtt_port, server))

---------------------------------------------------------------------------
-- Create MQTT hub transport and wait for robot registration
---------------------------------------------------------------------------
local mqtt_hub = mqtt_hub_tx.new(mqtt_host, mqtt_port, site)
mqtt_hub:connect()
local lm = link_helper.setup(mqtt_hub, site, robot_id, 10)

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
-- Plan route
---------------------------------------------------------------------------
print("--- Route Planning ---")

local planner = global_planner.new({
    db_file    = db_file,
    board_name = "landing_zone",
})

local route, plan_info = mission_builder.build({
    start   = "lander_pad",
    board   = "landing_zone",
    stops = {
        { node = "habitat_site" },
        { node = "charging_station", action = "recharge",
          params = { target_energy = 0 } },
        { node = "lander_pad" },
    },
    bookend = true,
}, planner)
planner:close()

check("route built", route ~= nil)
if route then
    print(string.format("  Route: %d actions, %d legs, cost=%d",
        #route, #plan_info.legs, plan_info.total_cost))
end

---------------------------------------------------------------------------
-- Execute via sequencer
---------------------------------------------------------------------------
print("\n--- Sequencer Execution ---")

local seq = sequencer.new({
    robot_id    = robot_id,
    db_file     = db_file,
    site        = site,
    nats_server = server,
    mqtt_hub    = mqtt_hub,
})

seq:load_route(route)

local valid, errors = seq:validate_route()
check("route valid", valid, errors and errors[1])

local result = seq:run()

check("mission success", result.success == true,
    result.fault and result.fault.reason or "")
check("all completed", result.completed == #route,
    string.format("%d/%d", result.completed or 0, #route))
check("no fault", result.fault == nil)
check("no replan needed", not result.needs_replan)
check("elapsed > 0", (result.elapsed_ms or 0) > 0)

if result.final_pose then
    local p = result.final_pose
    print(string.format("  Final pose: x=%.0f y=%.0f heading=%.0f arm=%.0f",
        p.x, p.y, p.heading, p.arm_angle))
    check("returned to origin x", math.abs(p.x) < 1)
    check("returned to origin y", math.abs(p.y) < 1)
end

---------------------------------------------------------------------------
-- NATS telemetry
---------------------------------------------------------------------------
print("\n--- NATS Telemetry ---")

local m = seq:get_mission()
local ks_status = m:read_status()
if ks_status then
    check("status type", ks_status.type == "mission_complete")
    check("status success", ks_status.success == true)
end

local stream_entries = m:read_stream()
if stream_entries then
    check("stream has entries", #stream_entries > 0)
    local type_counts = {}
    for _, e in ipairs(stream_entries) do
        type_counts[e.type] = (type_counts[e.type] or 0) + 1
    end
    check("stream action_start", (type_counts.action_start or 0) > 0)
    check("stream action_complete", (type_counts.action_complete or 0) > 0)
    check("stream mission_complete", (type_counts.mission_complete or 0) == 1)
end

---------------------------------------------------------------------------
seq:shutdown()
mqtt_hub:close()

print(string.format("\n--- Results ---\nPassed: %d\nFailed: %d\n", pass_count, fail_count))
if fail_count == 0 then print("PASSED") else print("FAILED"); os.exit(1) end
