--[[
    test_hub_runtime.lua -- Validate hub_runtime state machine directly.

    Uses a planner-generated route but drives hub_runtime action-by-action
    (not through sequencer). Validates per-action pose accumulation and
    the state machine cycle: send_command → wait_ack → active → done.
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
    typedef void (*sighandler_t)(int);
    sighandler_t signal(int signum, sighandler_t handler);
]]
ffi.C.signal(13, ffi.cast("sighandler_t", 1))

local json_util       = require("json_util")
local hub_runtime     = require("hub_runtime")
local mission_builder = require("mission_builder")
local global_planner  = require("global_planner")
local mission_mod     = require("mission")
local mqtt_hub_tx     = require("mqtt_hub_transport")
local link_helper     = require("test_link_helper")

local robot_id  = os.getenv("ROBOT_ID") or "rover_1"
local server    = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222"
local mqtt_host = os.getenv("MQTT_HOST") or "localhost"
local mqtt_port = tonumber(os.getenv("MQTT_PORT") or "1883")

local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local root_dir   = script_dir .. "../"
local db_file    = root_dir .. "hub_dsl/kb_construct/surface_ops.db"
local site       = os.getenv("VMRT_KB_SITE") or "moonbase.alpha.surface_ops"

print("=== Hub Runtime + Mission Test (MQTT) ===\n")
print(string.format("Robot: %s, MQTT: %s:%d, NATS: %s\n",
    robot_id, mqtt_host, mqtt_port, server))

---------------------------------------------------------------------------
-- Create MQTT hub transport and wait for robot registration
---------------------------------------------------------------------------
local mqtt_hub = mqtt_hub_tx.new(mqtt_host, mqtt_port, site)
mqtt_hub:connect()
local lm = link_helper.setup(mqtt_hub, site, robot_id, 10)

---------------------------------------------------------------------------
-- Create hub_runtime
---------------------------------------------------------------------------
local hub_rt = hub_runtime.new({
    robot_id     = robot_id,
    db_file      = db_file,
    site         = site,
    transport    = mqtt_hub:robot_transport(robot_id),
    mqtt_hub     = mqtt_hub,
    initial_pose = { x = 0, y = 0, z = 0, heading = 0, arm_angle = 0 },
})

local bb = hub_rt:get_blackboard()
local kb_by_name = hub_rt:get_kb_by_name()

print("Hub runtime initialized (state machine).")

---------------------------------------------------------------------------
-- Plan route from moon map
---------------------------------------------------------------------------
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

---------------------------------------------------------------------------
-- Mission telemetry
---------------------------------------------------------------------------
local m = mission_mod.new({
    robot_id     = robot_id,
    db_file      = db_file,
    site         = site,
    nats_server  = server,
    route_length = #route,
    stream_size  = 200,
})

---------------------------------------------------------------------------
-- Test runner
---------------------------------------------------------------------------
local pass_count = 0
local fail_count = 0

local function check(name, condition, msg)
    if condition then
        print(string.format("  PASS: %-20s global: x=%.0f y=%.0f heading=%.0f arm=%.0f",
            name, hub_rt:get_global_pose().x, hub_rt:get_global_pose().y,
            hub_rt:get_global_pose().heading, hub_rt:get_global_pose().arm_angle))
        pass_count = pass_count + 1
    else
        print(string.format("  FAIL: %s — %s", name, msg or ""))
        fail_count = fail_count + 1
    end
end

---------------------------------------------------------------------------
-- Execute each action via hub_runtime directly
---------------------------------------------------------------------------
print(string.format("\n--- Executing %d actions ---", #route))

m:start()
local action_index = 0

for _, action in ipairs(route) do
    action_index = action_index + 1
    local kb_name = action.kb_name

    -- Build action JSON
    local action_json = {}
    if action.params then
        for k, v in pairs(action.params) do action_json[k] = v end
    end
    action_json.test_id   = action_index
    action_json.next_test = (action_index < #route) and (action_index + 1) or 0

    bb.current_test_json = json_util.encode(action_json)

    local activated = hub_rt:activate_kb(kb_name)
    if not activated then
        check(kb_name, false, "KB not found")
        break
    end

    m:action_start(action_index, action, hub_rt:get_global_pose())

    -- Tick until complete
    local completed = false
    for tick = 1, 500 do
        hub_rt:tick()

        if tick % 5 == 0 then
            m:heartbeat(action_index, kb_name, hub_rt:get_global_pose())
        end

        if hub_rt:kb_is_complete(kb_name) then
            completed = true
            hub_rt:deactivate_kb(kb_name)
            break
        end

        ffi.C.usleep(2000)
    end

    if completed and bb.kb_done_success then
        local pose = hub_rt:get_global_pose()
        m:action_complete(action_index, action, pose)
        check(kb_name, true)
    elseif completed then
        m:action_failed(action_index, action, bb.fault_reason or "kb_done_failed")
        check(kb_name, false, bb.fault_reason or "kb_done_failed")
        break
    else
        m:action_failed(action_index, action, "timeout")
        check(kb_name, false, "timeout after 500 ticks")
        break
    end
end

-- Mission complete
m:finish({
    success    = (action_index == #route and fail_count == 0),
    completed  = action_index,
    total      = #route,
    final_pose = hub_rt:get_global_pose(),
})

---------------------------------------------------------------------------
-- NATS telemetry checks
---------------------------------------------------------------------------
print("\n--- NATS Telemetry ---")

local ks_status = m:read_status()
if ks_status then
    check("NATS status", ks_status.type == "mission_complete",
        "got " .. tostring(ks_status.type))
    print(string.format("  NATS KeyStore status: type=%s, success=%s, completed=%s",
        tostring(ks_status.type), tostring(ks_status.success),
        tostring(ks_status.completed)))
end

local stream_entries = m:read_stream()
if stream_entries then
    local type_counts = {}
    for _, e in ipairs(stream_entries) do
        type_counts[e.type] = (type_counts[e.type] or 0) + 1
    end
    print(string.format("  NATS JetStream stream: %d entries", #stream_entries))
    local parts = {}
    for t, c in pairs(type_counts) do parts[#parts + 1] = t .. "=" .. c end
    print(string.format("  NATS stream types: %s", table.concat(parts, ", ")))

    check("stream action_start", (type_counts.action_start or 0) > 0)
    check("stream action_complete", (type_counts.action_complete or 0) > 0)
end

---------------------------------------------------------------------------
-- Cleanup
---------------------------------------------------------------------------
hub_rt:send_shutdown()
ffi.C.usleep(200000)
hub_rt:close()
m:close()
mqtt_hub:close()

print(string.format("\n--- Results ---\nPassed: %d\nFailed: %d\n", pass_count, fail_count))
if fail_count == 0 then print("PASSED") else print("FAILED"); os.exit(1) end
