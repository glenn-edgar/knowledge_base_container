--[[
    test_mqtt_direct.lua -- End-to-end: planner → MQTT → robot (no bridge).

    Demonstrates the MQTT-first architecture:
      - mqtt_hub_transport: single shared MQTT client for the planner
      - hub_runtime: transport-injectable, MQTT auto-poll, no NATS on tick path
      - Status published via kv-bridge container (MQTT → NATS KV async)

    Two processes (started by run_tests.sh):
      1. This test (planner side, MQTT direct)
      2. remote_mqtt_ct.lua (MQTT robot)

    Same 18-action route as test_hub_runtime.lua.
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
    typedef void (*sighandler_t)(int);
    sighandler_t signal(int signum, sighandler_t handler);
]]
ffi.C.signal(13, ffi.cast("sighandler_t", 1))  -- ignore SIGPIPE

local json_util        = require("json_util")
local hub_runtime      = require("hub_runtime")
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
local hub_json   = root_dir .. "hub_dsl/hub.json"
local db_file    = root_dir .. "hub_dsl/kb_construct/surface_ops.db"

print("=== MQTT Direct Integration Test ===\n")
print(string.format("Robot: %s, MQTT: %s:%d, NATS: %s\n",
    robot_id, mqtt_host, mqtt_port, server))

---------------------------------------------------------------------------
-- Create MQTT hub transport (single shared client)
---------------------------------------------------------------------------
local mqtt_hub = mqtt_hub_tx.new(mqtt_host, mqtt_port, site)
mqtt_hub:connect()
print("MQTT hub connected.")

---------------------------------------------------------------------------
-- Wait for robot to register via link protocol
---------------------------------------------------------------------------
local lm = link_helper.setup(mqtt_hub, site, robot_id, 10)

---------------------------------------------------------------------------
-- Create hub_runtime with injected MQTT transport
---------------------------------------------------------------------------
local hub_rt = hub_runtime.new({
    robot_id     = robot_id,
    nats_server  = server,
    hub_json     = hub_json,
    site         = site,
    transport    = mqtt_hub:robot_transport(robot_id),
    mqtt_hub     = mqtt_hub,
    initial_pose = { x = 0, y = 0, z = 0, heading = 0, arm_angle = 0 },
})

local bb = hub_rt:get_blackboard()
local kb_by_name = hub_rt:get_kb_by_name()

print("Hub runtime initialized (MQTT transport).")

---------------------------------------------------------------------------
-- Initialize mission telemetry
---------------------------------------------------------------------------
local m = mission_mod.new({
    robot_id     = robot_id,
    db_file      = db_file,
    site         = site,
    nats_server  = server,
    route_length = 18,
})
m:start()
print("Mission started.\n")

local action_index = 0

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

local function test_virtual_node(plugin, action_json)
    local kb_name = plugin.name
    action_index = action_index + 1
    local action = { kb_name = kb_name }

    bb.current_test_json = json_util.encode(action_json)

    local activated = hub_rt:activate_kb(kb_name)
    check(kb_name .. " activate", activated, "KB not found")
    if not activated then return end

    check(kb_name .. " active_kb", bb.active_kb == kb_name,
        "expected " .. kb_name .. ", got " .. tostring(bb.active_kb))

    m:action_start(action_index, action, hub_rt:get_global_pose())

    local max_ticks = 500
    local completed = false
    for tick = 1, max_ticks do
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

    check(kb_name .. " complete", completed,
        "KB did not complete in " .. max_ticks .. " ticks")

    -- Drain extra ticks for MQTT latency (same pattern as bridge test)
    if completed then
        local success = bb.kb_done_success
        if not success then
            for _ = 1, 50 do
                hub_rt:tick()
                ffi.C.usleep(2000)
                success = bb.kb_done_success
                if success then break end
            end
        end
        check(kb_name .. " success", success == true,
            "kb_done_success: " .. tostring(success))

        local pose = hub_rt:get_global_pose()
        m:action_complete(action_index, action, pose)

        print(string.format("  PASS: %-18s  global: x=%.0f y=%.0f heading=%.0f arm=%.0f",
            kb_name, pose.x, pose.y, pose.heading, pose.arm_angle))
    else
        m:action_failed(action_index, action, "timeout")
    end
end

---------------------------------------------------------------------------
-- 18-action route (same as test_hub_runtime.lua)
---------------------------------------------------------------------------

test_virtual_node(kb_by_name["init_check"], {
    test_id = 1, next_test = 2,
})

test_virtual_node(kb_by_name["path_spline"], {
    test_id = 2, next_test = 3,
    from_x = 0, from_y = 0, to_x = 800, to_y = 0,
    speed = 150, distance = 800, segment_index = 0, total_segments = 1,
})

test_virtual_node(kb_by_name["path_line"], {
    test_id = 3, next_test = 4,
    from_x = 800, from_y = 0, to_x = 1600, to_y = 0,
    speed = 120, distance = 800,
})

test_virtual_node(kb_by_name["path_rotate"], {
    test_id = 4, next_test = 5,
    from_heading = 0, to_heading = 90,
})

test_virtual_node(kb_by_name["path_wall"], {
    test_id = 5, next_test = 6,
    from_x = 1600, from_y = 0, to_x = 1600, to_y = 800,
    speed = 100, distance = 800, wall_standoff = 50,
})

test_virtual_node(kb_by_name["deliver_part"], {
    test_id = 6, next_test = 7,
    arm_target = -45, arm_speed = 80, arm_return = 0, payload_type = 1,
})

test_virtual_node(kb_by_name["path_spline"], {
    test_id = 7, next_test = 8,
    from_x = 1600, from_y = 800, to_x = 800, to_y = 800,
    speed = 130, distance = 800, segment_index = 0, total_segments = 1,
})

test_virtual_node(kb_by_name["paint_sample"], {
    test_id = 8, next_test = 9,
    arm_target = -60, arm_speed = 60, arm_return = 0, hold_time = 500,
})

test_virtual_node(kb_by_name["path_rotate"], {
    test_id = 9, next_test = 10,
    from_heading = 90, to_heading = 180,
})

test_virtual_node(kb_by_name["path_spline"], {
    test_id = 10, next_test = 11,
    from_x = 800, from_y = 800, to_x = 800, to_y = 1600,
    speed = 130, distance = 800, segment_index = 0, total_segments = 1,
})

test_virtual_node(kb_by_name["load_shipping"], {
    test_id = 11, next_test = 12,
    arm_target = -30, arm_speed = 80, arm_return = 0, payload_type = 2,
})

test_virtual_node(kb_by_name["path_line"], {
    test_id = 12, next_test = 13,
    from_x = 800, from_y = 1600, to_x = 0, to_y = 1600,
    speed = 120, distance = 800,
})

test_virtual_node(kb_by_name["pass_gate"], {
    test_id = 13, next_test = 14,
    rpc_open_hash = 1001, rpc_close_hash = 1002, drive_through = 200,
})

test_virtual_node(kb_by_name["path_line"], {
    test_id = 14, next_test = 15,
    from_x = 0, from_y = 1600, to_x = 0, to_y = 800,
    speed = 100, distance = 800,
})

test_virtual_node(kb_by_name["inspection_scan"], {
    test_id = 15, next_test = 16,
    sensor_port = 0, sensor_type = 0,
})

test_virtual_node(kb_by_name["path_rotate"], {
    test_id = 16, next_test = 17,
    from_heading = 180, to_heading = 270,
})

test_virtual_node(kb_by_name["path_spline"], {
    test_id = 17, next_test = 18,
    from_x = 0, from_y = 800, to_x = 0, to_y = 0,
    speed = 120, distance = 800, segment_index = 0, total_segments = 1,
})

test_virtual_node(kb_by_name["idle"], {
    test_id = 18, next_test = 0,
})

---------------------------------------------------------------------------
-- Finish mission + shutdown
---------------------------------------------------------------------------
local final_pose = hub_rt:get_global_pose()

m:finish({
    success    = (fail_count == 0),
    final_pose = final_pose,
    completed  = action_index,
    total      = 18,
})

hub_rt:send_shutdown()
hub_rt:close()
mqtt_hub:close()

---------------------------------------------------------------------------
-- Verify NATS KeyStore status
---------------------------------------------------------------------------
local live_status = m:read_status()
check("nats status exists", live_status ~= nil, "no status in KeyStore")
if live_status then
    check("nats status.type", live_status.type == "mission_complete",
        "expected mission_complete, got " .. tostring(live_status.type))
    check("nats status.success", live_status.success == true,
        "expected true, got " .. tostring(live_status.success))
    check("nats status.completed", live_status.completed == 18,
        "expected 18, got " .. tostring(live_status.completed))
    print(string.format("  NATS KeyStore status: type=%s, success=%s, completed=%s",
        tostring(live_status.type), tostring(live_status.success),
        tostring(live_status.completed)))
end

---------------------------------------------------------------------------
-- Verify NATS JetStream KV stream
---------------------------------------------------------------------------
local stream_entries = m:read_stream()
local stream_count = #stream_entries
check("nats stream has entries", stream_count > 0,
    "expected entries in JetStream KV stream, got " .. stream_count)
print(string.format("  NATS JetStream stream: %d entries", stream_count))

local type_counts = {}
for _, entry in ipairs(stream_entries) do
    if entry.type then
        type_counts[entry.type] = (type_counts[entry.type] or 0) + 1
    end
end

check("nats stream has mission_start", (type_counts["mission_start"] or 0) > 0,
    "no mission_start in stream")
check("nats stream has action_start", (type_counts["action_start"] or 0) > 0,
    "no action_start in stream")
check("nats stream has heartbeat", (type_counts["heartbeat"] or 0) > 0,
    "no heartbeat in stream")
check("nats stream has action_complete", (type_counts["action_complete"] or 0) > 0,
    "no action_complete in stream")
check("nats stream has mission_complete", (type_counts["mission_complete"] or 0) > 0,
    "no mission_complete in stream")

local type_summary = {}
for t, c in pairs(type_counts) do type_summary[#type_summary+1] = t .. "=" .. c end
table.sort(type_summary)
print("  NATS stream types: " .. table.concat(type_summary, ", "))

m:close()

---------------------------------------------------------------------------
-- Verify SQLite telemetry (bookend records)
---------------------------------------------------------------------------
local kb_runtime = require("kb_runtime")
local rt = kb_runtime.new(db_file, site, robot_id)

local status = rt:read_status()
check("sqlite status.mission_active", status.mission_active == false,
    "expected false, got " .. tostring(status.mission_active))
check("sqlite status.success", status.success == true,
    "expected true, got " .. tostring(status.success))
check("sqlite status.actions_complete", status.actions_complete == 18,
    "expected 18, got " .. tostring(status.actions_complete))

local beats = rt:read_heartbeats(100)
local beat_count = #beats
check("sqlite heartbeats exist", beat_count > 0,
    "expected heartbeats, got " .. beat_count)
print(string.format("  SQLite heartbeat bookends: %d records", beat_count))

if beat_count > 0 then
    local has_bookend = false
    for _, b in ipairs(beats) do
        if b.type == "action_start" or b.type == "action_complete" then
            has_bookend = true
            break
        end
    end
    check("sqlite has mission bookends", has_bookend,
        "no action_start/action_complete records found in stream")
end

rt:close()

---------------------------------------------------------------------------
-- Results
---------------------------------------------------------------------------
print(string.format("\n--- Results ---"))
print(string.format("Passed: %d", pass_count))
print(string.format("Failed: %d", fail_count))
print(string.format("Final global pose: x=%.0f y=%.0f heading=%.0f arm=%.0f",
    final_pose.x, final_pose.y, final_pose.heading, final_pose.arm_angle))

if fail_count == 0 then
    print("\nPASSED")
else
    print("\nFAILED")
    os.exit(1)
end
