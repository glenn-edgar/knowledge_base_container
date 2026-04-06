--[[
    test_sequencer.lua -- Validate sequencer.lua with the same 18-action route.

    Uses sequencer.new() / load_route() / run() / shutdown() API.
    Verifies: result table, NATS JetStream stream, KeyStore status, SQLite bookends.
    Transport: MQTT (via mqtt_hub_transport).
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
]]

local json_util   = require("json_util")
local sequencer   = require("sequencer")
local mqtt_hub_tx = require("mqtt_hub_transport")
local link_helper = require("test_link_helper")

local robot_id  = os.getenv("ROBOT_ID") or "rover_1"
local server    = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222"
local mqtt_host = os.getenv("MQTT_HOST") or "localhost"
local mqtt_port = tonumber(os.getenv("MQTT_PORT") or "1883")
local site      = os.getenv("VMRT_KB_SITE") or "moonbase.alpha.surface_ops"

local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local root_dir   = script_dir .. "../"
local hub_json   = root_dir .. "hub_dsl/hub.json"
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
-- Build the same 18-action route as a data structure
---------------------------------------------------------------------------
local route = {
    { kb_name = "init_check",      params = {} },
    { kb_name = "path_spline",     params = { from_x=0, from_y=0, to_x=800, to_y=0, speed=150, distance=800, segment_index=0, total_segments=1 } },
    { kb_name = "path_line",       params = { from_x=800, from_y=0, to_x=1600, to_y=0, speed=120, distance=800 } },
    { kb_name = "path_rotate",     params = { from_heading=0, to_heading=90 } },
    { kb_name = "path_wall",       params = { from_x=1600, from_y=0, to_x=1600, to_y=800, speed=100, distance=800, wall_standoff=50 } },
    { kb_name = "deliver_part",    params = { arm_target=-45, arm_speed=80, arm_return=0, payload_type=1 } },
    { kb_name = "path_spline",     params = { from_x=1600, from_y=800, to_x=800, to_y=800, speed=130, distance=800, segment_index=0, total_segments=1 } },
    { kb_name = "paint_sample",    params = { arm_target=-60, arm_speed=60, arm_return=0, hold_time=500 } },
    { kb_name = "path_rotate",     params = { from_heading=90, to_heading=180 } },
    { kb_name = "path_spline",     params = { from_x=800, from_y=800, to_x=800, to_y=1600, speed=130, distance=800, segment_index=0, total_segments=1 } },
    { kb_name = "load_shipping",   params = { arm_target=-30, arm_speed=80, arm_return=0, payload_type=2 } },
    { kb_name = "path_line",       params = { from_x=800, from_y=1600, to_x=0, to_y=1600, speed=120, distance=800 } },
    { kb_name = "pass_gate",       params = { rpc_open_hash=1001, rpc_close_hash=1002, drive_through=200 } },
    { kb_name = "path_line",       params = { from_x=0, from_y=1600, to_x=0, to_y=800, speed=100, distance=800 } },
    { kb_name = "inspection_scan", params = { sensor_port=0, sensor_type=0 } },
    { kb_name = "path_rotate",     params = { from_heading=180, to_heading=270 } },
    { kb_name = "path_spline",     params = { from_x=0, from_y=800, to_x=0, to_y=0, speed=120, distance=800, segment_index=0, total_segments=1 } },
    { kb_name = "idle",            params = {} },
}

---------------------------------------------------------------------------
-- Create sequencer, load route, run
---------------------------------------------------------------------------
local seq = sequencer.new({
    robot_id    = robot_id,
    db_file     = db_file,
    site        = site,
    nats_server = server,
    mqtt_hub    = mqtt_hub,
})

seq:load_route(route)

local valid, warnings = seq:validate_route()
check("route valid", valid, "route validation failed")
if #warnings > 0 then
    print(string.format("  Route warnings (%d): %s", #warnings, warnings[1]))
end

print("Sequencer initialized, route loaded. Running...\n")

local result = seq:run()

---------------------------------------------------------------------------
-- Verify result
---------------------------------------------------------------------------
check("result.success", result.success == true,
    "expected true, got " .. tostring(result.success))
check("result.completed", result.completed == 18,
    "expected 18, got " .. tostring(result.completed))
check("result.total", result.total == 18,
    "expected 18, got " .. tostring(result.total))
check("result.needs_replan", result.needs_replan == false,
    "expected false, got " .. tostring(result.needs_replan))
check("result.fault", result.fault == nil,
    "expected nil, got " .. tostring(result.fault))

local pose = result.final_pose
print(string.format("  Final pose: x=%.0f y=%.0f heading=%.0f arm=%.0f",
    pose.x, pose.y, pose.heading, pose.arm_angle))

---------------------------------------------------------------------------
-- Verify NATS KeyStore status
---------------------------------------------------------------------------
local m = seq:get_mission()
local live_status = m:read_status()
check("nats status exists", live_status ~= nil, "no status in KeyStore")
if live_status then
    check("nats status.type", live_status.type == "mission_complete",
        "expected mission_complete, got " .. tostring(live_status.type))
    check("nats status.success", live_status.success == true,
        "expected true, got " .. tostring(live_status.success))
    print(string.format("  NATS status: type=%s, success=%s",
        tostring(live_status.type), tostring(live_status.success)))
end

---------------------------------------------------------------------------
-- Verify NATS JetStream KV stream
---------------------------------------------------------------------------
local stream_entries = m:read_stream()
local stream_count = #stream_entries
check("nats stream has entries", stream_count > 0,
    "expected entries, got " .. stream_count)

local type_counts = {}
for _, entry in ipairs(stream_entries) do
    if entry.type then
        type_counts[entry.type] = (type_counts[entry.type] or 0) + 1
    end
end
check("nats stream has mission_start", (type_counts["mission_start"] or 0) > 0, "missing")
check("nats stream has action_complete", (type_counts["action_complete"] or 0) > 0, "missing")
check("nats stream has mission_complete", (type_counts["mission_complete"] or 0) > 0, "missing")

local type_summary = {}
for t, c in pairs(type_counts) do type_summary[#type_summary+1] = t .. "=" .. c end
table.sort(type_summary)
print(string.format("  NATS stream: %d entries (%s)", stream_count, table.concat(type_summary, ", ")))

---------------------------------------------------------------------------
-- Verify SQLite bookends
---------------------------------------------------------------------------
local kb_runtime = require("kb_runtime")
local rt = kb_runtime.new(db_file, site, robot_id)

local status = rt:read_status()
check("sqlite status.success", status.success == true,
    "expected true, got " .. tostring(status.success))
check("sqlite status.actions_complete", status.actions_complete == 18,
    "expected 18, got " .. tostring(status.actions_complete))

local beats = rt:read_heartbeats(100)
check("sqlite heartbeats exist", #beats > 0,
    "expected heartbeats, got " .. #beats)
print(string.format("  SQLite: status ok, %d heartbeat records", #beats))

rt:close()

---------------------------------------------------------------------------
-- Shutdown
---------------------------------------------------------------------------
seq:shutdown()
mqtt_hub:close()

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
