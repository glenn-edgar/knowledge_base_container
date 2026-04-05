--[[
    planner_server.lua -- Persistent planner action server.

    Runs as a long-lived process. Connects MQTT hub for robot transport,
    drains NATS job queue for mission submissions, executes missions
    via coroutines. Results published to NATS KV for client polling.

    Started by: start_server.sh
    Missions submitted by: submit_mission.sh → mission_client.lua

    Requires:
      - KB built (surface_ops.db, hub.json, remote.json)
      - KB exported to NATS KV (start_server.sh does this)
      - NATS server running
      - MQTT broker running
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
    typedef void (*sighandler_t)(int);
    sighandler_t signal(int signum, sighandler_t handler);
]]
ffi.C.signal(13, ffi.cast("sighandler_t", 1))  -- ignore SIGPIPE

local action_server = require("action_server")
local mqtt_hub_tx   = require("mqtt_hub_transport")

local nats_server = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222"
local mqtt_host   = os.getenv("MQTT_HOST") or "localhost"
local mqtt_port   = tonumber(os.getenv("MQTT_PORT") or "1883")
local site        = os.getenv("VMRT_KB_SITE") or "moonbase.alpha.surface_ops"

-- Resolve paths relative to ros_planner_ii
local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local bb_dir     = script_dir .. "../"
local planner    = bb_dir .. "ros_planner_ii/"
local hub_json   = planner .. "hub_dsl/hub.json"
local db_file    = planner .. "hub_dsl/kb_construct/surface_ops.db"

---------------------------------------------------------------------------
-- Connect MQTT hub transport
---------------------------------------------------------------------------
local mqtt_hub = mqtt_hub_tx.new(mqtt_host, mqtt_port, site)
mqtt_hub:connect()
print(string.format("MQTT hub connected (%s:%d)", mqtt_host, mqtt_port))

---------------------------------------------------------------------------
-- Create action server with MQTT transport
---------------------------------------------------------------------------
local srv = action_server.new({
    db_file     = db_file,
    hub_json    = hub_json,
    nats_server = nats_server,
    site        = site,
    mqtt_hub    = mqtt_hub,
})

print(string.format("Action server ready (site=%s)", site))
print("Waiting for missions on NATS queue...\n")

---------------------------------------------------------------------------
-- Run persistent server (drains NATS job queue indefinitely)
---------------------------------------------------------------------------
local ok, err = pcall(function()
    srv:serve({ drain_nats = true })
end)

if not ok then
    io.stderr:write("Server error: " .. tostring(err) .. "\n")
end

---------------------------------------------------------------------------
-- Cleanup on shutdown
---------------------------------------------------------------------------
print("\nShutting down...")
pcall(function() srv:close() end)
pcall(function() mqtt_hub:close() end)
print("Server stopped.")
