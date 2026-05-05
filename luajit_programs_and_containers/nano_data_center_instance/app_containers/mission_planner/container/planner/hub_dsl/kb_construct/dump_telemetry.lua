--[[
    dump_telemetry.lua -- Dump full telemetry from KB after a test run.

    Usage:
        luajit dump_telemetry.lua [robot_id]

    Requires surface_ops.db to exist (built by run_tests.sh nats_ct).
]]

local j = require("sqlite3_helpers").json
local kb_runtime = require("kb_runtime")

local robot_id = arg and arg[1] or "rover_1"
local site = "moonbase.alpha.surface_ops"
local db_file = "surface_ops.db"

local rt = kb_runtime.new(db_file, site, robot_id)

-- Status
print("=== Robot Status: " .. robot_id .. " ===\n")
local status = rt:read_status()
for k, v in pairs(status) do
    print(string.format("  %-20s = %s", k, tostring(v)))
end

-- Full heartbeat log
local count = rt:heartbeat_count()
print(string.format("\n=== Heartbeat Log (%d records) ===\n", count))

print(string.format("  %-8s  %4s  %-20s  %8s %8s %7s %7s  %6s %6s %7s  %s",
    "Phase", "Test", "Worker", "Glob_X", "Glob_Y", "Head", "Arm",
    "dX", "dY", "dHead", "WD"))
print(string.rep("-", 110))

local beats = rt:read_heartbeats(count)
-- Reverse to show chronological order
for i = #beats, 1, -1 do
    local b = beats[i]
    print(string.format("  %-8s  %4d  %-20s  %8.0f %8.0f %7.0f %7.0f  %6.0f %6.0f %7.0f  %4d",
        b.phase or "?",
        b.test_id or 0,
        b.worker or "?",
        b.global_x or 0, b.global_y or 0,
        b.global_heading or 0, b.global_arm_angle or 0,
        b.delta_x or 0, b.delta_y or 0,
        b.delta_heading or 0,
        b.watchdog_ticks or 0))
end

-- Connection info
print("\n=== Connection ===\n")
local conn = rt:read_connection()
for k, v in pairs(conn) do
    print(string.format("  %-20s = %s", k, tostring(v)))
end

rt:close()
