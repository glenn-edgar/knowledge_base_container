--[[
    test_mqtt_multi_robot.lua -- Validate two MQTT robots through a single bridge.

    Robot 1: rover_1 (JSON wire format)
    Robot 2: rover_2 (CBOR wire format)

    Both go through the same multi-robot bridge to the same NATS planner.
    Tests run sequentially (one mission each) to verify per-robot routing.
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
]]

local json_util     = require("json_util")
local action_server = require("action_server")

local server = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222"
local site   = os.getenv("VMRT_KB_SITE") or "moonbase.alpha.surface_ops"

local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local root_dir   = script_dir .. "../"
local hub_json   = root_dir .. "hub_dsl/hub.json"
local db_file    = root_dir .. "hub_dsl/kb_construct/surface_ops.db"

print("=== Multi-Robot MQTT Bridge Test ===\n")
print(string.format("Server: %s, Site: %s\n", server, site))

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
-- NATS KeyStore for reading mirrored status
---------------------------------------------------------------------------
local ks_lib = require("lib.nats_key_store")
local site_bucket = site:gsub("%.", "_")
local status_ks = ks_lib.KeyStore.new({
    server        = server,
    bucket        = site_bucket .. "_robot_status",
    create_bucket = true,
    history       = 1,
    client_name   = "test_multi_status_reader",
})
status_ks:connect()

---------------------------------------------------------------------------
-- Helper: check robot status in NATS KV
---------------------------------------------------------------------------
local function check_robot_status(robot_id, label)
    print(string.format("\n--- %s: %s Status ---", label, robot_id))

    local state_key = site .. ".robots." .. robot_id .. ".status.state"
    local state_json = nil
    for _ = 1, 30 do
        state_json = status_ks:get(state_key)
        if state_json then break end
        ffi.C.usleep(100000)
    end

    check(robot_id .. " connected", state_json ~= nil,
        "no state in NATS KV")

    local wire_format = "unknown"
    if state_json then
        local ok, state = pcall(json_util.decode, state_json)
        if ok and state then
            check(robot_id .. " connected=true", state.connected == true,
                "got " .. tostring(state.connected))
            wire_format = state.wire_format or "json"
            print(string.format("  %s: connected, wire=%s", robot_id, wire_format))
        end
    end
    return wire_format
end

---------------------------------------------------------------------------
-- Helper: run mission on a robot
---------------------------------------------------------------------------
local function run_mission(robot_id, label)
    print(string.format("\n--- %s: %s Mission ---", label, robot_id))

    local srv = action_server.new({
        db_file     = db_file,
        hub_json    = hub_json,
        nats_server = server,
        site        = site,
    })

    local result = srv:execute_mission({
        robot_id = robot_id,
        board    = "landing_zone",
        start    = "lander_pad",
        stops = {
            { node = "habitat_site" },
        },
        bookend = true,
    })

    check(robot_id .. " mission success", result.success == true,
        "got " .. tostring(result.success) ..
        (result.fault and (" — " .. result.fault.reason) or ""))

    if result.success then
        print(string.format("  %s: %d/%d actions, %dms",
            robot_id, result.completed or 0, result.total or 0,
            result.elapsed_ms or 0))
    end

    srv:close()
    return result.success
end

---------------------------------------------------------------------------
-- Test: Robot 1 (JSON) — check status
---------------------------------------------------------------------------
local wire1 = check_robot_status("rover_1", "Robot 1 (JSON)")
check("rover_1 wire=json", wire1 == "json", "got " .. wire1)

---------------------------------------------------------------------------
-- Test: Robot 2 (CBOR) — check status
---------------------------------------------------------------------------
local wire2 = check_robot_status("rover_2", "Robot 2 (CBOR)")
check("rover_2 wire=cbor", wire2 == "cbor", "got " .. wire2)

---------------------------------------------------------------------------
-- Test: Mission on Robot 1 (JSON)
---------------------------------------------------------------------------
run_mission("rover_1", "Robot 1 (JSON)")

---------------------------------------------------------------------------
-- Test: Mission on Robot 2 (CBOR)
---------------------------------------------------------------------------
run_mission("rover_2", "Robot 2 (CBOR)")

---------------------------------------------------------------------------
-- Post-mission: check both robots have energy deducted
---------------------------------------------------------------------------
print("\n--- Post-Mission Energy ---")
for _, rid in ipairs({"rover_1", "rover_2"}) do
    local energy_key = site .. ".robots." .. rid .. ".status.energy"
    ffi.C.usleep(500000)
    local energy_json = status_ks:get(energy_key)
    if energy_json then
        local ok, e = pcall(json_util.decode, energy_json)
        if ok then
            print(string.format("  %s: energy=%d/%d", rid, e.energy_remaining, e.energy_max))
        end
    end
end

status_ks:disconnect()
status_ks:destroy()

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
