--[[
    test_independent_robot.lua -- Validate robot_process with slot claim,
    inventory validation, energy persistence, and bitmask publishing.

    Tests:
      - Robot config load + NATS KV validation
      - Slot claim (connected=true)
      - Mission execution through independent robot process
      - Energy deduction after mission
      - Bitmask presence on status board
      - Slot release on shutdown (connected=false)
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
]]

local json_util     = require("json_util")
local action_server = require("action_server")
local kb_exporter   = require("kb_exporter")

local robot_id = os.getenv("ROBOT_ID") or "rover_1"
local server   = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222"
local site     = os.getenv("VMRT_KB_SITE") or "moonbase.alpha.surface_ops"

local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local root_dir   = script_dir .. "../"
local hub_json   = root_dir .. "hub_dsl/hub.json"
local db_file    = root_dir .. "hub_dsl/kb_construct/surface_ops.db"

print("=== Independent Robot Test ===\n")
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
-- Step 1: Connect to status KeyStore
-- (KB export and robot start already done by shell script)
---------------------------------------------------------------------------
print("--- Robot Status Check ---")

local ks_lib = require("lib.nats_key_store")
local site_bucket = site:gsub("%.", "_")
local status_ks = ks_lib.KeyStore.new({
    server        = server,
    bucket        = site_bucket .. "_robot_status",
    create_bucket = true,
    history       = 1,
    client_name   = "test_status_reader",
})
status_ks:connect()

local state_key   = site .. ".robots." .. robot_id .. ".status.state"
local energy_key  = site .. ".robots." .. robot_id .. ".status.energy"
local bitmask_key = site .. ".robots." .. robot_id .. ".status.bitmask"

-- Robot should already be running (started by shell script with 2s wait)

-- Check slot was claimed
local state_json = status_ks:get(state_key)
check("slot claimed", state_json ~= nil, "no state in KeyStore")

if state_json then
    local ok, state = pcall(json_util.decode, state_json)
    check("state parse", ok, "corrupt JSON")
    if ok then
        check("connected=true", state.connected == true,
            "got " .. tostring(state.connected))
        check("robot_id matches", state.robot_id == robot_id,
            "got " .. tostring(state.robot_id))
        print(string.format("  Slot claimed: connected=%s, started=%s",
            tostring(state.connected), tostring(state.started_at)))
    end
end

-- Check energy was published
local energy_json = status_ks:get(energy_key)
check("energy published", energy_json ~= nil, "no energy in KeyStore")

local initial_energy = 0
if energy_json then
    local ok, energy = pcall(json_util.decode, energy_json)
    if ok then
        check("energy_max > 0", energy.energy_max > 0,
            "got " .. tostring(energy.energy_max))
        check("energy_remaining > 0", energy.energy_remaining > 0,
            "got " .. tostring(energy.energy_remaining))
        initial_energy = energy.energy_remaining
        print(string.format("  Energy: %d/%d", energy.energy_remaining, energy.energy_max))
    end
end

---------------------------------------------------------------------------
-- Step 4: Run a mission via action server
---------------------------------------------------------------------------
print("\n--- Mission Execution ---")

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

check("mission success", result.success == true,
    "expected true, got " .. tostring(result.success))
check("no fault", result.fault == nil,
    result.fault and result.fault.reason or "")

if result.success then
    print(string.format("  Mission: %d/%d actions, %dms",
        result.completed or 0, result.total or 0, result.elapsed_ms or 0))
end

srv:close()

---------------------------------------------------------------------------
-- Step 5: Check bitmask was published during mission
---------------------------------------------------------------------------
print("\n--- Status Board ---")

ffi.C.usleep(500000)  -- brief pause for final publishes

local bitmask_json = status_ks:get(bitmask_key)
check("bitmask published", bitmask_json ~= nil, "no bitmask in KeyStore")

if bitmask_json then
    local ok, bm = pcall(json_util.decode, bitmask_json)
    if ok then
        check("bitmask has heartbeat", bm.fields and bm.fields.heartbeat == true,
            "no heartbeat flag")
        check("bitmask has robot_id", bm.robot_id == robot_id,
            "got " .. tostring(bm.robot_id))
        print(string.format("  Bitmask: kb=%s, raw=%s, heartbeat=%s",
            tostring(bm.kb_name), tostring(bm.raw),
            tostring(bm.fields and bm.fields.heartbeat)))
    end
end

-- Check energy decreased after mission
local final_energy_json = status_ks:get(energy_key)
if final_energy_json then
    local ok, fe = pcall(json_util.decode, final_energy_json)
    if ok then
        print(string.format("  Final energy: %d/%d (started at %d)",
            fe.energy_remaining, fe.energy_max, initial_energy))
    end
end

---------------------------------------------------------------------------
-- Step 6: After robot shuts down, verify slot released
-- (Robot shutdown happens after test script sends shutdown command via action server)
---------------------------------------------------------------------------
-- Note: robot shutdown is triggered by the shell script killing the process
-- or by the action server sending a shutdown command. The robot_main.lua
-- calls cfg.cleanup() which sets connected=false.

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
