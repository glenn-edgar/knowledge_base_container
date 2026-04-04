--[[
    test_remote_robot.lua -- Validate planner against an externally running robot.

    Assumes the robot is already running on NATS (e.g. on a Pi Zero 2 or
    another machine). No spawn, no cleanup, no shutdown sent.

    Tests:
      - Robot is connected (slot claimed, status published)
      - Mission execution via action server (coroutine + direct)
      - Energy and bitmask status after mission
      - Robot stays running after test completes

    Usage:
        ROBOT_ID=rover_1 NATS_SERVER=nats://... luajit test_remote_robot.lua
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
]]

local json_util     = require("json_util")
local action_server = require("action_server")

local robot_id = os.getenv("ROBOT_ID") or "rover_1"
local server   = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222"
local site     = os.getenv("VMRT_KB_SITE") or "moonbase.alpha.surface_ops"

local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")  or "./"
local root_dir   = script_dir .. "../"
local hub_json   = root_dir .. "hub_dsl/hub.json"
local db_file    = root_dir .. "hub_dsl/kb_construct/surface_ops.db"

print("=== Remote Robot Test ===\n")
print(string.format("Robot: %s, Server: %s, Site: %s\n", robot_id, server, site))

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
-- Step 1: Verify robot is connected
---------------------------------------------------------------------------
print("--- Connection Check ---")

local ks_lib = require("lib.nats_key_store")
local site_bucket = site:gsub("%.", "_")
local status_ks = ks_lib.KeyStore.new({
    server        = server,
    bucket        = site_bucket .. "_robot_status",
    create_bucket = false,
    history       = 1,
    client_name   = "remote_test_reader",
})
status_ks:connect()

local state_key   = site .. ".robots." .. robot_id .. ".status.state"
local energy_key  = site .. ".robots." .. robot_id .. ".status.energy"
local bitmask_key = site .. ".robots." .. robot_id .. ".status.bitmask"

local state_json = status_ks:get(state_key)
check("robot state exists", state_json ~= nil,
    "no state — is the robot running?")

local connected = false
if state_json then
    local ok, state = pcall(json_util.decode, state_json)
    check("state parse", ok, "corrupt JSON")
    if ok then
        connected = state.connected == true
        check("robot connected", connected,
            "connected=" .. tostring(state.connected) .. " — robot may not be running")
        check("robot_id matches", state.robot_id == robot_id,
            "got " .. tostring(state.robot_id))
        print(string.format("  Connected: %s, started: %s",
            tostring(state.connected), tostring(state.started_at)))
    end
end

-- Read initial energy
local energy_json = status_ks:get(energy_key)
check("energy published", energy_json ~= nil, "no energy status")

local initial_energy = 0
local energy_infinite = false
if energy_json then
    local ok, energy = pcall(json_util.decode, energy_json)
    if ok then
        check("energy_max > 0", energy.energy_max > 0,
            "got " .. tostring(energy.energy_max))
        initial_energy = energy.energy_remaining
        energy_infinite = energy.energy_infinite == true
        print(string.format("  Energy: %d/%d%s",
            energy.energy_remaining, energy.energy_max,
            energy_infinite and " (infinite)" or ""))
    end
end

if not connected then
    print("\nRobot not connected — cannot run missions. Aborting.")
    status_ks:disconnect(); status_ks:destroy()
    print(string.format("\n--- Results ---"))
    print(string.format("Passed: %d", pass_count))
    print(string.format("Failed: %d", fail_count))
    print("\nFAILED")
    os.exit(1)
end

---------------------------------------------------------------------------
-- Step 2: Coroutine mission via action server
---------------------------------------------------------------------------
print("\n--- Coroutine Mission ---")

local srv = action_server.new({
    db_file     = db_file,
    hub_json    = hub_json,
    nats_server = server,
    site        = site,
})

srv:submit({
    robot_id = robot_id,
    board    = "landing_zone",
    start    = "lander_pad",
    stops = {
        { node = "habitat_site" },
    },
    bookend = true,
})

srv:serve()

local results = srv:get_results()
check("coroutine result exists", results[robot_id] ~= nil,
    "no result for " .. robot_id)
if results[robot_id] then
    local r = results[robot_id]
    check("coroutine success", r.success == true,
        "expected true, got " .. tostring(r.success))
    if r.fault then
        print(string.format("  Fault: %s — %s",
            tostring(r.fault.reason), tostring(r.fault.detail)))
    end
    print(string.format("  Coroutine mission: success=%s, completed=%s",
        tostring(r.success), tostring(r.completed)))
end

---------------------------------------------------------------------------
-- Step 3: Second coroutine mission (multi-stop)
-- Note: execute_mission sends shutdown to remote, so we use coroutine
-- path only — the remote is external and must stay alive.
---------------------------------------------------------------------------
print("\n--- Multi-Stop Coroutine Mission ---")

srv:submit({
    robot_id = robot_id,
    board    = "landing_zone",
    start    = "lander_pad",
    stops = {
        { node = "mining_zone_b", action = "deliver_part",
          params = { arm_target = -45, arm_speed = 80, arm_return = 0, payload_type = 1 } },
        { node = "lander_pad" },
    },
    bookend = true,
})

srv:serve()

local results2 = srv:get_results()
check("multi-stop result exists", results2[robot_id] ~= nil,
    "no result for " .. robot_id)
if results2[robot_id] then
    local r = results2[robot_id]
    check("multi-stop success", r.success == true,
        "expected true, got " .. tostring(r.success))
    if r.fault then
        print(string.format("  Fault: %s — %s",
            tostring(r.fault.reason), tostring(r.fault.detail)))
    end
    print(string.format("  Multi-stop mission: success=%s, completed=%s, elapsed=%sms",
        tostring(r.success), tostring(r.completed), tostring(r.elapsed_ms)))
end

srv:close()

---------------------------------------------------------------------------
-- Step 4: Verify status after missions
---------------------------------------------------------------------------
print("\n--- Post-Mission Status ---")

ffi.C.usleep(500000)  -- brief pause for final publishes

-- Check bitmask
local bitmask_json = status_ks:get(bitmask_key)
check("bitmask published", bitmask_json ~= nil, "no bitmask")
if bitmask_json then
    local ok, bm = pcall(json_util.decode, bitmask_json)
    if ok then
        check("bitmask heartbeat", bm.fields and bm.fields.heartbeat == true,
            "no heartbeat flag")
        print(string.format("  Bitmask: kb=%s, raw=%s, heartbeat=%s",
            tostring(bm.kb_name), tostring(bm.raw),
            tostring(bm.fields and bm.fields.heartbeat)))
    end
end

-- Check energy after missions
local final_energy_json = status_ks:get(energy_key)
if final_energy_json then
    local ok, fe = pcall(json_util.decode, final_energy_json)
    if ok then
        print(string.format("  Energy: %d/%d (started at %d)",
            fe.energy_remaining, fe.energy_max, initial_energy))
    end
end

-- Verify robot is still connected (we did NOT send shutdown)
local final_state_json = status_ks:get(state_key)
if final_state_json then
    local ok, fs = pcall(json_util.decode, final_state_json)
    if ok then
        check("robot still connected", fs.connected == true,
            "robot disconnected unexpectedly")
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
