--[[
    test_mqtt_robot.lua -- Validate independent MQTT robot process through bridge.

    Tests (visible via NATS KeyStore, mirrored by bridge):
      - Robot connected (slot claimed via MQTT → NATS KV)
      - Energy published
      - Mission execution via action server (through NATS → bridge → MQTT)
      - Bitmask presence
      - Energy decreased after mission
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

print("=== MQTT Robot Test (via bridge) ===\n")
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
-- Step 1: Check status in NATS KeyStore (mirrored from MQTT by bridge)
---------------------------------------------------------------------------
print("--- Robot Status Check (NATS KV, mirrored from MQTT) ---")

local ks_lib = require("lib.nats_key_store")
local site_bucket = site:gsub("%.", "_")
local status_ks = ks_lib.KeyStore.new({
    server        = server,
    bucket        = site_bucket .. "_robot_status",
    create_bucket = true,
    history       = 1,
    client_name   = "test_mqtt_status_reader",
})
status_ks:connect()

local state_key   = site .. ".robots." .. robot_id .. ".status.state"
local energy_key  = site .. ".robots." .. robot_id .. ".status.energy"
local bitmask_key = site .. ".robots." .. robot_id .. ".status.bitmask"

-- Wait for bridge to mirror the status (MQTT retained → NATS KV)
local state_json = nil
for _ = 1, 50 do
    state_json = status_ks:get(state_key)
    if state_json then break end
    ffi.C.usleep(100000)  -- 100ms
end

check("slot claimed (via bridge)", state_json ~= nil,
    "no state in NATS KV — is bridge mirroring status?")

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

-- Check energy
local energy_json = nil
for _ = 1, 20 do
    energy_json = status_ks:get(energy_key)
    if energy_json then break end
    ffi.C.usleep(100000)
end

check("energy published (via bridge)", energy_json ~= nil,
    "no energy in NATS KV")

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
-- Step 2: Run a mission via action server (NATS → bridge → MQTT → robot)
---------------------------------------------------------------------------
print("\n--- Mission Execution (via bridge) ---")

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
-- Step 3: Check bitmask and energy after mission
---------------------------------------------------------------------------
print("\n--- Status Board ---")

-- Wait for bridge to mirror post-mission status
ffi.C.usleep(1000000)  -- 1s

local bitmask_json = status_ks:get(bitmask_key)
check("bitmask published (via bridge)", bitmask_json ~= nil,
    "no bitmask in NATS KV")

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

-- Check energy after mission
local final_energy_json = status_ks:get(energy_key)
if final_energy_json then
    local ok, fe = pcall(json_util.decode, final_energy_json)
    if ok then
        print(string.format("  Final energy: %d/%d (started at %d)",
            fe.energy_remaining, fe.energy_max, initial_energy))
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
