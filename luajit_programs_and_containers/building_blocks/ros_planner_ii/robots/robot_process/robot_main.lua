--[[
    robot_main.lua -- Independent robot process.

    Standalone LuaJIT process that connects to NATS, validates against
    the KB inventory, claims a slot, and runs the ChainTree tick loop.

    Template for a Pi Zero 2 deployment. The robot firmware (remote.json
    + user functions) is provisioned separately.

    Usage:
        luajit robot_main.lua <config.json>

    Config file format:
        {
            "robot_id": "rover_1",
            "site": "moonbase.alpha.surface_ops",
            "nats_server": "nats://127.0.0.1:4222",
            "robot_class": "lunar_rover",
            "remote_json": "remote.json"
        }

    Startup:
      1. Load config → validate inventory → claim slot
      2. Load ChainTree remote → register functions
      3. Activate controller KB → enter tick loop
      4. On shutdown or signal → release slot → exit
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
    typedef void (*sighandler_t)(int);
    sighandler_t signal(int signum, sighandler_t handler);
]]
ffi.C.signal(13, ffi.cast("sighandler_t", 1))  -- ignore SIGPIPE

---------------------------------------------------------------------------
-- Parse args
---------------------------------------------------------------------------

local config_path = arg and arg[1]
if not config_path then
    io.stderr:write("Usage: luajit robot_main.lua <config.json>\n")
    os.exit(1)
end

---------------------------------------------------------------------------
-- Load config, validate, claim slot
---------------------------------------------------------------------------

local robot_config = require("robot_config")
local cfg, err = robot_config.load(config_path)
if not cfg then
    io.stderr:write("ROBOT: startup failed: " .. err .. "\n")
    os.exit(1)
end

io.stderr:write(string.format("ROBOT [%s]: validated, slot claimed (energy=%d/%d)\n",
    cfg.robot_id, cfg.energy_remaining, cfg.energy_max))

---------------------------------------------------------------------------
-- Load ChainTree remote
---------------------------------------------------------------------------

local json_util     = require("json_util")
local ct_runtime    = require("ct_runtime")
local ct_loader     = require("ct_loader_pure")
local builtins      = require("ct_builtins")
local fn_registry   = require("fn_registry")
local defs          = require("ct_definitions")
local engine        = require("ct_engine")
local queue_monitor = require("queue_monitor")

local remote_data = ct_loader.load(cfg.remote_json)

-- Register user functions with transport
local remote_fns = require("remote_user_functions")
remote_fns.set_transport(cfg.transport)
remote_fns.set_energy(cfg.energy_max, cfg.energy_infinite)

-- Set initial energy to recovered value (not max)
local robot_energy = remote_fns.get_energy()
robot_energy.remaining = cfg.energy_remaining

fn_registry.register_functions(remote_data, builtins, remote_fns.registry)

local ok, missing = fn_registry.validate(remote_data)
if not ok then
    io.stderr:write("ROBOT [" .. cfg.robot_id .. "]: missing functions:\n")
    for _, m in ipairs(missing) do io.stderr:write("  " .. m .. "\n") end
    cfg.cleanup(cfg.energy_remaining)
    os.exit(1)
end

local remote_handle = ct_runtime.create({delta_time = 0.1, max_ticks = 50000}, remote_data)

-- Initialize blackboard booleans
remote_handle.blackboard.shutdown_requested = false
remote_handle.blackboard.watchdog_expired = false
remote_handle.blackboard.worker_done = false
remote_handle.blackboard.worker_success = false
remote_handle.blackboard.exec_active = false
remote_handle.blackboard.exec_start = false
remote_handle.blackboard.controller_active = false
remote_handle.blackboard.lookahead_pending = false

-- Activate controller KB
engine.init_test(remote_handle, "controller")
remote_handle.active_tests["controller"] = true
remote_handle.active_test_count = 1

-- Queue monitor
local monitor = queue_monitor.new({
    handle    = remote_handle,
    transport = cfg.transport,
    direction = "robot",
})

io.stderr:write(string.format("ROBOT [%s]: running (%s, %d capabilities)\n",
    cfg.robot_id, cfg.robot_class, #cfg.capabilities))
io.stderr:flush()

---------------------------------------------------------------------------
-- Tick loop with energy save and bitmask publishing
---------------------------------------------------------------------------

local ENERGY_SAVE_INTERVAL = 30  -- seconds
local BITMASK_PUBLISH_INTERVAL = 10  -- ticks
local last_energy_save = os.time()
local tick_count = 0

while true do
    if remote_handle.blackboard.shutdown_requested == true then
        break
    end

    -- Drain inbound queue → inject events
    monitor:tick()

    -- Timer events for ALL active KBs
    for kb_name, _ in pairs(remote_handle.active_tests) do
        local kb = remote_handle.kb_table[kb_name]
        if kb then
            table.insert(remote_handle.event_queue, {
                node_id  = kb.root_node,
                event_id = defs.CFL_TIMER_EVENT,
            })
        end
    end

    -- Process all events
    while #remote_handle.event_queue > 0 do
        local ev = table.remove(remote_handle.event_queue, 1)
        engine.execute_event(remote_handle, ev.node_id,
            ev.event_id, ev.event_data, ev.event_type)
    end

    tick_count = tick_count + 1

    -- Publish bitmask + heartbeat every N ticks
    if tick_count % BITMASK_PUBLISH_INTERVAL == 0 then
        local bb = remote_handle.blackboard
        local active_kb = bb.active_worker or ""
        local raw = 0
        local fields = {}

        -- Read bitmask from blackboard if a worker is active
        if active_kb ~= "" then
            raw = bb[active_kb .. ".bitmask"] or 0
        end

        robot_config.publish_bitmask(
            cfg.status_ks, cfg.site, cfg.robot_id,
            active_kb, raw, fields)
    end

    -- Save energy every 30 seconds
    local now = os.time()
    if now - last_energy_save >= ENERGY_SAVE_INTERVAL then
        last_energy_save = now
        local current_energy = remote_fns.get_energy()
        robot_config.save_energy(
            cfg.status_ks, cfg.site, cfg.robot_id,
            current_energy.max, current_energy.remaining)
    end

    ffi.C.usleep(1000)  -- 1ms tick
end

---------------------------------------------------------------------------
-- Shutdown: release slot, save final energy
---------------------------------------------------------------------------

local final_energy = remote_fns.get_energy()
io.stderr:write(string.format("ROBOT [%s]: shutdown (energy=%d/%d)\n",
    cfg.robot_id, final_energy.remaining, final_energy.max))

cfg.cleanup(final_energy.remaining)
io.stderr:flush()
