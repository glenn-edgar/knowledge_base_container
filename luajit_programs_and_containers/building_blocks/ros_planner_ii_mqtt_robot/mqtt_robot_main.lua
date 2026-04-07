--[[
    mqtt_robot_main.lua -- Independent MQTT robot process.

    Robot-independent controller (robot_controller.lua) runs directly.
    Workers run through ChainTree engine.

    Template for ESP32 / Pico 2 W deployment.

    Usage:
        luajit mqtt_robot_main.lua <config.json>

    Config file format:
        {
            "robot_id": "rover_1",
            "site": "moonbase.alpha.surface_ops",
            "mqtt_host": "localhost",
            "mqtt_port": 1883,
            "robot_class": "lunar_rover",
            "remote_json": "remote.json",
            "energy_max": 10000,
            "energy_infinite": false,
            "capabilities": ["init_check", "path_spline", ...]
        }
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
    io.stderr:write("Usage: luajit mqtt_robot_main.lua <config.json>\n")
    os.exit(1)
end

---------------------------------------------------------------------------
-- Load config, connect, claim slot
---------------------------------------------------------------------------

local mqtt_robot_config = require("mqtt_robot_config")
local cfg, err = mqtt_robot_config.load(config_path)
if not cfg then
    io.stderr:write("MQTT_ROBOT: startup failed: " .. err .. "\n")
    os.exit(1)
end

io.stderr:write(string.format("MQTT_ROBOT [%s]: validated, slot claimed (energy=%d/%d)\n",
    cfg.robot_id, cfg.energy_remaining, cfg.energy_max))

---------------------------------------------------------------------------
-- Load ChainTree remote (workers only — no controller KB)
---------------------------------------------------------------------------

local json_util     = require("json_util")
local ct_runtime    = require("ct_runtime")
local ct_loader     = require("ct_loader_pure")
local builtins      = require("ct_builtins")
local fn_registry   = require("fn_registry")
local defs          = require("ct_definitions")
local engine        = require("ct_engine")
local link_client   = require("link_client")
local robot_ctrl    = require("robot_controller")

local remote_data = ct_loader.load(cfg.remote_json)

-- Register worker functions
local remote_fns = require("remote_user_functions")

fn_registry.register_functions(remote_data, builtins, remote_fns.registry)

local ok, missing = fn_registry.validate(remote_data)
if not ok then
    io.stderr:write("MQTT_ROBOT [" .. cfg.robot_id .. "]: missing functions:\n")
    for _, m in ipairs(missing) do io.stderr:write("  " .. m .. "\n") end
    cfg.cleanup(cfg.energy_remaining)
    os.exit(1)
end

local remote_handle = ct_runtime.create({delta_time = 0.1, max_ticks = 50000}, remote_data)

-- Initialize blackboard booleans
remote_handle.blackboard.shutdown_requested = false
remote_handle.blackboard.worker_done = false
remote_handle.blackboard.worker_success = false
remote_handle.blackboard.worker_alive = false
remote_handle.blackboard.exec_active = false
remote_handle.blackboard.exec_start = false
remote_handle.blackboard.lookahead_pending = false
remote_handle.blackboard.active_worker = ""

-- No controller KB — controller runs directly via robot_controller
remote_handle.active_test_count = 0

-- Robot controller: dispatch, watchdog, heartbeat, completion
local ctrl = robot_ctrl.new({
    handle          = remote_handle,
    transport       = cfg.transport,
    energy_max      = cfg.energy_max,
    energy_infinite = cfg.energy_infinite,
})
ctrl:set_energy_remaining(cfg.energy_remaining)

-- Link client: manages registration and planner liveness
local lc = link_client.new({
    robot_id         = cfg.robot_id,
    transport        = cfg.transport,
    wire_format      = cfg.wire_format,
    capabilities     = cfg.capabilities,
    energy_max       = cfg.energy_max,
    energy_remaining = cfg.energy_remaining,
    on_planner_lost  = function(reason)
        io.stderr:write(string.format(
            "MQTT_ROBOT [%s]: planner lost (%s) — aborting mission\n",
            cfg.robot_id, reason))
        for kb_name, _ in pairs(remote_handle.active_tests) do
            remote_handle.active_tests[kb_name] = nil
        end
        remote_handle.active_test_count = 0
        local bb = remote_handle.blackboard
        bb.exec_active = false
        bb.exec_start = false
        bb.worker_done = false
        bb.worker_success = false
        bb.active_worker = ""
    end,
})

io.stderr:write(string.format("MQTT_ROBOT [%s]: running (%s, %d capabilities, wire=%s)\n",
    cfg.robot_id, cfg.robot_class, #cfg.capabilities, cfg.wire_format))
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

    -- Link protocol tick (announce, monitor planner heartbeats)
    lc:tick()

    -- Only process missions when registered with planner
    if lc:is_live() then
        -- Controller: drain RPC commands, watchdog, heartbeat, completion
        ctrl:tick()

        -- Workers: inject timer events and execute via ChainTree
        for kb_name, _ in pairs(remote_handle.active_tests) do
            local kb = remote_handle.kb_table[kb_name]
            if kb then
                table.insert(remote_handle.event_queue, {
                    node_id  = kb.root_node,
                    event_id = defs.CFL_TIMER_EVENT,
                })
            end
        end

        while #remote_handle.event_queue > 0 do
            local ev = table.remove(remote_handle.event_queue, 1)
            engine.execute_event(remote_handle, ev.node_id,
                ev.event_id, ev.event_data, ev.event_type)
        end
    end

    tick_count = tick_count + 1

    -- Publish bitmask + heartbeat every N ticks
    if tick_count % BITMASK_PUBLISH_INTERVAL == 0 then
        local bb = remote_handle.blackboard
        local active_kb = bb.active_worker or ""
        local raw = 0
        local fields = {}

        if active_kb ~= "" then
            raw = bb[active_kb .. ".bitmask"] or 0
        end

        mqtt_robot_config.publish_bitmask(
            cfg.status_pub, cfg.site, cfg.robot_id,
            active_kb, raw, fields)
    end

    -- Update link_client energy and save periodically
    local now = os.time()
    if now - last_energy_save >= ENERGY_SAVE_INTERVAL then
        last_energy_save = now
        local current_energy = ctrl:get_energy()
        lc:set_energy(current_energy.remaining)
        mqtt_robot_config.save_energy(
            cfg.status_pub, cfg.site, cfg.robot_id,
            current_energy.max, current_energy.remaining)
    end

    ffi.C.usleep(1000)  -- 1ms tick
end

---------------------------------------------------------------------------
-- Shutdown
---------------------------------------------------------------------------

local final_energy = ctrl:get_energy()
io.stderr:write(string.format("MQTT_ROBOT [%s]: shutdown (energy=%d/%d)\n",
    cfg.robot_id, final_energy.remaining, final_energy.max))

lc:shutdown()
cfg.cleanup(final_energy.remaining)
io.stderr:flush()
