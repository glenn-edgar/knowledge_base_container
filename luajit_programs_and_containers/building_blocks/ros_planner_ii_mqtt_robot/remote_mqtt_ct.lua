--[[
    remote_mqtt_ct.lua -- ChainTree remote process over MQTT.

    Robot-independent controller (robot_controller.lua) runs directly.
    Workers run through ChainTree engine.

    Usage: luajit remote_mqtt_ct.lua <robot_id>

    Environment:
      MQTT_HOST        MQTT broker host (default: localhost)
      MQTT_PORT        MQTT broker port (default: 1883)
      VMRT_KB_SITE     KB site name (default: moonbase.alpha.surface_ops)
      VMRT_REMOTE_JSON Path to remote ChainTree JSON (required)
      VMRT_KB_DB       Path to KB database for status/stream (optional)
]]

local ffi = require("ffi")
ffi.cdef[[ int usleep(unsigned int usec); ]]

local robot_id    = arg and arg[1] or "test_robot_1"
local mqtt_host   = os.getenv("MQTT_HOST") or "localhost"
local mqtt_port   = tonumber(os.getenv("MQTT_PORT") or "1883")
local site        = os.getenv("VMRT_KB_SITE") or "moonbase.alpha.surface_ops"
local remote_json = os.getenv("VMRT_REMOTE_JSON")
local wire_format = os.getenv("VMRT_WIRE_FORMAT") or "json"

if not remote_json then
    io.stderr:write("remote_mqtt_ct: VMRT_REMOTE_JSON required\n")
    os.exit(1)
end

io.stderr:write(string.format("MQTT_ROBOT [%s]: connecting to %s:%d (wire=%s)\n",
    robot_id, mqtt_host, mqtt_port, wire_format))

-- MQTT transport (PubSub-backed, with optional CBOR encoding)
local mqtt_transport = require("mqtt_transport")
local tx = mqtt_transport.remote_side(robot_id, mqtt_host, mqtt_port, site,
    { wire_format = wire_format })
tx:flush()

-- ChainTree runtime (workers only — no controller KB)
local ct_runtime    = require("ct_runtime")
local ct_loader     = require("ct_loader_pure")
local builtins      = require("ct_builtins")
local fn_registry   = require("fn_registry")
local defs          = require("ct_definitions")
local engine        = require("ct_engine")
local link_client   = require("link_client")
local robot_ctrl    = require("robot_controller")

-- Load remote tree
local remote_data = ct_loader.load(remote_json)

-- Register worker functions
local remote_fns = require("remote_user_functions")

-- Optional: KB runtime for status/stream tables
local kb_rt = nil
local kb_db_file = os.getenv("VMRT_KB_DB")
if kb_db_file then
    local kb_runtime = require("kb_runtime")
    kb_rt = kb_runtime.new(kb_db_file, site, robot_id)
    io.stderr:write(string.format("MQTT_ROBOT [%s]: KB runtime connected (%s)\n",
        robot_id, kb_db_file))
end

fn_registry.register_functions(remote_data, builtins, remote_fns.registry)

local ok, missing = fn_registry.validate(remote_data)
if not ok then
    io.stderr:write("remote_mqtt_ct: missing functions:\n")
    for _, m in ipairs(missing) do io.stderr:write("  " .. m .. "\n") end
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
-- active_test_count starts at 0 (workers activated on demand)
remote_handle.active_test_count = 0

-- Robot controller: dispatch, watchdog, heartbeat, completion
local ctrl = robot_ctrl.new({
    handle     = remote_handle,
    transport  = tx,
    kb_rt      = kb_rt,
    energy_max = 10000,
})

-- Link client: manages registration and planner liveness
local lc = link_client.new({
    robot_id     = robot_id,
    transport    = tx,
    wire_format  = wire_format,
    capabilities = {"init_check", "path_spline", "path_line", "path_wall",
                    "path_rotate", "deliver_part", "paint_sample", "load_shipping",
                    "pass_gate", "inspection_scan", "recharge", "idle"},
    energy_max   = 10000,
    on_planner_lost = function(reason)
        io.stderr:write(string.format(
            "MQTT_ROBOT [%s]: planner lost (%s) — aborting\n", robot_id, reason))
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

io.stderr:write(string.format("MQTT_ROBOT [%s]: running (controller + workers)\n", robot_id))
io.stderr:flush()

-- Tick loop
while true do
    if remote_handle.blackboard.shutdown_requested == true then
        break
    end

    lc:tick()

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

    ffi.C.usleep(1000)  -- 1ms tick
end

io.stderr:write(string.format("MQTT_ROBOT [%s]: shutdown\n", robot_id))
io.stderr:flush()
lc:shutdown()
tx:close()
