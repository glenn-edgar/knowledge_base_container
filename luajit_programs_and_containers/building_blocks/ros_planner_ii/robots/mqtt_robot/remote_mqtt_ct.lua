--[[
    remote_mqtt_ct.lua -- ChainTree remote process over MQTT.

    Identical to remote_nats_ct.lua but uses mqtt_transport (PubSub-backed).
    Same ChainTree, same user functions, same queue_monitor.

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

if not remote_json then
    io.stderr:write("remote_mqtt_ct: VMRT_REMOTE_JSON required\n")
    os.exit(1)
end

io.stderr:write(string.format("MQTT_ROBOT [%s]: connecting to %s:%d\n",
    robot_id, mqtt_host, mqtt_port))

-- MQTT transport (PubSub-backed)
local mqtt_transport = require("mqtt_transport")
local tx = mqtt_transport.remote_side(robot_id, mqtt_host, mqtt_port, site)
tx:flush()

-- ChainTree runtime
local ct_runtime    = require("ct_runtime")
local ct_loader     = require("ct_loader_pure")
local builtins      = require("ct_builtins")
local fn_registry   = require("fn_registry")
local defs          = require("ct_definitions")
local engine        = require("ct_engine")
local queue_monitor = require("queue_monitor")

-- Load remote tree
local remote_data = ct_loader.load(remote_json)

-- Register user functions with transport
local remote_fns = require("remote_user_functions")
remote_fns.set_transport(tx)

-- Optional: KB runtime for status/stream tables
local kb_db_file = os.getenv("VMRT_KB_DB")
if kb_db_file then
    local kb_runtime = require("kb_runtime")
    local kb_rt = kb_runtime.new(kb_db_file, site, robot_id)
    remote_fns.set_kb_runtime(kb_rt)
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

-- Ensure boolean fields are proper booleans
remote_handle.blackboard.shutdown_requested = false
remote_handle.blackboard.watchdog_expired = false
remote_handle.blackboard.worker_done = false
remote_handle.blackboard.worker_success = false
remote_handle.blackboard.exec_active = false
remote_handle.blackboard.exec_start = false
remote_handle.blackboard.controller_active = false
remote_handle.blackboard.lookahead_pending = false

-- Activate ONLY the controller KB
engine.init_test(remote_handle, "controller")
remote_handle.active_tests["controller"] = true
remote_handle.active_test_count = 1

-- Queue monitor: drains rpc queue, injects ROBOT_RPC_COMMAND events
local monitor = queue_monitor.new({
    handle    = remote_handle,
    transport = tx,
    direction = "robot",
})

io.stderr:write(string.format("MQTT_ROBOT [%s]: running (controller + workers)\n", robot_id))
io.stderr:flush()

-- Tick loop
while true do
    if remote_handle.blackboard.shutdown_requested == true then
        break
    end

    monitor:tick()

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

    ffi.C.usleep(1000)  -- 1ms tick
end

io.stderr:write(string.format("MQTT_ROBOT [%s]: shutdown\n", robot_id))
io.stderr:flush()
tx:close()
