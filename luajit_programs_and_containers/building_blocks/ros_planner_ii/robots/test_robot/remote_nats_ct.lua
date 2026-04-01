--[[
    remote_nats_ct.lua -- ChainTree remote process over NATS.

    Dual KB architecture:
      - controller KB: always active, receives commands, manages workers
      - worker KBs: dormant until controller activates them

    Usage: luajit remote_nats_ct.lua <robot_id>
]]

local ffi = require("ffi")
ffi.cdef[[ int usleep(unsigned int usec); ]]

local robot_id = arg and arg[1] or "test_robot_1"
local server   = os.getenv("NATS_SERVER") or "nats://127.0.0.1:4222"
local remote_json = os.getenv("VMRT_REMOTE_JSON")

if not remote_json then
    io.stderr:write("remote_nats_ct: VMRT_REMOTE_JSON required\n")
    os.exit(1)
end

io.stderr:write(string.format("REMOTE_CT [%s]: connecting to %s\n", robot_id, server))

-- NATS transport
local nats_transport = require("nats_transport")
local tx = nats_transport.remote_side(robot_id, server)
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
    local site = os.getenv("VMRT_KB_SITE") or "moonbase.alpha.surface_ops"
    local kb_rt = kb_runtime.new(kb_db_file, site, robot_id)
    remote_fns.set_kb_runtime(kb_rt)
    io.stderr:write(string.format("REMOTE_CT [%s]: KB runtime connected (%s)\n",
        robot_id, kb_db_file))
end

fn_registry.register_functions(remote_data, builtins, remote_fns.registry)

local ok, missing = fn_registry.validate(remote_data)
if not ok then
    io.stderr:write("remote_nats_ct: missing functions:\n")
    for _, m in ipairs(missing) do io.stderr:write("  " .. m .. "\n") end
    os.exit(1)
end

local remote_handle = ct_runtime.create({delta_time = 0.1, max_ticks = 50000}, remote_data)

-- Ensure boolean fields are proper booleans (DSL defaults are 0, not false)
remote_handle.blackboard.shutdown_requested = false
remote_handle.blackboard.watchdog_expired = false
remote_handle.blackboard.worker_done = false
remote_handle.blackboard.worker_success = false
remote_handle.blackboard.exec_active = false
remote_handle.blackboard.exec_start = false
remote_handle.blackboard.controller_active = false
remote_handle.blackboard.lookahead_pending = false

-- Activate ONLY the controller KB (workers stay dormant)
engine.init_test(remote_handle, "controller")
remote_handle.active_tests["controller"] = true
remote_handle.active_test_count = 1

-- Queue monitor: drains rpc queue, injects ROBOT_RPC_COMMAND events
local monitor = queue_monitor.new({
    handle    = remote_handle,
    transport = tx,
    direction = "robot",
})

io.stderr:write(string.format("REMOTE_CT [%s]: running (controller + %d workers)\n",
    robot_id, 7))
io.stderr:flush()

-- Tick loop
while true do
    if remote_handle.blackboard.shutdown_requested == true then
        break
    end

    -- Drain inbound queue → inject events
    monitor:tick()

    -- Timer events for ALL active KBs (controller + any active worker)
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

    ffi.C.usleep(1000)  -- 1ms tick
end

io.stderr:write(string.format("REMOTE_CT [%s]: shutdown\n", robot_id))
io.stderr:flush()
tx:close()
