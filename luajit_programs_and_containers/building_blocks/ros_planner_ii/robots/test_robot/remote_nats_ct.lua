--[[
    remote_nats_ct.lua -- ChainTree remote process over NATS.

    Standalone process. Connects to NATS independently.
    Queue monitor drains inbound rpc jobs, injects as events.
    ChainTree runtime processes events, user functions send responses.

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
tx:flush()  -- drain stale jobs from previous runs

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

fn_registry.register_functions(remote_data, builtins, remote_fns.registry)

local ok, missing = fn_registry.validate(remote_data)
if not ok then
    io.stderr:write("remote_nats_ct: missing functions:\n")
    for _, m in ipairs(missing) do io.stderr:write("  " .. m .. "\n") end
    os.exit(1)
end

local remote_handle = ct_runtime.create({delta_time = 0.1, max_ticks = 50000}, remote_data)

-- Activate remote_unit KB
engine.init_test(remote_handle, "remote_unit")
remote_handle.active_tests["remote_unit"] = true
remote_handle.active_test_count = 1

-- Queue monitor: drains rpc queue, injects ROBOT_RPC_COMMAND events
local monitor = queue_monitor.new({
    handle    = remote_handle,
    transport = tx,
    direction = "robot",
})

-- Ensure shutdown_requested is a proper boolean (DSL default is 0, not false)
remote_handle.blackboard.shutdown_requested = false

io.stderr:write(string.format("REMOTE_CT [%s]: running\n", robot_id))
io.stderr:flush()

-- Tick loop
local tick_count = 0
while true do
    tick_count = tick_count + 1
    if remote_handle.blackboard.shutdown_requested == true then
        io.stderr:write(string.format("REMOTE_CT [%s]: shutdown_requested at tick %d\n",
            robot_id, tick_count))
        io.stderr:flush()
        break
    end

    -- Drain inbound queue → inject events
    monitor:tick()

    -- Inject timer event for active KBs
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
