--[[
    hub_runtime.lua -- Hub ChainTree lifecycle manager.

    Wraps NATS transport, KeyStore blackboard, ChainTree runtime,
    function registration, queue monitor, and tick loop into a
    single reusable object.

    Usage:
        local hub_runtime = require("hub_runtime")
        local hub_rt = hub_runtime.new({
            robot_id     = "rover_1",
            nats_server  = "nats://127.0.0.1:4222",
            hub_json     = "hub_dsl/hub.json",
            initial_pose = { x = 0, y = 0, z = 0, heading = 0, arm_angle = 0 },
        })

        -- Stage action and activate KB
        hub_rt:get_blackboard().current_test_json = json_encode(action)
        hub_rt:activate_kb("path_spline")

        -- Tick until complete
        while not hub_rt:kb_is_complete("path_spline") do
            hub_rt:tick()
        end
        hub_rt:deactivate_kb("path_spline")

        -- Cleanup
        hub_rt:send_shutdown()
        hub_rt:close()
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
    typedef void (*sighandler_t)(int);
    sighandler_t signal(int signum, sighandler_t handler);
]]
ffi.C.signal(13, ffi.cast("sighandler_t", 1))  -- ignore SIGPIPE

local json_util      = require("json_util")
local cmd_packets    = require("command_packets")
local event_ids      = require("event_ids")
local nats_transport = require("nats_transport")
local queue_monitor  = require("queue_monitor")
local ks_blackboard  = require("ks_blackboard")
local hub_control    = require("hub_control")

local ct_runtime    = require("ct_runtime")
local ct_loader     = require("ct_loader_pure")
local builtins      = require("ct_builtins")
local fn_registry   = require("fn_registry")
local defs          = require("ct_definitions")
local engine        = require("ct_engine")
local packet_mapper = require("packet_mapper")

local M = {}
M.__index = M

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------
function M.new(opts)
    local self = setmetatable({}, M)

    local robot_id    = opts.robot_id    or error("hub_runtime: robot_id required")
    local nats_server = opts.nats_server or "nats://127.0.0.1:4222"
    local hub_json    = opts.hub_json    or error("hub_runtime: hub_json required")
    local initial_pose = opts.initial_pose or { x = 0, y = 0, z = 0, heading = 0, arm_angle = 0 }

    self.robot_id = robot_id

    -- NATS transport (two job queues: rpc + stream)
    self.tx = nats_transport.hub_side(robot_id, nats_server)
    self.tx:flush()

    -- KeyStore-backed blackboard
    self.bb = ks_blackboard.new(robot_id, nats_server)

    -- Load KB plugins
    local hub_dsl_mod = require("hub_dsl")
    self.plugins = hub_dsl_mod.plugins

    self.kb_by_name = {}
    self.kb_by_index = {}
    for _, p in ipairs(self.plugins) do
        self.kb_by_name[p.name] = p
        self.kb_by_index[p.index] = p
    end

    -- Load hub ChainTree
    local hub_data = ct_loader.load(hub_json)

    -- Register all functions
    fn_registry.register_functions(hub_data, builtins)

    local mapper_fns = { one_shot = {} }
    for _, plugin in ipairs(self.plugins) do
        local name, fn = packet_mapper.make_one_shot(plugin, json_util.decode)
        mapper_fns.one_shot[name] = fn
    end
    fn_registry.register_functions(hub_data, mapper_fns)

    local planner_chain = require("planner_start_next_test")
    planner_chain.set_plugins(self.plugins)
    fn_registry.register_functions(hub_data, planner_chain.registry)

    local event_handler_fns = require("event_handlers")
    fn_registry.register_functions(hub_data, event_handler_fns.registry)

    local error_recovery_fns = require("error_recovery")
    fn_registry.register_functions(hub_data, error_recovery_fns.registry)

    -- Create ChainTree runtime handle
    self.handle = ct_runtime.create({ delta_time = 0.1, max_ticks = 50000 }, hub_data)

    -- Replace in-memory blackboard with KeyStore-backed one
    self.handle.blackboard = self.bb

    -- Queue monitor: bridges NATS ↔ ChainTree events
    self.monitor = queue_monitor.new({
        handle    = self.handle,
        transport = self.tx,
        direction = "hub",
    })

    -- Set initial global pose
    hub_control.set_global_pose(initial_pose)

    return self
end

---------------------------------------------------------------------------
-- KB lifecycle
---------------------------------------------------------------------------
function M:activate_kb(kb_name)
    local kb = self.handle.kb_table[kb_name]
    if not kb then return false end

    -- Reset all nodes in this KB
    for _, nid in ipairs(kb.node_ids) do
        local n = self.handle.nodes[nid]
        if n then
            n.ct_control.enabled = false
            n.ct_control.initialized = false
        end
        self.handle.node_state[nid] = nil
    end

    -- Clear command buffers
    self.bb.command_packet = nil
    self.bb.command_packet_size = 0
    self.bb.command_packet_json = nil

    -- Track active KB
    local plugin = self.kb_by_name[kb_name]
    hub_control.on_kb_start(self.bb, kb_name, plugin)

    engine.init_test(self.handle, kb_name)
    self.handle.active_tests[kb_name] = true
    self.handle.active_test_count = (self.handle.active_test_count or 0) + 1
    return true
end

function M:kb_is_complete(kb_name)
    local kb = self.handle.kb_table[kb_name]
    if not kb then return true end
    return not engine.node_is_enabled(self.handle, kb.root_node)
end

function M:deactivate_kb(kb_name)
    if self.handle.active_tests[kb_name] then
        self.handle.active_tests[kb_name] = nil
        self.handle.active_test_count = self.handle.active_test_count - 1
    end
    hub_control.on_kb_done(self.bb, kb_name, nil)
end

---------------------------------------------------------------------------
-- Tick
---------------------------------------------------------------------------
function M:tick()
    -- Drain inbound NATS → inject ChainTree events
    self.monitor:tick()

    -- Update elapsed time
    hub_control.on_tick(self.bb)

    -- Inject TIMER_EVENT for each active KB
    for kb_name, _ in pairs(self.handle.active_tests) do
        local kb = self.handle.kb_table[kb_name]
        if kb then
            table.insert(self.handle.event_queue, {
                node_id  = kb.root_node,
                event_id = defs.CFL_TIMER_EVENT,
            })
        end
    end

    -- Drain ChainTree event queue
    while #self.handle.event_queue > 0 do
        local ev = table.remove(self.handle.event_queue, 1)
        engine.execute_event(self.handle, ev.node_id,
            ev.event_id, ev.event_data, ev.event_type)
    end

    -- Flush outbound commands to NATS
    self.monitor:flush_outbound()

    -- Persist dirty blackboard fields to KeyStore
    self.bb.flush()
end

---------------------------------------------------------------------------
-- Accessors
---------------------------------------------------------------------------
function M:get_blackboard()
    return self.bb
end

function M:get_global_pose()
    return hub_control.get_global_pose()
end

function M:get_plugins()
    return self.plugins
end

function M:get_kb_by_name()
    return self.kb_by_name
end

---------------------------------------------------------------------------
-- Shutdown and cleanup
---------------------------------------------------------------------------
function M:send_shutdown()
    self.tx:send_rpc(json_util.encode({
        packet_type = cmd_packets.TYPE_SHUTDOWN,
        seq = 99,
    }))
    ffi.C.usleep(500000)  -- give remote time to acknowledge
end

function M:close()
    self.tx:close()
    self.bb.close()
end

return M
