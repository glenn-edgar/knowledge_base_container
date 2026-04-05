--[[
    hub_runtime.lua -- Hub ChainTree lifecycle manager.

    Wraps transport, KeyStore blackboard, ChainTree runtime,
    function registration, queue monitor, and tick loop into a
    single reusable object.

    Transport is injected — MQTT (via mqtt_hub_transport:robot_transport())
    or any object with send_rpc(json) / recv_stream() / close().

    Usage:
        local hub_runtime = require("hub_runtime")
        local mqtt_hub = require("mqtt_hub_transport")
        local hub = mqtt_hub.new("localhost", 1883, "moonbase.alpha.surface_ops")
        hub:connect()

        local hub_rt = hub_runtime.new({
            robot_id     = "rover_1",
            nats_server  = "nats://127.0.0.1:4222",
            hub_json     = "hub_dsl/hub.json",
            transport    = hub:robot_transport("rover_1"),
            mqtt_hub     = hub,   -- optional: auto-poll in tick()
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
local queue_monitor  = require("queue_monitor")
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
    local site        = opts.site        or "moonbase.alpha.surface_ops"
    local initial_pose = opts.initial_pose or { x = 0, y = 0, z = 0, heading = 0, arm_angle = 0 }

    self.robot_id = robot_id
    self.site = site

    -- Transport: must be injected (MQTT robot_transport adapter or compatible)
    self.tx = opts.transport or error("hub_runtime: transport required (use mqtt_hub:robot_transport())")

    -- Optional: shared MQTT hub for auto-polling in tick()
    -- When set, tick() calls mqtt_hub:poll_and_route() to drain MQTT messages
    -- into per-robot stream buffers before queue_monitor drains them.
    self.mqtt_hub = opts.mqtt_hub

    -- In-memory blackboard (no NATS on tick path)
    local bb_cache = {}
    self.bb = setmetatable({
        flush = function() end,  -- no-op: nothing to persist
        close = function() end,  -- no-op
    }, {
        __index = function(_, key)
            if key == "flush" or key == "close" then return rawget(_, key) end
            return bb_cache[key]
        end,
        __newindex = function(_, key, value)
            if key == "flush" or key == "close" then rawset(_, key, value); return end
            bb_cache[key] = value
        end,
    })

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

    -- Queue monitor: bridges transport ↔ ChainTree events
    self.monitor = queue_monitor.new({
        handle    = self.handle,
        transport = self.tx,
        direction = "hub",
    })

    -- Per-instance pose tracking (not shared — safe for coroutines)
    self.hub_control = hub_control.new(initial_pose)

    -- Status publishing via kv-bridge (MQTT → NATS KV, non-blocking)
    local site_bucket = site:gsub("%.", "_")
    self._status_bucket = site_bucket .. "_robot_status"
    self._bitmask_key = site .. ".robots." .. robot_id .. ".status.bitmask"
    self._energy_key  = site .. ".robots." .. robot_id .. ".status.energy"

    -- Energy tracking (initialized from KB or defaults)
    self.energy_max       = opts.energy_max or 10000
    self.energy_remaining = opts.energy_remaining or self.energy_max
    self.energy_infinite  = opts.energy_infinite or false

    -- Bitmask change tracking (only publish on change)
    self._last_bitmask_raw = nil
    self._last_active_kb   = nil
    self._tick_count       = 0

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
    self.hub_control:on_kb_start(self.bb, kb_name, plugin)

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
    self.hub_control:on_kb_done(self.bb, kb_name, nil)
end

---------------------------------------------------------------------------
-- Tick
---------------------------------------------------------------------------
function M:tick()
    -- If MQTT hub provided, poll and route stream messages to buffers
    if self.mqtt_hub then
        self.mqtt_hub:poll_and_route(1)
    end

    -- Drain inbound transport → inject ChainTree events
    self.monitor:tick()

    -- Update elapsed time
    self.hub_control:on_tick(self.bb)

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

    -- Flush outbound commands to transport
    self.monitor:flush_outbound()

    -- Publish bitmask + energy via kv-bridge (every 10 ticks or on change)
    self._tick_count = self._tick_count + 1
    if self._tick_count % 10 == 0 then
        self:_publish_robot_status()
    end
end

---------------------------------------------------------------------------
-- Robot status publishing (bitmask + energy → kv-bridge via MQTT)
-- Fire-and-forget: MQTT publish is microseconds, never blocks tick.
-- kv-bridge container writes to NATS KV asynchronously.
---------------------------------------------------------------------------

function M:_publish_robot_status()
    if not self.mqtt_hub then return end

    local bb = self.bb
    local active_kb = bb.active_kb or ""

    -- Read current bitmask from blackboard
    local raw = 0
    local fields = {}
    if active_kb ~= "" then
        raw = bb[active_kb .. ".bitmask"] or 0
        -- Decode bitmask fields using plugin schema
        local plugin = self.kb_by_name[active_kb]
        if plugin and plugin.bitmask then
            for _, field in ipairs(plugin.bitmask) do
                local bit_val = 2 ^ field.bit
                fields[field.name] = (math.floor(raw / bit_val) % 2) == 1
            end
        end
    end

    -- Heartbeat: bit 0 is always the heartbeat flag (robot sets to 1)
    if raw % 2 == 0 then raw = raw + 1 end
    fields.heartbeat = true

    -- Only publish if changed or periodic
    if raw ~= self._last_bitmask_raw or active_kb ~= self._last_active_kb then
        self._last_bitmask_raw = raw
        self._last_active_kb   = active_kb

        local bitmask_json = json_util.encode({
            kb_name = active_kb,
            raw     = raw,
            fields  = fields,
            robot_id = self.robot_id,
            timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        })
        self.mqtt_hub:publish_kv(self._status_bucket, self._bitmask_key, bitmask_json)
    end

    -- Publish energy status
    local energy_json = json_util.encode({
        energy_max       = self.energy_max,
        energy_remaining = self.energy_remaining,
        robot_id         = self.robot_id,
        timestamp        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    })
    self.mqtt_hub:publish_kv(self._status_bucket, self._energy_key, energy_json)
end

--- Deduct energy cost for an action (no-op when energy_infinite).
function M:deduct_energy(cost)
    if self.energy_infinite then return end
    self.energy_remaining = math.max(0, self.energy_remaining - cost)
end

--- Get current energy remaining.
function M:get_energy_remaining()
    return self.energy_remaining
end

--- Recharge energy to max.
function M:recharge_energy()
    self.energy_remaining = self.energy_max
end

---------------------------------------------------------------------------
-- Accessors
---------------------------------------------------------------------------
function M:get_blackboard()
    return self.bb
end

function M:get_global_pose()
    return self.hub_control:get_global_pose()
end

function M:get_hub_control()
    return self.hub_control
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
