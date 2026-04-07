--[[
    hub_runtime.lua -- State-machine hub runtime (no ChainTree).

    Replaces the ChainTree-based hub with a simple state machine.
    Every virtual node follows the same cycle:
      send_command → wait_ack → active (bitmaps) → wait_kb_done → done

    VN definitions (packet_type_id, json_schema, bitmask, pose_fields)
    come from the SQLite KB at startup. Adding a new VN to the KB is
    all that's needed — no hub_dsl, no hub.json, no plugin files.

    Same API as before:
        hub_rt:activate_kb(kb_name)
        hub_rt:tick()
        hub_rt:kb_is_complete(kb_name)
        hub_rt:deactivate_kb(kb_name)
        hub_rt:get_blackboard()
        hub_rt:get_global_pose()
        hub_rt:send_shutdown()
        hub_rt:close()
]]

local ffi = require("ffi")
ffi.cdef[[
    int usleep(unsigned int usec);
]]

local json_util   = require("json_util")
local hub_control = require("hub_control")
local event_ids   = require("event_ids")

local M = {}
M.__index = M

-- Action state machine states
local STATE_IDLE      = "idle"
local STATE_WAIT_ACK  = "wait_ack"
local STATE_ACTIVE    = "active"
local STATE_DONE      = "done"
local STATE_ERROR     = "error"

-- Timeouts (wall-clock seconds)
local ACK_TIMEOUT     = 5
local KB_DONE_TIMEOUT = 10

-- Sequence counter
local seq_counter = 0

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------
function M.new(opts)
    local self = setmetatable({}, M)

    local robot_id = opts.robot_id or error("hub_runtime: robot_id required")
    local site     = opts.site     or error("hub_runtime: site required")
    local initial_pose = opts.initial_pose or { x = 0, y = 0, z = 0, heading = 0, arm_angle = 0 }

    self.robot_id = robot_id
    self.site = site

    -- Transport: must be injected
    self.tx = opts.transport or error("hub_runtime: transport required")

    -- Optional: shared MQTT hub for auto-polling in tick()
    self.mqtt_hub = opts.mqtt_hub

    -- In-memory blackboard (compatible with sequencer expectations)
    local bb_cache = {}
    self.bb = setmetatable({
        flush = function() end,
        close = function() end,
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

    -- Load VN definitions from KB
    self.vn_defs = {}    -- kb_name → { packet_type_id, json_schema, bitmask, pose_fields }
    self.kb_by_name = {} -- kb_name → { name, index, packet_type_id, bitmask, ... }
    if opts.db_file then
        local kb_query = require("kb_query")
        local ltree_path = opts.ltree_path or "/usr/local/lib/ltree"
        local q = kb_query.new(opts.db_file, "knowledge_base", ltree_path, opts.site)
        local all_vns = q:get_all_virtual_nodes()
        q:close()

        local idx = 0
        for name, vn in pairs(all_vns) do
            idx = idx + 1
            self.vn_defs[name] = vn
            self.kb_by_name[name] = {
                name           = name,
                index          = idx,
                packet_type_id = vn.packet_type_id,
                json_schema    = vn.json_schema or {},
                bitmask        = vn.bitmask or {},
                pose_fields    = vn.pose_fields or {},
            }
        end
    end

    -- Also accept plugins from opts (for backward compat with tests)
    if opts.plugins then
        for _, p in ipairs(opts.plugins) do
            self.kb_by_name[p.name] = p
        end
    end

    -- Per-instance pose tracking
    self.hub_control = hub_control.new(initial_pose)

    -- Action state machine
    self.action_state = STATE_IDLE
    self.active_kb    = nil
    self.ack_deadline = nil
    self.kb_done_deadline = nil

    -- Status publishing via kv-bridge (MQTT → NATS KV, non-blocking)
    local site_bucket = site:gsub("%.", "_")
    self._status_bucket = site_bucket .. "_robot_status"
    self._bitmask_key = site .. ".robots." .. robot_id .. ".status.bitmask"
    self._energy_key  = site .. ".robots." .. robot_id .. ".status.energy"

    -- Energy tracking
    self.energy_max       = opts.energy_max or 10000
    self.energy_remaining = opts.energy_remaining or self.energy_max
    self.energy_infinite  = opts.energy_infinite or false

    -- Bitmask change tracking
    self._last_bitmask_raw = nil
    self._last_active_kb   = nil
    self._tick_count       = 0

    return self
end

---------------------------------------------------------------------------
-- KB lifecycle (same API as before)
---------------------------------------------------------------------------

function M:activate_kb(kb_name)
    local plugin = self.kb_by_name[kb_name]
    if not plugin then return false end

    -- Reset blackboard for new action
    self.bb.ack_received = false
    self.bb.ack_seq = nil
    self.bb.ack_status = nil
    self.bb.kb_done_received = false
    self.bb.kb_done_success = nil
    self.bb.fault_reason = ""
    self.bb.active_kb = kb_name

    -- Track active KB
    self.hub_control:on_kb_start(self.bb, kb_name, plugin)

    -- Build and send command
    local json_str = self.bb.current_test_json
    if json_str then
        local ok, action_json = pcall(json_util.decode, json_str)
        if ok and action_json then
            seq_counter = seq_counter + 1
            action_json.packet_type = plugin.packet_type_id
            action_json.seq = seq_counter
            self.tx:send_rpc(json_util.encode(action_json))
        end
    end

    -- Start ACK timeout
    self.action_state = STATE_WAIT_ACK
    self.active_kb = kb_name
    self.ack_deadline = os.time() + ACK_TIMEOUT
    self.kb_done_deadline = nil

    return true
end

function M:kb_is_complete(kb_name)
    if self.active_kb ~= kb_name then return true end
    return self.action_state == STATE_DONE or self.action_state == STATE_ERROR
end

function M:deactivate_kb(kb_name)
    if self.active_kb == kb_name then
        self.hub_control:on_kb_done(self.bb, kb_name, nil)
        self.action_state = STATE_IDLE
        self.active_kb = nil
        self.bb.active_kb = ""
    end
end

---------------------------------------------------------------------------
-- Tick
---------------------------------------------------------------------------

function M:tick()
    -- If MQTT hub provided, poll and route stream messages to buffers
    if self.mqtt_hub then
        self.mqtt_hub:poll_and_route(1)
    end

    -- Drain inbound transport → process stream messages
    self:_process_stream()

    -- Check timeouts
    self:_check_timeouts()

    -- Publish robot status
    self._tick_count = self._tick_count + 1
    if self._tick_count % 10 == 0 then
        self:_publish_robot_status()
    end
end

---------------------------------------------------------------------------
-- Stream message processing
---------------------------------------------------------------------------

function M:_process_stream()
    while true do
        local raw = self.tx:recv_stream()
        if not raw then break end

        local ok, msg = pcall(json_util.decode, raw)
        if not ok or not msg then goto continue end

        local msg_type = msg.type

        if msg_type == "ack" then
            self:_on_ack(msg)
        elseif msg_type == "heartbeat" then
            self:_on_heartbeat(msg)
        elseif msg_type == "kb_done" then
            self:_on_kb_done(msg)
        end

        ::continue::
    end
end

function M:_on_ack(msg)
    if self.action_state ~= STATE_WAIT_ACK then return end

    self.bb.ack_received = true
    self.bb.ack_seq = msg.seq
    self.bb.ack_status = msg.status

    -- Transition to active, start KB_DONE timeout
    self.action_state = STATE_ACTIVE
    self.ack_deadline = nil
    self.kb_done_deadline = os.time() + KB_DONE_TIMEOUT
end

function M:_on_heartbeat(msg)
    if self.action_state ~= STATE_ACTIVE then return end

    -- Update bitmask from heartbeat data
    local kb_name = self.active_kb
    if kb_name and msg.delta_x then
        -- Progressive pose updates from heartbeat
        self.bb[kb_name .. ".delta_x"] = msg.delta_x
        self.bb[kb_name .. ".delta_y"] = msg.delta_y
    end

    -- Reset KB_DONE deadline on each heartbeat (robot is still alive)
    self.kb_done_deadline = os.time() + KB_DONE_TIMEOUT
end

function M:_on_kb_done(msg)
    if self.action_state ~= STATE_ACTIVE and
       self.action_state ~= STATE_WAIT_ACK then return end

    -- Apply delta pose
    self.hub_control:apply_delta_pose(self.bb, msg)

    -- Record result
    self.bb.kb_done_received = true
    self.bb.kb_done_success = msg.success
    if msg.fault_reason then
        self.bb.fault_reason = msg.fault_reason
    end

    -- Update energy from robot report
    if msg.energy_remaining then
        self.energy_remaining = msg.energy_remaining
    end

    self.action_state = STATE_DONE
    self.ack_deadline = nil
    self.kb_done_deadline = nil
end

---------------------------------------------------------------------------
-- Timeout checks
---------------------------------------------------------------------------

function M:_check_timeouts()
    local now = os.time()

    if self.action_state == STATE_WAIT_ACK and self.ack_deadline then
        if now >= self.ack_deadline then
            self.bb.fault_reason = "ack_timeout"
            self.bb.kb_done_success = false
            self.action_state = STATE_ERROR
            self.ack_deadline = nil
        end
    end

    if self.action_state == STATE_ACTIVE and self.kb_done_deadline then
        if now >= self.kb_done_deadline then
            self.bb.fault_reason = "kb_done_timeout"
            self.bb.kb_done_success = false
            self.action_state = STATE_ERROR
            self.kb_done_deadline = nil
        end
    end
end

---------------------------------------------------------------------------
-- Robot status publishing (bitmask + energy → kv-bridge via MQTT)
---------------------------------------------------------------------------

function M:_publish_robot_status()
    if not self.mqtt_hub then return end

    local bb = self.bb
    local active_kb = bb.active_kb or ""

    local raw = 0
    local fields = {}
    if active_kb ~= "" then
        raw = bb[active_kb .. ".bitmask"] or 0
        local plugin = self.kb_by_name[active_kb]
        if plugin and plugin.bitmask then
            for _, field in ipairs(plugin.bitmask) do
                local bit_val = 2 ^ field.bit
                fields[field.name] = (math.floor(raw / bit_val) % 2) == 1
            end
        end
    end

    -- Heartbeat bit
    if raw % 2 == 0 then raw = raw + 1 end
    fields.heartbeat = true

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

    local energy_json = json_util.encode({
        energy_max       = self.energy_max,
        energy_remaining = self.energy_remaining,
        robot_id         = self.robot_id,
        timestamp        = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    })
    self.mqtt_hub:publish_kv(self._status_bucket, self._energy_key, energy_json)
end

---------------------------------------------------------------------------
-- Energy
---------------------------------------------------------------------------

function M:deduct_energy(cost)
    if self.energy_infinite then return end
    self.energy_remaining = math.max(0, self.energy_remaining - cost)
end

function M:get_energy_remaining()
    return self.energy_remaining
end

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
    local plugins = {}
    for _, p in pairs(self.kb_by_name) do
        plugins[#plugins + 1] = p
    end
    return plugins
end

function M:get_kb_by_name()
    return self.kb_by_name
end

---------------------------------------------------------------------------
-- Shutdown and cleanup
---------------------------------------------------------------------------

function M:send_shutdown()
    seq_counter = seq_counter + 1
    pcall(function()
        self.tx:send_rpc(json_util.encode({
            packet_type = 255,
            seq = seq_counter,
        }))
    end)
end

function M:close()
    pcall(function() self.tx:close() end)
end

return M
