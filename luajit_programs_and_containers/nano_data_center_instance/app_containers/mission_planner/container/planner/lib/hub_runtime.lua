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

-- Phase 5 C2 lazy require: the new drive-packet path lives under
-- hub_dsl/protocol/encoder.lua. Lazy so existing callers don't need the
-- protocol dir on package.path until the new path is exercised.
local _encoder
local function encoder_module()
    if not _encoder then _encoder = require("encoder") end
    return _encoder
end

local M = {}
M.__index = M

-- Action state machine states (legacy activate_kb path)
local STATE_IDLE      = "idle"
local STATE_WAIT_ACK  = "wait_ack"
local STATE_ACTIVE    = "active"
local STATE_DONE      = "done"
local STATE_ERROR     = "error"

-- Drive-packet state machine (Phase 5 cmd_drive_t wire path).
-- Parallel to action_state above and keyed on packet_id (per-packet
-- completion contract: one ACK + one done per cmd_drive_t, not per
-- sub-segment). Kept separate from legacy state so the two paths can
-- coexist during the C5 cut-over.
local STATE_DRIVE_IDLE     = "drive_idle"
local STATE_DRIVE_WAIT_ACK = "drive_wait_ack"
local STATE_DRIVE_ACTIVE   = "drive_active"
local STATE_DRIVE_DONE     = "drive_done"
local STATE_DRIVE_ERROR    = "drive_error"

-- Timeouts (wall-clock seconds)
local ACK_TIMEOUT     = 5
local KB_DONE_TIMEOUT = 10
-- Drive-done can exceed KB_DONE_TIMEOUT because one cmd_drive_t bundles
-- a whole edge polyline (could be many meters). 60s is a placeholder
-- ceiling; per-packet timeout sourced from edge metadata in C3b.
local DRIVE_ACK_TIMEOUT  = 5
local DRIVE_DONE_TIMEOUT = 60

-- Sequence counter
local seq_counter = 0

---------------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------------
function M.new(opts)
    local self = setmetatable({}, M)

    local robot_id        = opts.robot_id        or error("hub_runtime: robot_id required")
    local site            = opts.site            or error("hub_runtime: site required")
    local system_name     = opts.system_name     or error("hub_runtime: system_name required (v3 kb_query positional arg)")
    local own_instance_id = opts.own_instance_id or error("hub_runtime: own_instance_id required (this container's name)")
    local initial_pose = opts.initial_pose or { x = 0, y = 0, z = 0, heading = 0, arm_angle = 0 }

    self.robot_id          = robot_id
    self.site              = site
    self.system_name       = system_name
    self.own_instance_id   = own_instance_id
    -- Phase 5 C4: tenant identifier; defaults to own_instance_id.
    self.planner_namespace = opts.planner_namespace or own_instance_id

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
    if opts.pg_conn then
        local kb_query = require("kb_query")
        local q = kb_query.new(opts.pg_conn, system_name, site,
            own_instance_id, self.planner_namespace)
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

    -- Action state machine (legacy)
    self.action_state = STATE_IDLE
    self.active_kb    = nil
    self.ack_deadline = nil
    self.kb_done_deadline = nil

    -- Drive-packet state machine (Phase 5)
    self.drive_state = STATE_DRIVE_IDLE
    self.pending_drive_packet_id = nil
    self.pending_drive_deadline  = nil
    self.last_drive_fault = nil

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

    -- Build and send command to robot.
    -- Strip next_test (ChainTree bookkeeping) from the wire payload.
    -- Keep packet_type, seq, test_id (needed for ack/kb_done matching).
    local json_str = self.bb.current_test_json
    if json_str then
        local ok, action_json = pcall(json_util.decode, json_str)
        if ok and action_json then
            seq_counter = seq_counter + 1
            action_json.packet_type = plugin.packet_type_id
            action_json.seq = seq_counter
            -- Build wire payload without next_test
            local wire = {}
            for k, v in pairs(action_json) do
                if k ~= "next_test" then wire[k] = v end
            end
            self.tx:send_rpc(json_util.encode(wire))
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
-- Phase 5 C2 + C3a: cmd_drive_t emit + per-packet ACK/done state machine
---------------------------------------------------------------------------

-- Send a single cmd_drive_t packet AND start tracking its ACK/done.
--
-- The packet must already be built (e.g. via
-- route_builder.build_drive_packets()). encode_drive() validates the
-- shape eagerly, so a malformed packet errors here with a stack trace
-- at the build/send site -- not deep in the wire codec.
--
-- State transitions (drive_state):
--   on send:        any-terminal -> drive_wait_ack
--   on drive_ack:   drive_wait_ack -> drive_active   (or drive_error)
--   on drive_done:  drive_active   -> drive_done     (or drive_error)
--   on timeout:     wait_ack/active -> drive_error
--
-- "Terminal" here means drive_idle / drive_done / drive_error: send is
-- only legal between packets. Calling while a packet is in flight is a
-- programmer error (the action_server dispatch loop in C3b ensures
-- one-at-a-time before C5 cut-over).
--
-- Routing of incoming ACK/done messages into on_drive_ack/on_drive_done
-- is the caller's responsibility for now -- the wire format for those
-- messages is finalized in Phase 3 (robot side). Until then, tests
-- drive the state machine through the public API directly.
--
-- @param packet  cmd_drive_t Lua table (passes validate_drive)
-- @return packet_id integer (copied from packet.packet_id for caller
--                convenience).
function M:send_drive_packet(packet)
    if self.drive_state ~= STATE_DRIVE_IDLE
       and self.drive_state ~= STATE_DRIVE_DONE
       and self.drive_state ~= STATE_DRIVE_ERROR then
        error("hub_runtime: drive packet already in flight (state="
            .. tostring(self.drive_state) .. ", pending_id="
            .. tostring(self.pending_drive_packet_id) .. ")")
    end
    local enc = encoder_module()
    local bytes = enc.encode_drive(packet)
    -- Prefer send_rpc_raw when the transport has it (mqtt_hub_transport
    -- adapter does, mqtt_transport.local_side doesn't need it because
    -- its send_rpc is already a raw passthrough). send_rpc_raw bypasses
    -- the wire_format auto-CBOR-wrap that would double-encode our
    -- already-CBOR bytes.
    if self.tx.send_rpc_raw then
        self.tx:send_rpc_raw(bytes)
    else
        self.tx:send_rpc(bytes)
    end
    self.drive_state = STATE_DRIVE_WAIT_ACK
    self.pending_drive_packet_id = packet.packet_id
    self.pending_drive_deadline  = os.time() + DRIVE_ACK_TIMEOUT
    self.last_drive_fault = nil
    return packet.packet_id
end

-- Process a drive_ack matched on packet_id.
-- @param packet_id  integer from the ack message
-- @param status     "ok" | other (other => transition to drive_error)
-- @return true if the ACK was applied, false if it was ignored
--         (wrong state or wrong packet_id -- common during transient
--         re-acks or stale messages, NOT an error)
function M:on_drive_ack(packet_id, status)
    if self.drive_state ~= STATE_DRIVE_WAIT_ACK then return false end
    if packet_id ~= self.pending_drive_packet_id then return false end
    if status ~= "ok" then
        self.drive_state = STATE_DRIVE_ERROR
        self.last_drive_fault = "drive_ack_status=" .. tostring(status)
        self.pending_drive_deadline = nil
        return true
    end
    self.drive_state = STATE_DRIVE_ACTIVE
    self.pending_drive_deadline = os.time() + DRIVE_DONE_TIMEOUT
    return true
end

-- Process a drive_done matched on packet_id.
-- @param packet_id     integer from the done message
-- @param success       boolean
-- @param fault_reason  optional string when success=false
-- @return true if applied, false if ignored
function M:on_drive_done(packet_id, success, fault_reason)
    if self.drive_state ~= STATE_DRIVE_ACTIVE
       and self.drive_state ~= STATE_DRIVE_WAIT_ACK then return false end
    if packet_id ~= self.pending_drive_packet_id then return false end
    self.pending_drive_deadline = nil
    if success then
        self.drive_state = STATE_DRIVE_DONE
    else
        self.drive_state = STATE_DRIVE_ERROR
        self.last_drive_fault = fault_reason or "drive_done_unspecified"
    end
    return true
end

-- Wall-clock timeout check; called from M:tick().
function M:_check_drive_timeouts()
    if not self.pending_drive_deadline then return end
    if os.time() < self.pending_drive_deadline then return end

    if self.drive_state == STATE_DRIVE_WAIT_ACK then
        self.last_drive_fault = "drive_ack_timeout"
        self.drive_state = STATE_DRIVE_ERROR
    elseif self.drive_state == STATE_DRIVE_ACTIVE then
        self.last_drive_fault = "drive_done_timeout"
        self.drive_state = STATE_DRIVE_ERROR
    end
    self.pending_drive_deadline = nil
end

-- Caller polls completion. Mirrors kb_is_complete for the legacy path.
function M:drive_is_complete()
    return self.drive_state == STATE_DRIVE_DONE
        or self.drive_state == STATE_DRIVE_ERROR
end

-- Inspect drive state for ACK matching, debug, and dispatch glue.
function M:drive_state_get()
    return self.drive_state,
           self.pending_drive_packet_id,
           self.last_drive_fault
end

-- Reset to drive_idle. Caller invokes between packets to release the
-- send_drive_packet guard. Required between successful packets too --
-- there is no auto-clear, so a stale drive_done doesn't masquerade as
-- the start of the next packet's window.
function M:drive_clear()
    self.drive_state = STATE_DRIVE_IDLE
    self.pending_drive_packet_id = nil
    self.pending_drive_deadline = nil
    self.last_drive_fault = nil
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

    -- Check timeouts (legacy + drive)
    self:_check_timeouts()
    self:_check_drive_timeouts()

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
